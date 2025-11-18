using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;

namespace BookReduce;

public class BookReduceFunctions
{
    private readonly ILogger<BookReduceFunctions> _logger;

    public BookReduceFunctions(ILogger<BookReduceFunctions> logger)
    {
        _logger = logger;
    }

    public class OrchestrationInput 
    {
        public List<BookInput> Books { get; set; } = new List<BookInput>();
    }

    public class BookInput
    {
        public string Name { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;
    }

    public class BucketOutput 
    {
        public string BookName { get; set; } = string.Empty;
        public int BucketNumber { get; set; } 
        public Dictionary<string, int> Frequencies { get; set; }

        public BucketOutput(string bookName, int bucketNumber)
        {
            Frequencies = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            BookName = bookName;
            BucketNumber = bucketNumber;
        }
    }

    public class TermOccurrence
    {
        public string BookName { get; set; } = string.Empty;
        public int BucketNumber { get; set; }
        public int TermFrequency { get; set; }
    }

    public class SaveIndexResult
    {
        public string BlobName { get; set; } = string.Empty;
        public int TermCount { get; set; }
        public int TotalOccurrences { get; set; }
    }

    private static readonly HashSet<string> StopWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "a", "an", "the", "or", "and", "but", "with"
    };

    [Function("BookReduce_HttpStart")]
    public async Task<HttpResponseData> HttpStart(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req,
        [DurableClient] DurableTaskClient client)
    {
        OrchestrationInput? input;
        try
        {
            input = await req.ReadFromJsonAsync<OrchestrationInput>();
            if (input == null || input.Books == null || input.Books.Count == 0)
            {
                throw new Exception("No books provided");
            }
        }
        catch
        {
            input = new OrchestrationInput
            {
                Books = new List<BookInput>
                {
                    new BookInput { Name = "Alice in Wonderland", Url = "https://www.gutenberg.org/files/11/11-0.txt" },
                    new BookInput { Name = "Pride and Prejudice", Url = "https://www.gutenberg.org/files/1342/1342-0.txt" },
                    new BookInput { Name = "Moby Dick", Url = "https://www.gutenberg.org/files/2701/2701-0.txt" },
                    new BookInput { Name = "Frankenstein", Url = "https://www.gutenberg.org/files/84/84-0.txt" },
                    new BookInput { Name = "Dracula", Url = "https://www.gutenberg.org/files/345/345-0.txt" },
                    new BookInput { Name = "Sherlock Holmes", Url = "https://www.gutenberg.org/files/1661/1661-0.txt" },
                    new BookInput { Name = "Tale of Two Cities", Url = "https://www.gutenberg.org/files/98/98-0.txt" },
                    new BookInput { Name = "Dorian Gray", Url = "https://www.gutenberg.org/files/174/174-0.txt" },
                    new BookInput { Name = "Great Gatsby", Url = "https://www.gutenberg.org/files/64317/64317-0.txt" },
                    new BookInput { Name = "Wuthering Heights", Url = "https://www.gutenberg.org/files/768/768-0.txt" }
                }
            };
        }

        string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
            "BookReduce", input);

        _logger.LogInformation("Started orchestration with ID = '{instanceId}' for {bookCount} books", 
            instanceId, input.Books.Count);

        return await client.CreateCheckStatusResponseAsync(req, instanceId);
    }

    [Function("BookReduce")]
    public async Task<string> RunOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        ILogger logger = context.CreateReplaySafeLogger("BookReduce");

        var input = context.GetInput<OrchestrationInput>();
        
        logger.LogInformation("Starting MapReduce for {bookCount} books", input!.Books.Count);
        context.SetCustomStatus($"Starting MapReduce for {input.Books.Count} books...");

        // MAP PHASE: Fan-out
        var mapTasks = new List<Task<BucketOutput[]>>();
        foreach (var book in input.Books)
        {
            mapTasks.Add(context.CallActivityAsync<BucketOutput[]>(
                "MapWorker",
                book));
        }

        logger.LogInformation("Map phase: Processing {mapperCount} books in parallel", mapTasks.Count);
        context.SetCustomStatus($"Map phase: Processing {mapTasks.Count} books...");

        var mapResults = await Task.WhenAll(mapTasks);
        var allBuckets = mapResults.SelectMany(buckets => buckets).ToArray();

        logger.LogInformation("Map complete: {bucketCount} buckets generated", allBuckets.Length);
        context.SetCustomStatus($"Map complete. Reducing {allBuckets.Length} buckets...");

        // REDUCE & SAVE PHASE - Combined to avoid passing huge data between activities
        var result = await context.CallActivityAsync<SaveIndexResult>(
            "ReduceAndSaveWorker",
            allBuckets);

        logger.LogInformation("MapReduce complete: {blobName}", result.BlobName);
        
        return $"SUCCESS: {result.BlobName} ({result.TermCount} terms, {result.TotalOccurrences} occurrences)";
    }

    [Function("MapWorker")]
    public async Task<BucketOutput[]> MapWorker(
        [ActivityTrigger] BookInput bookInput)
    {
        _logger.LogInformation("MAP: Processing '{bookName}'", bookInput.Name);

        var buckets = new List<BucketOutput>();

        try
        {
            using var httpClient = new HttpClient();
            httpClient.Timeout = TimeSpan.FromMinutes(2);
            string content = await httpClient.GetStringAsync(bookInput.Url);

            // Remove punctuation
            string cleanedContent = Regex.Replace(content, @"[^\w\s]", " ");
            
            var words = cleanedContent
                .Split(new char[] { ' ', '\n', '\r', '\t' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(w => w.Trim().ToLower())
                .Where(w => !string.IsNullOrWhiteSpace(w))
                .ToArray();

            _logger.LogInformation("MAP: '{bookName}' has {wordCount} words", bookInput.Name, words.Length);

            const int BUCKET_SIZE = 5000;
            int bucketNumber = 1;
            BucketOutput currentBucket = new BucketOutput(bookInput.Name, bucketNumber);
            int wordCount = 0;

            foreach (var word in words)
            {
                if (StopWords.Contains(word))
                    continue;

                if (currentBucket.Frequencies.ContainsKey(word))
                {
                    currentBucket.Frequencies[word]++;
                }
                else
                {
                    currentBucket.Frequencies[word] = 1;
                }

                wordCount++;

                if (wordCount % BUCKET_SIZE == 0)
                {
                    buckets.Add(currentBucket);
                    bucketNumber++;
                    currentBucket = new BucketOutput(bookInput.Name, bucketNumber);
                }
            }

            if (currentBucket.Frequencies.Count > 0)
            {
                buckets.Add(currentBucket);
            }

            _logger.LogInformation("MAP: '{bookName}' → {bucketCount} buckets", bookInput.Name, buckets.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MAP ERROR: Failed to process '{bookName}'", bookInput.Name);
        }

        return buckets.ToArray();
    }

    [Function("ReduceAndSaveWorker")]
    public async Task<SaveIndexResult> ReduceAndSaveWorker(
        [ActivityTrigger] BucketOutput[] buckets)
    {
        _logger.LogInformation("REDUCE & SAVE: Processing {bucketCount} buckets", buckets.Length);

        try
        {
            string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage") 
                ?? throw new Exception("AzureWebJobsStorage not configured");
            
            var blobServiceClient = new BlobServiceClient(connectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient("mapreduce-output");
            await containerClient.CreateIfNotExistsAsync();
            
            string blobName = $"inverted_index_{DateTime.UtcNow:yyyyMMdd_HHmmss}.json";
            var blobClient = containerClient.GetBlobClient(blobName);

            // Build inverted index
            var invertedIndex = new Dictionary<string, List<TermOccurrence>>(StringComparer.OrdinalIgnoreCase);
            
            foreach (var bucket in buckets)
            {
                foreach (var kvp in bucket.Frequencies)
                {
                    string term = kvp.Key;
                    int frequency = kvp.Value;

                    if (!invertedIndex.ContainsKey(term))
                    {
                        invertedIndex[term] = new List<TermOccurrence>();
                    }

                    invertedIndex[term].Add(new TermOccurrence
                    {
                        BookName = bucket.BookName,
                        BucketNumber = bucket.BucketNumber,
                        TermFrequency = frequency
                    });
                }
            }

            // Sort each term's occurrences
            foreach (var term in invertedIndex.Keys.ToList())
            {
                invertedIndex[term] = invertedIndex[term]
                    .OrderByDescending(occ => occ.TermFrequency)
                    .ThenBy(occ => occ.BookName)
                    .ThenBy(occ => occ.BucketNumber)
                    .ToList();
            }

            _logger.LogInformation("REDUCE: Index with {termCount} terms created", invertedIndex.Count);

            // Calculate statistics
            int totalTerms = invertedIndex.Count;
            int totalOccurrences = invertedIndex.Values.Sum(list => list.Count);
            var topTerms = invertedIndex
                .Select(kvp => new { 
                    Term = kvp.Key, 
                    TotalFreq = kvp.Value.Sum(o => o.TermFrequency)
                })
                .OrderByDescending(x => x.TotalFreq)
                .Take(10)
                .ToList();

            // Stream JSON directly to blob to avoid memory issues
            using var stream = new MemoryStream();
            using var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true });

            writer.WriteStartObject();
            foreach (var kvp in invertedIndex.OrderBy(x => x.Key))
            {
                writer.WritePropertyName(kvp.Key);
                writer.WriteStartArray();
                foreach (var occ in kvp.Value)
                {
                    writer.WriteStartObject();
                    writer.WriteString("bookName", occ.BookName);
                    writer.WriteNumber("bucketNumber", occ.BucketNumber);
                    writer.WriteNumber("termFrequency", occ.TermFrequency);
                    writer.WriteEndObject();
                }
                writer.WriteEndArray();
            }
            writer.WriteEndObject();
            await writer.FlushAsync();

            // Upload to blob
            stream.Position = 0;
            await blobClient.UploadAsync(stream, overwrite: true);

            _logger.LogInformation("=== INDEX STATISTICS ===");
            _logger.LogInformation("Total terms: {terms}", totalTerms);
            _logger.LogInformation("Total occurrences: {occ}", totalOccurrences);
            _logger.LogInformation("Top 10 terms:");
            foreach (var t in topTerms)
            {
                _logger.LogInformation("  {term}: {freq}", t.Term, t.TotalFreq);
            }
            _logger.LogInformation("Saved to blob: {blobName}", blobName);
            _logger.LogInformation("Download from: Azure Portal → bookreducestorage → mapreduce-output → {blobName}", blobName);

            return new SaveIndexResult
            {
                BlobName = blobName,
                TermCount = totalTerms,
                TotalOccurrences = totalOccurrences
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Reduce and Save failed");
            throw;
        }
    }
}