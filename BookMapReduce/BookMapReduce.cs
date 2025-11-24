using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;

namespace BookMapReduce;

/// <summary>
///  Map Reduce pattern for counting word frequencies in book sections using Azure Durable Functions.
/// </summary>
public static class BookReduce
{

    /// <summary>
    /// Input class for orchestration with book title and URL pairs
    /// </summary>
    public class OrchestrationInput
    {
        public List<BookInput> Books { get; set; } = new List<BookInput>();
    }

    /// <summary>
    /// Represents a book with its title and URL
    /// </summary>
    public class BookInput
    {
        public string Title { get; set; } = string.Empty;
        public string Url { get; set; } = string.Empty;
    }

    /// <summary>
    /// Output class for mapped books. Each object contains the book name and section of that book. Then a dictionary that maps words in that section to their frequency count.
    /// Same as having many KeyValuePair<string, int> objects for word counts, but grouped into a single object per section to keep track of book and section w/o repeating that data at each entry
    /// </summary>
    public class SectionOutput
    {
        public string Book { get; set; } = string.Empty;
        public int Section { get; set; }
        public Dictionary<string, int> Frequencies { get; set; } // word => word count

        public SectionOutput(string book, int section)
        {
            Frequencies = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            Book = book;
            Section = section;
        }
    }

    /// <summary>
    /// Class to reduce the mapped results into a single word occurrence object. Rather than a section with many words, this class represents a single occurence of a word in a book section (with frequency).
    /// Primarily for formatting for JSON output.
    /// </summary>
    public class WordOccurence
    {
        public string Book { get; set; } = string.Empty;
        public string Section { get; set; } = string.Empty;
        public int Count { get; set; }
    }

    /// <summary>
    /// Starts a new orchestration instance of the <see cref="BookReduce"/> function. Takes HTTP POST requests with book list.
    /// </summary>
    /// <param name="req">Contains the POST request information with book title-URL pairs</param>
    /// <param name="client"></param>
    /// <param name="executionContext"></param>
    /// <returns>Status of orchestration w/ output</returns>
    [Function("BookReduce_HttpStart")]
    public static async Task<HttpResponseData> HttpStart(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData req,
        [DurableClient] DurableTaskClient client,
        FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("BookReduce_HttpStart");

        try
        {
            // Parse the request body
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var input = JsonSerializer.Deserialize<OrchestrationInput>(requestBody, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            if (input == null || !input.Books.Any())
            {
                var badResponse = req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
                await badResponse.WriteStringAsync("input is null");
                return badResponse;
            }

            // Validate that all books have both title and URL
            foreach (var book in input.Books)
            {
                if (string.IsNullOrEmpty(book.Title) || string.IsNullOrEmpty(book.Url))
                {
                    var badResponse = req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
                    await badResponse.WriteStringAsync("Invalid input");
                    return badResponse;
                }
            }

            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(BookReduce), input);

            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Parseing input issue");
            var errorResponse = req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            await errorResponse.WriteStringAsync("Invalid JSON format");
            return errorResponse;
        }
    }


    /// <summary>
    /// HTTP GET endpoint to retrieve the results JSON file that is stored ephemerally
    /// </summary>
    [Function("GetResults")]
    public static async Task<HttpResponseData> GetResults(
        [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "GetResults")] HttpRequestData req,
        [DurableClient] DurableTaskClient client,
        FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("GetResults");

        string outputPath = Path.Combine(
            Path.GetTempPath(),
            "results.json");

        try
        {
            if (!File.Exists(outputPath))
            {
                logger.LogWarning("Results not found at {path}", outputPath);
                var notFoundResponse = req.CreateResponse(HttpStatusCode.NotFound);
                await notFoundResponse.WriteStringAsync("Results not found. Orchestration not run");
                return notFoundResponse;
            }

            string jsonContent = await File.ReadAllTextAsync(outputPath);

            var response = req.CreateResponse(HttpStatusCode.OK);
            response.Headers.Add("Content-Type", "application/json");
            await response.WriteStringAsync(jsonContent);

            logger.LogInformation("Successfully sent results.json");
            return response;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error reading results file from {path}", outputPath);
            var errorResponse = req.CreateResponse(HttpStatusCode.InternalServerError);
            await errorResponse.WriteStringAsync($"Error reading results: {ex.Message}");
            return errorResponse;
        }
    }

    /// <summary>
    /// Orchestrates the MapReduce pattern for all the given book URLs.
    /// Calls the mapper function for each book URL, then calls the reducer function to aggregate results
    /// </summary>
    /// <param name="context"></param>
    /// <returns>Output of where resulting JSON file can be found and a summary of the mapping procedure</returns>
    [Function(nameof(BookReduce))]
    public static async Task<List<string>> RunOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        ILogger logger = context.CreateReplaySafeLogger(nameof(BookReduce));

        // Get Books to process
        var input = context.GetInput<OrchestrationInput>();
        var books = input.Books;

        var outputs = new List<string>();

        logger.LogInformation("Books found: {bookCount}", books.Count);
        logger.LogInformation("Creating mappers to process books...");
        context.SetCustomStatus($"Books found: {books.Count}. Creating mappers to process books...");

        // Create mappers to process each book
        var tasks = new Task<SectionOutput[]>[books.Count];
        for (int i = 0; i < books.Count; i++)
        {
            // Maybe add delay here to avoid overwhelming project Gutenberg?
            //if (i > 0)
            //{
            //    await context.CreateTimer(context.CurrentUtcDateTime.AddSeconds(2), CancellationToken.None);
            //}
            tasks[i] = context.CallActivityAsync<SectionOutput[]>(
                nameof(MapperAsync),
                books[i]);
        }

        // Wait for all mapper tasks to complete
        var results = await Task.WhenAll(tasks);
        var flatResults = results.SelectMany(sections => sections).ToArray();

        // Reduce mapper results
        var reduced = await context.CallActivityAsync<string>(
            nameof(Reducer),
            flatResults);

        // Process results
        //foreach (var section in flatResults)
        //{
        //    outputs.Add($"Book: {section.Book}, Section: {section.Section}, Unique Words: {section.Frequencies.Count}");
        //}
        outputs.Add(reduced);

        return outputs;
    }

    /// <summary>
    /// Reduces the outputs of map which has many words per section into a single object per word with all occurrences across sections and books.
    /// </summary>
    /// <param name="sections">The results from mapping</param>
    /// <param name="executionContext"></param>
    /// <returns>The path to the resulting JSON file with reduced results</returns>
    [Function(nameof(Reducer))]
    public static async Task<string> Reducer([ActivityTrigger] SectionOutput[] sections, FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("Reducer");
        logger.LogInformation("Reducing {sectionCount} sections.", sections.Length);

        var reducedResults = new Dictionary<string, List<WordOccurence>>();
        var wordTotals = new Dictionary<string, int>();

        foreach (var section in sections)
        {
            foreach (var kvp in section.Frequencies)
            {
                var word = kvp.Key;
                var count = kvp.Value;
                if (!reducedResults.ContainsKey(word))
                {
                    reducedResults[word] = new List<WordOccurence>();
                    wordTotals[word] = 0;
                }
                reducedResults[word].Add(new WordOccurence
                {
                    Book = section.Book,
                    Section = section.Section.ToString(),
                    Count = count
                });

                wordTotals[word] += count;
            }
        }

        var sortedResults = wordTotals
            .OrderByDescending(kvp => kvp.Value) // Sort by total count
            .ToDictionary(
                kvp => kvp.Key,
                kvp => reducedResults[kvp.Key]
                    .OrderByDescending(wo => wo.Count) // Sort occurrences within word
                    .ToList()
            );

        reducedResults = sortedResults;

        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        string jsonContent = JsonSerializer.Serialize(reducedResults, jsonOptions);

        string outputPath = Path.Combine(
            Path.GetTempPath(),
            "results.json");

        try
        {
            await File.WriteAllTextAsync(outputPath, jsonContent);
            return outputPath;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error writing reduced results to file.");
            return $"Error writing reduced results: {ex.Message}";
        }
    }

    /// <summary>
    /// Set of words to ignore when parsing and counting word frequencies.
    /// </summary>
    private static readonly HashSet<string> StopWords = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "a", "an", "the", "or", "and", "but", "with", "in", "on", "at", "to", "for", "of", "as", "by", "is", "are", "was", "were", "be", "been", "have", "has", "had", "do", "does", "did", "will", "would", "could", "should", "may", "might", "can", "this", "that", "these", "those", "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them", "my", "your", "his", "her", "its", "our", "their", "not", "no", "yes", "s"
    };

    /// <summary>
    /// Mapper function takes a book input with title and URL, downloads the content, splits into sections, and counts word frequencies in each section.
    /// Creates section objects of 5000 words each which are returned together as an array.
    /// </summary>
    [Function(nameof(MapperAsync))]
    public static async Task<SectionOutput[]> MapperAsync(
        [ActivityTrigger] BookInput bookInput, FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("MapperAsync");
        logger.LogInformation("Processing book: {title} from URL: {url}", bookInput.Title, bookInput.Url);

        var sections = new List<SectionOutput>();

        try
        {
            string content;
            using (var httpClient = new HttpClient())
            {
                // Download content from URL. This is where I got content when testing locally. Keep here?
                content = await httpClient.GetStringAsync(bookInput.Url);
            }

            string cleanedContent = Regex.Replace(content, @"[^\w\s]", " ");
            var words = cleanedContent
                .Split(new char[] { ' ', '\n', '\r', '\t' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(w => {
                    string cleaned = w.Trim().ToLower();
                    return cleaned;
                })
                .Where(w => !string.IsNullOrWhiteSpace(w))
                .ToArray();


            var total = 0;
            var sectionIndex = 0;
            var curr = new SectionOutput(bookInput.Title, sectionIndex);
            sections.Add(curr);

            foreach (var word in words)
            {
                if (total % 5000 == 0 && total > 0) // New section every 5000 words
                {
                    sectionIndex++;
                    curr = new SectionOutput(bookInput.Title, sectionIndex);
                    sections.Add(curr);
                }

                if (!StopWords.Contains(word))
                {
                    if (curr.Frequencies.ContainsKey(word))
                    {
                        curr.Frequencies[word]++;
                    }
                    else
                    {
                        curr.Frequencies[word] = 1;
                    }
                }

                total++;
            }

            logger.LogInformation("Processed book '{title}' with {sectionCount} sections", bookInput.Title, sections.Count);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing book: {title} from URL: {url}", bookInput.Title, bookInput.Url);
        }

        return sections.ToArray();
    }
}