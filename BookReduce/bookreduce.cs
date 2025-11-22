using System.Net;
using System.Numerics;
using System.Security.AccessControl;
using System.Security.Policy;
using System.Text.Json;
using System.Threading.Tasks;
using System.Web;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Microsoft.Extensions.Logging;
using static System.Reflection.Metadata.BlobBuilder;
using static Microsoft.ApplicationInsights.MetricDimensionNames.TelemetryContext;

namespace BookReduce;

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
                await badResponse.WriteStringAsync("Invalid request. Provide a list of books with title and URL pairs.");
                return badResponse;
            }

            // Validate that all books have both title and URL
            foreach (var book in input.Books)
            {
                if (string.IsNullOrEmpty(book.Title) || string.IsNullOrEmpty(book.Url))
                {
                    var badResponse = req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
                    await badResponse.WriteStringAsync("All books must have both title and URL.");
                    return badResponse;
                }
            }

            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
                nameof(BookReduce), input);

            logger.LogInformation("Started orchestration with ID = '{instanceId}' for {bookCount} books.", instanceId, input.Books.Count);

            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Failed to parse request JSON");
            var errorResponse = req.CreateResponse(System.Net.HttpStatusCode.BadRequest);
            await errorResponse.WriteStringAsync("Invalid JSON format");
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
        foreach (var section in flatResults)
        {
            outputs.Add($"Book: {section.Book}, Section: {section.Section}, Unique Words: {section.Frequencies.Count}");
        }
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

        foreach (var section in sections)
        {
            foreach (var kvp in section.Frequencies)
            {
                var word = kvp.Key;
                var count = kvp.Value;
                if (!reducedResults.ContainsKey(word))
                {
                    reducedResults[word] = new List<WordOccurence>();
                }
                reducedResults[word].Add(new WordOccurence
                {
                    Book = section.Book,
                    Section = section.Section.ToString(),
                    Count = count
                });
            }
        }

        var jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        string jsonContent = JsonSerializer.Serialize(reducedResults, jsonOptions);

        string outputPath = Path.Combine(
            "C:\\Users\\HamSa\\csClasses\\CS722\\2P_Real\\BookReduce\\BookReduce",
            "reduced_results.json");

        try
        {
            await File.WriteAllTextAsync(outputPath, jsonContent);
            return jsonContent;
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
        "a", "an", "the", "or", "and", "but", "with", "in", "on", "at", "to", "for", "of", "as", "by", "is", "are", "was", "were", "be", "been", "have", "has", "had", "do", "does", "did", "will", "would", "could", "should", "may", "might", "can", "this", "that", "these", "those", "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them", "my", "your", "his", "her", "its", "our", "their", "not", "no", "yes"
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

            var words = content.Split(new char[] { ' ', '\n', '\r', ',', '.', ';', ':', '!', '?' }, StringSplitOptions.RemoveEmptyEntries);

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

                // Skip common words (stop words)
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

    /// <summary>
    ///    Currently gets list of .txt files from local directory asynchronously for testing. TODO: Change to blob storage for production. !!!
    /// </summary>
    /// <param name="path">Local path to book files</param>
    /// <param name="executionContext"></param>
    /// <returns>.txt files found at path</returns>
    //[Function(nameof(GetFileListAsync))]
    //public static async Task<string[]> GetFileListAsync([ActivityTrigger] string path, FunctionContext executionContext)
    //{
    //    ILogger logger = executionContext.GetLogger("GetFileListAsync");
    //    logger.LogInformation("Getting file list from path: {path}", path);
    //    // For local testing, get .txt files from the specified directory asynchronously
    //    var files = await Task.Run(() => Directory.GetFiles(path, "*.txt"));
    //    return files;
    //}
}