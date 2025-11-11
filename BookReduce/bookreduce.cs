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
    /// Input class for orchestration, can expand to list of books and URIs for blob storage implementation
    /// </summary>
    public class OrchestrationInput 
    {
        public string Path { get; set; } = string.Empty;
    }

    /// <summary>
    /// Output class for mapped books. Each object contains the book name and section of that book. Then a dictionary that maps words in that section to their frequency count.
    /// </summary>
    public class SectionOutput 
    {
        public string Book { get; set; } = string.Empty;
        public int Section { get; set; } 
        public Dictionary<string, int> Frequencies { get; set; } = new Dictionary<string, int>(); // word => word count

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
    /// Starts a new orchestration instance of the <see cref="BookReduce"/> function. Takes HTTP POST requests with file location.
    /// </summary>
    /// <param name="req">Contains the POST request information with data path</param>
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

        // TODO: Change to blob storage for prod, for now use .txts locally !!!
        var path = "C:\\Users\\HamSa\\csClasses\\CS722\\2P_Real\\BookReduce\\BookReduce\\books\\"; // Local path for testing

        // Function input comes from the request content.
        string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(
            nameof(BookReduce), new OrchestrationInput {Path = path});

        logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

        // Returns an HTTP 202 response with an instance management payload.
        // See https://learn.microsoft.com/azure/azure-functions/durable/durable-functions-http-api#start-orchestration

        return await client.CreateCheckStatusResponseAsync(req, instanceId);
    }

    /// <summary>
    /// Orchestrates the MapReduce pattern for all the given book paths.
    /// Calls the mapper function for each file found, then calls the reducer function to aggregate results
    /// </summary>
    /// <param name="context"></param>
    /// <returns>Output of where resulting JSON file can be found and a summary of the mapping procedure</returns>
    [Function(nameof(BookReduce))]
    public static async Task<List<string>> RunOrchestrator(
        [OrchestrationTrigger] TaskOrchestrationContext context)
    {
        ILogger logger = context.CreateReplaySafeLogger(nameof(BookReduce));

        // Get Files to process
        var input = context.GetInput<OrchestrationInput>();
        string path = input.Path;

        var files = await context.CallActivityAsync<string[]>
            (nameof(GetFileListAsync), path);

        var outputs = new List<string>();

        logger.LogInformation("Files found: {fileCount}", files.Length);
        logger.LogInformation("Creating mappers to process files...");
        context.SetCustomStatus($"Files found: {files.Length}. Creating mappers to process files...");

        // Create mappers to process each file
        var tasks = new Task<SectionOutput[]>[files.Length];
        for (int i = 0; i < files.Length; i++)
        {
            tasks[i] = context.CallActivityAsync<SectionOutput[]>(
                nameof(MapperAsync),
                files[i]);
        }

        // Wait for all mapper tasks to complete
        var results = await Task.WhenAll(tasks); 
        var flatResults = results.SelectMany(sections => sections).ToArray();

        // Reduce mapper results
        var reducedPath = await context.CallActivityAsync<string>(
            nameof(Reducer),
            flatResults);

        // Process results
        foreach (var section in flatResults)
        {
            outputs.Add($"Book: {section.Book}, Section: {section.Section}, Unique Words: {section.Frequencies.Count}");
        }
        outputs.Add($"Reduced Results located at: {reducedPath}");

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
            return $"Reduced results written to {outputPath}";
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
    /// Mapper function takes a file path, reads the file, splits into sections, and counts word frequencies in each section.
    /// Creates section objects of 5000 words each which are returned together as an array.
    /// </summary>
    [Function(nameof(MapperAsync))]
    public static async Task<SectionOutput[]> MapperAsync(
        [ActivityTrigger] string filePath, FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("MapperAsync");
        logger.LogInformation("Processing file: {filePath}", filePath);

        var sections = Array.Empty<SectionOutput>();

        try
        {
            string content = await File.ReadAllTextAsync(filePath);

            var words = content.Split(new char[] { ' ', '\n', '\r', ',', '.', ';', ':', '!', '?' }, StringSplitOptions.RemoveEmptyEntries);

            var total = 0;
            
            var curr = new SectionOutput(Path.GetFileName(filePath), 0);
            sections.Append(curr);
            foreach (var word in words)
            {
                if (total % 5000 == 0) // New section every 5000 words
                {
                    Array.Resize(ref sections, sections.Length + 1);
                    curr = new SectionOutput(Path.GetFileName(filePath), sections.Length - 1);
                    sections[^1] = curr;
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
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing file: {filePath}", filePath);
        }

        return sections;
    }

    /// <summary>
    ///    Currently gets list of .txt files from local directory asynchronously for testing. TODO: Change to blob storage for production. !!!
    /// </summary>
    /// <param name="path">Local path to book files</param>
    /// <param name="executionContext"></param>
    /// <returns>.txt files found at path</returns>
    [Function(nameof(GetFileListAsync))]
    public static async Task<string[]> GetFileListAsync([ActivityTrigger] string path, FunctionContext executionContext)
    {
        ILogger logger = executionContext.GetLogger("GetFileListAsync");
        logger.LogInformation("Getting file list from path: {path}", path);
        // For local testing, get .txt files from the specified directory asynchronously
        var files = await Task.Run(() => Directory.GetFiles(path, "*.txt"));
        return files;
    }
}