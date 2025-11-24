# BookReduce

### To run function

    Make sure you are in BookReduce and run: $ azurite
        -This starts the local storage
    Open a new terminal window and navigate back to BookReduce and run: $ func start
        -This starts function and exposes local HTTP port
    Finally, in a new terminal, run a curl line to post to the outputted HTTP port.
        -For example: $ curl -X POST "http://localhost:7071/api/BookReduce_HttpStart" -H "Content-Type: application/json" --data-binary "@test_books.json"
        -This starts the function using the test_books.json file for links to the books and tells the method you'll be giving it a JSON file.

    The function will take a bit to run. The output will be stored in a json file with a name similar to inverted_index_20241119_153045.json
    The file will be saved in BookReduce/bin/output/MapReduceOutput/inverted_index_currentdate_currenttime.json


### AI Usage

    Using Claude 4.5 Sonnet and Gemini 3 we were able to refactor the starting code given in the assignment spec to conform to the assignment requirements. In the beginning it was
    useful to use prompts like "How do you pass a list of txt files to a durable azure function" and "How to call Azure functions within other Azure functions" to get
    a baseline and make both debugging and developing a lot easier. The ReduceandSaveWorker function was also a pain because it kept trying to save to the cloud rather than
    using local storage (azurite) so we used prompts like "My Azure function is trying to save to the cloud but I want it to save to a local folder".