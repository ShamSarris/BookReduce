# BookReduce

### To run in Azure

    - Create a resource group (I named mine bookreduce)
    - Create a storage account within the resource group (this is the blob storage)
    - Create the function app within the storage group
        - From the command line, run
            - $func azure functionapp publish [yourfunctionname]

        - You should see an output similar to:
            - Functions in bookreduce-func-1762995717:
        BookReduce_HttpStart - [httpTrigger]
        Invoke url: https://bookreduce-func-1762995717.azurewebsites.net/api/bookreduce_httpstart

        Using your OWN Invoke url, run a curl request to invoke the function 
            - ex. curl -X POST https://bookreduce-func-1762995717.azurewebsites.net/api/BookReduce_HttpStart

        The function will take a bit to run. The output will be stored in a json file with a name similar to
        inverted_index_20241119_153045.json


### AI Usage

