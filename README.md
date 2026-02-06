# decide-pdf-scraper
This service allows to gather the download URLs of new PDFs containing meeting resolution of local governments. A PDF is considered new if its download URL is not yet present in the triple store as the object of the predicate eli:is_exemplified_by of an ELI Manifestation.

## Set-up
1. Clone the repository [lblod/app-decide](https://github.com/lblod/app-decide), expose the containers so that they can communicate with each other, and then run both containers. 

   Exposing the containers was done by adding a file 'docker-compose.override.yaml' to the lblod/app-decide repo containing:
   ```
   services:
     virtuoso:
       networks:
         - decide
       ports:
         - "8890:8890"
   
   networks:
     decide:
       external: true
   ```
   Create the 'decide' Docker network using the following command:
   ```
   docker network create decide
   ```

2. Mount the folder data/files in the lblod/app-decide repo as a volume and add the mounted path as the environment variable 'MOUNTED_SHARE_FOLDER'. This is the location where the local PDFs must be stored, whereas the remote PDFs will be saved in the folder 'extract' at that location.

3. The file sparql_config.py allows to easily configure SPARQL prefixes and URIs. In case a single graph for input and a single graph for output is desired, set the environment variables TARGET_GRAPH (input) and/or PUBLICATION_GRAPH (output).

4. Set the environment variable SOURCE to an URL or the name of the city to scrape PDFs from. In case of a city name, only Freiburg and Flemish cities are currently supported. (For the latter, only if their decision PDFs are included in https://lokaalbeslist-harvester-2.s.redhost.be/sparql)
   
## Running
Run the container using 
```
docker compose up -d # run without -d flag when you don't want to run it in the background
```

### Example
Open your local SPARQL query editor (by default configured to run on http://localhost:8890/sparql as set by lblod/app-decide), and run the following query to create a Task to scrape for new PDFs:
```
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX dct:  <http://purl.org/dc/terms/>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
PREFIX nfo:  <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nie:  <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>

INSERT DATA {

  GRAPH <http://mu.semte.ch/graphs/harvesting> {
    <http://data.lblod.info/id/tasks/demo-pdf-scraping>
      a task:Task ;
      mu:uuid "demo-pdf-scraping" ;
      adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> ;
      task:operation <http://lblod.data.gift/id/jobs/concept/TaskOperation/pdf-scraping> ;
      dct:created "2025-10-31T09:00:00Z"^^xsd:dateTime .
  }
}
```

Trigger this task using
```
curl -X POST http://localhost:8080/delta \
  -H "Content-Type: application/json" \
  -d '[
    {
      "inserts": [
        {
          "subject": { "type": "uri", "value": "http://data.lblod.info/id/tasks/demo-pdf-scraping" },
          "predicate": { "type": "uri", "value": "http://www.w3.org/ns/adms#status" },
          "object": { "type": "uri", "value": "http://redpencil.data.gift/id/concept/JobStatus/scheduled" },
          "graph": { "type": "uri", "value": "http://mu.semte.ch/graphs/harvesting" }
        }
      ],
      "deletes": []
    }
  ]'
```
The new PDFs are represented as remote objects in the triple store, grouped within a harvesting collection that belongs to the tasks’s output data container The following SPARQL queries can be used to check the results:

Check the tasks (including data output containers):
```
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>

SELECT ?task ?status ?operation ?resultsContainer
WHERE {
  GRAPH <http://mu.semte.ch/graphs/harvesting> {
    ?task a task:Task ;
          adms:status ?status ;
          task:operation ?operation .

    OPTIONAL { ?task task:resultsContainer ?resultsContainer . }
  }
}
ORDER BY ?task
```

Check the remote objects:
```
PREFIX nfo:  <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nie:  <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
PREFIX mu:   <http://mu.semte.ch/vocabularies/core/>

SELECT ?remoteDataObject ?uuid ?url
FROM <http://mu.semte.ch/graphs/harvesting>
WHERE {
  ?remoteDataObject a nfo:RemoteDataObject ;
                    mu:uuid ?uuid ;
                    nie:url ?url .
}
```

Check the harvesting collection and its parts:
```
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>

SELECT ?harvestCollection ?uuid ?part
FROM <http://mu.semte.ch/graphs/harvesting>
WHERE {
  ?harvestCollection a <http://lblod.data.gift/vocabularies/harvesting/HarvestingCollection> ;
                     mu:uuid ?uuid ;
                     dct:hasPart ?part .
}
```

Check the output data container and its harvesting collection:
```
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>

SELECT ?dataContainer ?uuid ?harvestCollection
FROM <http://mu.semte.ch/graphs/harvesting>
WHERE {
  ?dataContainer a nfo:DataContainer ;
                 mu:uuid ?uuid ;
                 task:hasHarvestingCollection ?harvestCollection .
}
```

