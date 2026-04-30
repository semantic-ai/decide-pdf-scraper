import uuid
from string import Template

from decide_ai_service_base.task import DecisionTask
from decide_ai_service_base.sparql_config import get_prefixes_for_query, GRAPHS, TASK_OPERATIONS

from .scraping_functions import get_all_pdf_links_from_a_url, get_flanders_city_download_urls, get_freiburg_download_urls, is_url
from escape_helpers import sparql_escape_uri, sparql_escape_string
from helpers import query, update


class PdfScrapingTask(DecisionTask):
    """
    Task for scraping new PDF documents for a given source.
    """

    __task_type__ = TASK_OPERATIONS["pdf_scraping"]

    def __init__(self, task_uri: str):
        super().__init__(task_uri)

    def fetch_sources_from_task(self) -> str:
        """
        Retrieve the URL sources (or identifiers) linked to this task.

        This combines fetching the input container and then fetching
        the sources associated with that container.

        Returns:
            list of strings containing the sources to scrape (either a URL, "Freiburg" or a Flemish city name)
        """
        q_source = Template(
            get_prefixes_for_query("task", "dct", "nfo", "nie")
            + """
                SELECT ?source WHERE {
                GRAPH $graphs_jobs {
                    $task task:inputContainer ?container .
                }
                GRAPH $graphs_data_containers {
                    ?container task:hasHarvestingCollection ?collection .
                }
                GRAPH $graphs_harvest_collections {
                    ?collection dct:hasPart ?remote .
                }
                GRAPH $graphs_remote_objects {
                    ?remote a nfo:RemoteDataObject ;
                            nie:url ?source .
                }
                }
            """
        ).substitute(
            prefixes=get_prefixes_for_query("task", "dct", "nfo", "nie"),
            graphs_jobs=sparql_escape_uri(GRAPHS["jobs"]),
            graphs_data_containers=sparql_escape_uri(GRAPHS["data_containers"]),
            graphs_harvest_collections=sparql_escape_uri(GRAPHS["harvest_collections"]),
            graphs_remote_objects=sparql_escape_uri(GRAPHS["remote_objects"]),
            task=sparql_escape_uri(self.task_uri)
        )

        bindings = query(q_source, sudo=True).get(
            "results", {}).get("bindings", [])
        if not bindings:
            raise RuntimeError(
                "No remote files found in harvesting collection")

        sources = [item["source"]["value"] for item in bindings]
        return sources

    @staticmethod
    def get_new_download_urls(urls: list[str], batch_size: int = 20) -> list[str]:
        """
        Return the list of PDF download urls that are not yet present in the triple store.

        Args:
            urls: List of URLs to check.
            batch_size: Maximum number of URLs to include per SPARQL query.
        """
        if not urls:
            return []

        existing_urls = set()

        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            values_clause = " ".join(sparql_escape_uri(u) for u in batch)

            q = Template(
                get_prefixes_for_query("eli") + """
                SELECT ?url WHERE {
                    GRAPH $graphs_manifestations {
                        VALUES ?url { $values_clause }
                        ?manifestation a eli:Manifestation ;
                                    eli:is_exemplified_by ?url .  
                    }
                }
                """
            ).substitute(
                graphs_manifestations=sparql_escape_uri(GRAPHS['manifestations']),
                values_clause=values_clause
            )

            results = query(q, sudo=True)
            existing_urls.update(
                b.get("url", {}).get("value")
                for b in results.get("results", {}).get("bindings", [])
                if b.get("url", {}).get("value")
            )

        missing_urls = [u for u in urls if u not in existing_urls]
        return missing_urls

    @staticmethod
    def create_remote_data_object(url: str) -> str:
        """
        Function to create a single remote data object
        for a given PDF downloadURL.

        Args:
            url: The download URL of the PDF

        Returns:
            The created remote data object URI
        """
        remote_object_uuid = str(uuid.uuid4())
        remote_object_uri = f"http://lblod.data.gift/id/remote-data-objects/{remote_object_uuid}"

        q = Template(
            get_prefixes_for_query("nfo", "mu", "nie")
            + """
            INSERT DATA {
            GRAPH $graphs_remote_objects {
                $obj a nfo:RemoteDataObject ;
                    mu:uuid $uuid ;
                    nie:url	$url .
            }
            }
            """
        ).substitute(
            graphs_remote_objects=sparql_escape_uri(GRAPHS["remote_objects"]),
            obj=sparql_escape_uri(remote_object_uri),
            uuid=sparql_escape_string(remote_object_uuid),
            url=sparql_escape_uri(url)
        )

        update(q, sudo=True)

        return remote_object_uri

    @staticmethod
    def create_harvest_collection(remote_object_uris: list[str]) -> str:
        """
        Function to create a single harvesting collection.

        Args:
            remote_object_uris: List of remote data object URIs to include in the collection.

        Returns:
            The created harvesting collection URI.
        """
        harvest_uuid = str(uuid.uuid4())
        harvest_uri = f"http://lblod.data.gift/id/harvest-collections/{harvest_uuid}"

        parts = ", ".join(sparql_escape_uri(uri) for uri in remote_object_uris)

        q = Template(
            get_prefixes_for_query("mu", "dct", "harvesting")
            + """
            INSERT DATA {
            GRAPH $graphs_harvest_collections {
                $harvest a harvesting:HarvestingCollection ;
                    mu:uuid $uuid ;
                    dct:hasPart $parts .
            }
            }
            """
        ).substitute(
            graphs_harvest_collections=sparql_escape_uri(GRAPHS["harvest_collections"]),
            harvest=sparql_escape_uri(harvest_uri),
            uuid=sparql_escape_string(harvest_uuid),
            parts=parts,
        )

        update(q, sudo=True)
        return harvest_uri

    @staticmethod
    def create_data_container(harvest_collection_uri: str) -> str:
        """
        Function to create an output data container containg the harvesting collection.

        Args:
            harvest_collection_uri: URI of the harvesting collection to include in the data container.

        Returns:
            The created data container URI.
        """
        container_uuid = str(uuid.uuid4())
        container_uri = f"http://data.lblod.info/id/data-container/{container_uuid}"

        q = Template(
            get_prefixes_for_query("nfo", "mu", "task")
            + """
            INSERT DATA {
            GRAPH $graphs_data_containers {
                $container a nfo:DataContainer ;
                    mu:uuid $uuid ;
                    task:hasHarvestingCollection $harvest .
            }
            }
            """
        ).substitute(
            graphs_data_containers=sparql_escape_uri(GRAPHS["data_containers"]),
            container=sparql_escape_uri(container_uri),
            uuid=sparql_escape_string(container_uuid),
            harvest=sparql_escape_uri(harvest_collection_uri),
        )

        update(q, sudo=True)
        return container_uri

    def process(self):
        """
        Implementation of Task's process function that
        - gathers all PDF download URLs from a source
          (either a URL, Freiburg or a Flemish city, depending on the SOURCE environment variable)
        - checks which URLs are not yet present in the triplestore
        - creates remote data objects for the missing URLs
        - creates a harvesting collection containing these remote data objects
        - creates a data container containing the harvesting collection
        """
        sources = self.fetch_sources_from_task()
        for source in sources:
            if is_url(source):
                download_urls = get_all_pdf_links_from_a_url(source)
            elif source.lower() == "freiburg":
                download_urls = get_freiburg_download_urls()
            else:
                download_urls = get_flanders_city_download_urls(source)

            missing_download_urls = self.get_new_download_urls(download_urls)

            if missing_download_urls:
                remote_objects = []
                for url in missing_download_urls:
                    remote_objects.append(self.create_remote_data_object(url))

                harvest_collection_uri = self.create_harvest_collection(remote_objects)
                container_uri = self.create_data_container(harvest_collection_uri)
                self.results_container_uris.append(container_uri)
