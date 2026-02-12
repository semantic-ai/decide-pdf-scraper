import uuid
import logging
import contextlib
from string import Template
from typing import Optional, Type
from abc import ABC, abstractmethod

from .scraping_functions import get_all_pdf_links_from_a_url, get_flanders_city_download_urls, get_freiburg_download_urls, is_url
from .sparql_config import TASK_OPERATIONS, get_prefixes_for_query, GRAPHS, JOB_STATUSES
from escape_helpers import sparql_escape_uri, sparql_escape_string
from helpers import query, update


class Task(ABC):
    """Base class for background tasks that process data from the triplestore."""

    def __init__(self, task_uri: str):
        super().__init__()
        self.task_uri = task_uri
        self.results_container_uris = []
        self.logger = logging.getLogger(self.__class__.__name__)

    @classmethod
    def supported_operations(cls) -> list[Type['Task']]:
        all_ops = []
        for subclass in cls.__subclasses__():
            if hasattr(subclass, '__task_type__'):
                all_ops.append(subclass)
            else:
                all_ops.extend(subclass.supported_operations())
        return all_ops

    @classmethod
    def lookup(cls, task_type: str) -> Optional['Task']:
        """
        Yield all subclasses of the given class, per:
        """
        for subclass in cls.supported_operations():
            if hasattr(subclass, '__task_type__') and subclass.__task_type__ == task_type:
                return subclass
        return None

    @classmethod
    def from_uri(cls, task_uri: str) -> 'Task':
        """Create a Task instance from its URI in the triplestore."""
        q = Template(
            get_prefixes_for_query("adms", "task") +
            """
            SELECT ?task ?taskType WHERE {
              ?task task:operation ?taskType .
              BIND($uri AS ?task)
            }
        """).substitute(uri=sparql_escape_uri(task_uri))
        for b in query(q, sudo=True).get('results').get('bindings'):
            candidate_cls = cls.lookup(b['taskType']['value'])
            if candidate_cls is not None:
                return candidate_cls(task_uri)
            raise RuntimeError(
                "Unknown task type {0}".format(b['taskType']['value']))
        raise RuntimeError("Task with uri {0} not found".format(task_uri))

    def change_state(self, old_state: str, new_state: str, results_container_uris: list = []) -> None:
        """Update the task status in the triplestore."""
        query_template = Template(
            get_prefixes_for_query("task", "adms") +
            """
            DELETE {
            GRAPH <""" + GRAPHS["jobs"] + """> {
                ?task adms:status ?oldStatus .
            }
            }
            INSERT {
            GRAPH <""" + GRAPHS["jobs"] + """> {
                ?task
                $results_container_line
                adms:status <$new_status> .

            }
            }
            WHERE {
            GRAPH <""" + GRAPHS["jobs"] + """> {
                BIND($task AS ?task)
                BIND(<$old_status> AS ?oldStatus)
                OPTIONAL { ?task adms:status ?oldStatus . }
            }
            }
            """)

        results_container_line = ""
        if results_container_uris:
            results_container_line = "\n".join(
                [f"task:resultsContainer {sparql_escape_uri(uri)} ;" for uri in results_container_uris])

        query_string = query_template.substitute(
            new_status=JOB_STATUSES[new_state],
            old_status=JOB_STATUSES[old_state],
            task=sparql_escape_uri(self.task_uri),
            results_container_line=results_container_line)

        update(query_string, sudo=True)

    @contextlib.contextmanager
    def run(self):
        """Context manager for task execution with state transitions."""
        self.change_state("scheduled", "busy")
        yield
        self.change_state("busy", "success", self.results_container_uris)

    def execute(self):
        """Run the task and handle state transitions."""
        with self.run():
            self.process()

    @abstractmethod
    def process(self):
        """Process task data (implemented by subclasses)."""
        pass


class PdfScrapingTask(Task, ABC):
    """
    Task for scraping new PDF documents for a given source.
    """

    __task_type__ = TASK_OPERATIONS["pdf_scraping"]

    def __init__(self, task_uri: str):
        super().__init__(task_uri)

    def fetch_source_from_task(self) -> str:
        """
        Retrieve the source URL (or identifier) linked to this task.

        This combines fetching the input container and then fetching
        the source associated with that container.

        Returns:
            string containing the source to scrape (either a URL, "Freiburg" or a Flemish city name)
        """

        q_container = Template(
            get_prefixes_for_query("task") +
            f"""
            SELECT ?container WHERE {{
            GRAPH <{GRAPHS["jobs"]}> {{
                BIND($task AS ?task)
                ?task task:inputContainer ?container .
            }}
            }}
            """
        ).substitute(task=sparql_escape_uri(self.task_uri))

        bindings = query(q_container, sudo=True).get(
            "results", {}).get("bindings", [])
        if not bindings:
            raise RuntimeError(
                f"No input container found for task {self.task_uri}")

        container_uri = bindings[0]["container"]["value"]

        q_source = f"""
            {get_prefixes_for_query("task", "dct", "nfo", "nie")}
            SELECT ?source WHERE {{
            GRAPH <{GRAPHS["data_containers"]}> {{
                <{container_uri}> task:hasHarvestingCollection ?collection .
            }}
            GRAPH <{GRAPHS["harvest_collections"]}> {{
                ?collection dct:hasPart ?remote .
            }}
            GRAPH <{GRAPHS["remote_objects"]}> {{
                ?remote a nfo:RemoteDataObject ;
                        nie:url ?source .
            }}
            }}
            """

        bindings = query(q_source, sudo=True).get(
            "results", {}).get("bindings", [])
        if not bindings:
            raise RuntimeError(
                "No remote files found in harvesting collection")

        source = bindings[0]["source"]["value"]
        return source

    def get_new_download_urls(self, urls: list[str], batch_size: int = 20) -> list[str]:
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
                get_prefixes_for_query("eli") + f"""
                SELECT ?url WHERE {{
                    GRAPH <{GRAPHS['manifestations']}> {{
                        ?manifestation a eli:Manifestation ;
                                    eli:is_exemplified_by ?url .
                        VALUES ?url {{ {values_clause} }}
                    }}
                }}
                """
            ).substitute()

            results = query(q, sudo=True)
            existing_urls.update(
                b.get("url", {}).get("value")
                for b in results.get("results", {}).get("bindings", [])
                if b.get("url", {}).get("value")
            )

        missing_urls = [u for u in urls if u not in existing_urls]
        return missing_urls

    def create_remote_data_object(self, url: str) -> str:
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
            + f"""
            INSERT DATA {{
            GRAPH <{GRAPHS["remote_objects"]}> {{
                $obj a nfo:RemoteDataObject ;
                    mu:uuid $uuid ;
                    nie:url	$url .
            }}
            }}
            """
        ).substitute(
            obj=sparql_escape_uri(remote_object_uri),
            uuid=sparql_escape_string(remote_object_uuid),
            url=sparql_escape_uri(url)
        )

        update(q, sudo=True)

        return remote_object_uri

    def create_harvest_collection(self, remote_object_uris: list[str]) -> str:
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
            + f"""
            INSERT DATA {{
            GRAPH <{GRAPHS["harvest_collections"]}> {{
                $harvest a harvesting:HarvestingCollection ;
                    mu:uuid $uuid ;
                    dct:hasPart {parts} .
            }}
            }}
            """
        ).substitute(
            harvest=sparql_escape_uri(harvest_uri),
            uuid=sparql_escape_string(harvest_uuid),
        )

        update(q, sudo=True)
        return harvest_uri

    def create_data_container(self, harvest_collection_uri: str) -> str:
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
            + f"""
            INSERT DATA {{
            GRAPH <{GRAPHS["data_containers"]}> {{
                $container a nfo:DataContainer ;
                    mu:uuid $uuid ;
                    task:hasHarvestingCollection $harvest .
            }}
            }}
            """
        ).substitute(
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
        source = self.fetch_source_from_task()

        if is_url(source):
            download_urls = get_all_pdf_links_from_a_url(source)
        elif source.lower() == "freiburg":
            download_urls = get_freiburg_download_urls()
        else:
            download_urls = get_flanders_city_download_urls(source)

        missing_download_urls = self.get_new_download_urls(download_urls)

        if missing_download_urls != []:
            remote_objects = []
            for url in missing_download_urls:
                remote_objects.append(self.create_remote_data_object(url))

            harvest_collection_uri = self.create_harvest_collection(
                remote_objects)

            container_uri = self.create_data_container(harvest_collection_uri)

            self.results_container_uris.append(container_uri)
