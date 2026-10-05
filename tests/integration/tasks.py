"""Seed tasks and run them the way the service does."""
import pathlib
from collections import defaultdict

from decide_ai_service_base.sparql_config import GRAPHS, JOB_STATUSES
from decide_ai_service_base.task import Task
from helpers import query, update
from rdflib import Graph

import src.task  # noqa: F401  registers PdfScrapingTask

FIXTURES = pathlib.Path(__file__).parent / "fixtures"

SUCCESS = JOB_STATUSES["success"]
FAILED = JOB_STATUSES["failed"]

# The graph the service's queries read each fixture subject from.
GRAPH_BY_SUBJECT_PREFIX = {
    "http://example.org/test/task/": "jobs",
    "http://example.org/test/container/": "data_containers",
    "http://example.org/test/collection/": "harvest_collections",
    "http://example.org/test/remote/": "remote_objects",
    "http://example.org/test/manifestation/": "manifestations",
}

INSERT_INTO_GRAPH = "INSERT DATA { GRAPH <%s> { %s } }"

TASK_STATUS = """
SELECT ?status WHERE {
  GRAPH <%s> { <%s> <http://www.w3.org/ns/adms#status> ?status }
}
"""

SCRAPED_URLS = """
SELECT ?url WHERE {
  GRAPH <%s> { <%s> <http://redpencil.data.gift/vocabularies/tasks/resultsContainer> ?container }
  GRAPH <%s> { ?container <http://redpencil.data.gift/vocabularies/tasks/hasHarvestingCollection> ?collection }
  GRAPH <%s> { ?collection <http://purl.org/dc/terms/hasPart> ?remote }
  GRAPH <%s> { ?remote <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#url> ?url }
}
"""


def bindings(sparql: str) -> list[dict]:
    return query(sparql, sudo=True)["results"]["bindings"]


def _graph_for(subject: str) -> str:
    for prefix, key in GRAPH_BY_SUBJECT_PREFIX.items():
        if subject.startswith(prefix):
            return key
    raise ValueError(f"no graph for fixture subject {subject}")


def seed(fixture_name: str) -> None:
    buckets = defaultdict(Graph)
    for triple in Graph().parse(FIXTURES / f"{fixture_name}.ttl"):
        buckets[_graph_for(str(triple[0]))].add(triple)

    for key, data in buckets.items():
        update(INSERT_INTO_GRAPH % (GRAPHS[key], data.serialize(format="nt")), sudo=True)


def run(task_uri: str) -> None:
    Task.from_uri(task_uri).execute()


def status(task_uri: str) -> str:
    rows = bindings(TASK_STATUS % (GRAPHS["jobs"], task_uri))
    assert len(rows) == 1, f"expected one status for {task_uri}, got {len(rows)}"
    return rows[0]["status"]["value"]


def scraped_urls(task_uri: str) -> set[str]:
    rows = bindings(SCRAPED_URLS % (GRAPHS["jobs"], task_uri, GRAPHS["data_containers"],
                                    GRAPHS["harvest_collections"], GRAPHS["remote_objects"]))
    return {row["url"]["value"] for row in rows}
