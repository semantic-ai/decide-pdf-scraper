import pytest
from decide_ai_service_base.sparql_config import GRAPHS

from . import tasks

TASK_URI = "http://example.org/test/task/no-collection"
NO_SOURCES_MESSAGE = "No remote files found in harvesting collection"

ERROR_MESSAGES = """
SELECT ?message WHERE {
  GRAPH <%s> {
    ?error a <http://mu.semte.ch/vocabularies/ext/ErrorMessage> ;
           <http://purl.org/dc/terms/description> ?message
  }
}
"""


def test_a_task_without_remote_sources_fails_and_logs_an_error():
    tasks.seed("no_collection")

    with pytest.raises(RuntimeError, match=NO_SOURCES_MESSAGE):
        tasks.run(TASK_URI)

    assert tasks.status(TASK_URI) == tasks.FAILED
    rows = tasks.bindings(ERROR_MESSAGES % GRAPHS["data_containers"])
    assert any(NO_SOURCES_MESSAGE in r["message"]["value"] for r in rows), rows
