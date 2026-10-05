import functools
import http.server
import os
import threading
import time

import pytest
import requests

from .tasks import FIXTURES

ENDPOINT = os.environ["MU_SPARQL_ENDPOINT"]

SITE_PORT = 8000


@pytest.fixture(scope="session", autouse=True)
def virtuoso():
    deadline = time.monotonic() + 180
    while True:
        try:
            requests.post(
                ENDPOINT, data={"query": "ASK { ?s ?p ?o }"}, timeout=10
            ).raise_for_status()
            return
        except requests.RequestException:
            if time.monotonic() > deadline:
                raise
            time.sleep(2)


@pytest.fixture(scope="session")
def pdf_site():
    """Serve the fixtures directory on 127.0.0.1:8000, where the scraper fetches it."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=FIXTURES)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", SITE_PORT), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield
    server.shutdown()
