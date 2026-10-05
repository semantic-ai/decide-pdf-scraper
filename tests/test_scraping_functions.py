import pytest
import requests

from src import scraping_functions
from src.scraping_functions import (
    get_all_pdf_links_from_a_url,
    get_flanders_city_download_urls,
    get_freiburg_download_urls,
    is_url,
)


class FakeResponse:
    def __init__(self, json_data=None, headers=None, text=""):
        self._json = json_data
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._json

    def raise_for_status(self):
        pass


@pytest.fixture
def fake_get(monkeypatch):
    """Route ``requests.get`` to ``respond(url, params)``; returns the list of calls."""
    def install(respond):
        calls = []

        def get(url, params=None, headers=None):
            calls.append((url, params))
            return respond(url, params)

        monkeypatch.setattr(scraping_functions.requests, "get", get)
        return calls
    return install


class TestGetAllPdfLinksFromAUrl:
    def test_a_pdf_url_is_returned_without_a_request(self, fake_get):
        calls = fake_get(lambda url, params: pytest.fail("unexpected request"))

        assert get_all_pdf_links_from_a_url("https://example.org/a.pdf") == [
            "https://example.org/a.pdf"
        ]
        assert calls == []

    def test_a_pdf_content_type_returns_the_url_itself(self, fake_get):
        fake_get(lambda url, params: FakeResponse(
            headers={"Content-Type": "Application/PDF"}
        ))

        assert get_all_pdf_links_from_a_url("https://example.org/download?id=7") == [
            "https://example.org/download?id=7"
        ]

    def test_a_request_error_returns_no_links(self, fake_get):
        def respond(url, params):
            raise requests.ConnectionError("down")

        fake_get(respond)

        assert get_all_pdf_links_from_a_url("https://example.org/page") == []


class TestGetFreiburgDownloadUrls:
    def test_collects_present_download_urls_from_every_page(self, fake_get):
        page_1 = {
            "pagination": {"totalPages": 2},
            "data": [{
                "agendaItem": [
                    {"resolutionFile": {"downloadUrl": "https://ris.example/f/1.pdf"}},
                    {},
                    {"resolutionFile": {}},
                ],
            }],
        }
        page_2 = {
            "data": [{
                "agendaItem": [
                    {"resolutionFile": {"downloadUrl": "https://ris.example/f/2.pdf"}},
                ],
            }],
        }
        fake_get(lambda url, params: FakeResponse(page_1 if url.endswith("1") else page_2))

        assert get_freiburg_download_urls("https://ris.example/page/") == [
            "https://ris.example/f/1.pdf",
            "https://ris.example/f/2.pdf",
        ]


class TestGetFlandersCityDownloadUrls:
    def test_pages_in_steps_of_1000_until_an_empty_page(self, fake_get):
        def binding(url):
            return {"notulepdf": {"value": url}}

        def respond(url, params):
            if "OFFSET 0" in params["query"]:
                bindings = [binding("https://lblod.gent/a.pdf"), binding("https://lblod.gent/b.pdf")]
            elif "OFFSET 1000" in params["query"]:
                bindings = [binding("https://lblod.gent/c.pdf")]
            else:
                bindings = []
            return FakeResponse({"results": {"bindings": bindings}})

        calls = fake_get(respond)

        assert get_flanders_city_download_urls("gent", "https://harvester.example/sparql") == [
            "https://lblod.gent/a.pdf",
            "https://lblod.gent/b.pdf",
            "https://lblod.gent/c.pdf",
        ]
        assert len(calls) == 3
        assert "https://lblod.gent" in calls[0][1]["query"]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("https://ris.freiburg.de/x", True),
        ("Freiburg", False),
        ("gent", False),
        ("", False),
    ],
)
def test_is_url_requires_a_scheme_and_a_host(value, expected):
    assert is_url(value) is expected
