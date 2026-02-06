import requests
from string import Template
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from .sparql_config import get_prefixes_for_query


def get_freiburg_download_urls(
    base_endpoint: str = "https://ris.freiburg.de/oparl/body/FR/meeting/page/"
) -> list[str]:
    """
    Scrape the Freiburg OParl endpoint to gather PDF download URLs
    for meeting resolution files.

    Args:
        base_endpoint: The base URL for the meeting pages.

    Returns:
        A list of all PDF download URLs.
    """
    first_page_url = base_endpoint + "1"
    meeting_data = requests.get(first_page_url).json()
    total_pages = meeting_data.get("pagination", {}).get("totalPages", 1)

    download_urls = []
    for page in range(1, total_pages + 1):
        page_url = base_endpoint + str(page)
        page_data = requests.get(page_url).json()

        meetings_on_page = page_data.get("data", [])

        for meeting in meetings_on_page:
            meeting_agenda_items = meeting.get("agendaItem", [])
            for agenda_item in meeting_agenda_items:
                resolution_file = agenda_item.get("resolutionFile", {})
                if resolution_file:
                    download_url = resolution_file.get("downloadUrl", "")
                    if download_url:
                        download_urls.append(download_url)

    return download_urls


def get_flanders_city_download_urls(
    city: str,
    base_endpoint: str = "https://lokaalbeslist-harvester-2.s.redhost.be/sparql"
) -> list[str]:
    """
    Fetch PDF URLs for decisions of a given city.
    Args:
        city: The city to filter the decisions by.
        base_endpoint: The SPARQL endpoint to query.
    Returns:
        A list of PDF download URLs for the given city.
    """

    download_urls = []
    offset = 0
    HEADERS = {"Accept": "application/sparql-results+json"}

    while True:
        q = Template(
            get_prefixes_for_query("prov", "besluit") +
            f"""
            SELECT DISTINCT ?notulepdf WHERE {{
              ?s a besluit:Besluit ;
                 prov:value ?notulepdf .
              FILTER(
                STRSTARTS(STR(?notulepdf), "https://lblod.{city}") &&
                CONTAINS(STR(?notulepdf), "pdf")
              )
            }}
            OFFSET $offset
            LIMIT 1000
            """
        ).substitute(offset=offset)

        response = requests.get(
            base_endpoint, params={"query": q}, headers=HEADERS)

        data = response.json()
        bindings = data.get("results", {}).get("bindings", [])

        if not bindings:
            break

        download_urls.extend(b["notulepdf"]["value"] for b in bindings)

        offset += 1000

    return download_urls


def get_all_pdf_links_from_a_url(url: str) -> list[str]:
    """
    Fetch all PDF links from a given URL.

    Args:
        url: The URL to scrape for PDF links.

    Returns:
        A list of PDF links found on the page.
    """
    if url.endswith(".pdf"):
        return [url]
    else:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")

        pdf_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                full_url = urljoin(url, href)
                pdf_links.append(full_url)

        return pdf_links


def is_url(string: str) -> bool:
    """Check if a string is a valid URL.

    Args:
        string: The string to check.

    Returns:
        True if the string is a valid URL, False otherwise."""
    try:
        result = urlparse(string)
        return all([result.scheme, result.netloc])
    except:
        return False
