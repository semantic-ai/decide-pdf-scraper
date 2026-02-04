import requests


def get_freiburg_download_urls(
    base_meeting_endpoint: str = "https://ris.freiburg.de/oparl/body/FR/meeting/page/"
) -> list[str]:
    """
    Scrape the Freiburg OParl endpoint to gather PDF download URLs
    for meeting resolution files.

    Args:
        base_meeting_endpoint: The base URL for the meeting pages.

    Returns:
        A list of all PDF download URLs.
    """
    first_page_url = base_meeting_endpoint + "1"
    meeting_data = requests.get(first_page_url).json()
    total_pages = meeting_data.get("pagination", {}).get("totalPages", 1)

    download_urls = []
    for page in range(1, total_pages + 1):
        page_url = base_meeting_endpoint + str(page)
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
                        if len(download_urls) >= 11:
                            break
            if len(download_urls) >= 11:
                break

        if len(download_urls) >= 11:
            break

    return download_urls
