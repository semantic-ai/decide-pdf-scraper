from . import tasks

TASK_URI = "http://example.org/test/task/scraping"


def test_scraping_records_every_pdf_link_on_the_page(pdf_site):
    tasks.seed("scraping")

    tasks.run(TASK_URI)

    assert tasks.status(TASK_URI) == tasks.SUCCESS
    assert tasks.scraped_urls(TASK_URI) == {
        "http://127.0.0.1:8000/docs/besluit-1.pdf",
        "https://other.example.org/besluit-2.PDF",
    }
