from . import tasks


def test_scraping_records_every_pdf_link_on_the_page(pdf_site):
    task_uri = "http://example.org/test/task/scraping"
    tasks.seed("scraping")

    tasks.run(task_uri)

    assert tasks.status(task_uri) == tasks.SUCCESS
    assert tasks.scraped_urls(task_uri) == {
        "http://127.0.0.1:8000/docs/besluit-1.pdf",
        "https://other.example.org/besluit-2.PDF",
    }


def test_pdfs_that_already_have_a_manifestation_are_skipped(pdf_site):
    task_uri = "http://example.org/test/task/known-pdf"
    tasks.seed("known_pdf")

    tasks.run(task_uri)

    assert tasks.status(task_uri) == tasks.SUCCESS
    assert tasks.scraped_urls(task_uri) == {"https://other.example.org/besluit-2.PDF"}
