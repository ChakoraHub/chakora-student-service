import json
import os
import urllib.error
import urllib.parse
import urllib.request

import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# Reads the same env var the existing student-registration workflow sets.
BASE_URL = os.getenv(
    "CHAKORAHUB_BASE_URL",
    os.getenv("BASE_URL", "https://www.chakorahub.com")
).rstrip("/")

WAIT = 25


@pytest.fixture
def driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,1000")

    browser = webdriver.Chrome(options=options)
    browser.set_page_load_timeout(45)

    try:
        yield browser
    finally:
        browser.quit()


def wait_loaded(browser):
    WebDriverWait(browser, WAIT).until(
        lambda d: d.execute_script(
            "return document.readyState"
        ) == "complete"
    )


def get_test_subject():
    """
    Mirrors get_test_subject() from test_admin_upload_practice_tests.py:
    pull a real course/subject from GET /api/courses instead of
    hardcoding one, so this smoke test doesn't break across environments.
    """
    with urllib.request.urlopen(f"{BASE_URL}/api/courses", timeout=15) as resp:
        courses = json.loads(resp.read().decode("utf-8"))
    assert isinstance(courses, list) and courses, (
        "GET /api/courses returned no courses -- cannot pick a subject "
        "for the smoke test."
    )
    first = courses[0]
    name = first.get("course_name") or first.get("COURSE_NAME")
    assert name, f"Course record has no name field: {first}"
    return name


def test_practice_test_page_opens_without_login(driver):
    """Verify the public Practice Tests page loads without signing in.
    Mirrors blogger_smoke.py's test_public_blog_opens_without_login."""
    subject = get_test_subject()
    driver.get(f"{BASE_URL}/practice-test/{urllib.parse.quote(subject)}")
    wait_loaded(driver)

    heading = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located((By.TAG_NAME, "h2"))
    )
    assert subject in heading.text
    assert "Practice Tests" in heading.text

    # Either the empty-state message or real file links must be present.
    no_files = driver.find_elements(By.CLASS_NAME, "no-files")
    file_links = driver.find_elements(By.CSS_SELECTOR, "li a")
    assert no_files or file_links, (
        "Neither the empty-state message nor any file links were found."
    )


def test_maintenance_notice_visible_when_expected(driver):
    """
    Check the maintenance banner when the workflow expects it to be enabled.

    The banner comes from practice_test_maintenance_notice.html, which asks
    the browser to read /api/student/maintenance/status and then reveals
    #practice-test-maintenance-notice when maintenance_mode is true.
    """
    expected = os.getenv(
        "EXPECT_MAINTENANCE_NOTICE",
        "false"
    ).lower() == "true"

    if not expected:
        pytest.skip(
            "Maintenance notice is not expected for this run."
        )

    subject = get_test_subject()
    driver.get(f"{BASE_URL}/practice-test/{urllib.parse.quote(subject)}")
    wait_loaded(driver)

    notice = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.ID, "practice-test-maintenance-notice")
        )
    )

    assert "Scheduled maintenance notice" in notice.text


# ---------------------------------------------------------------------------
# Additional coverage (edge cases, error handling, security, negative banner)
# ---------------------------------------------------------------------------

def http_status(url):
    """GET a URL without a browser and return its HTTP status code."""
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


def get_course_names():
    """All subject names from GET /api/courses (same source as get_test_subject)."""
    with urllib.request.urlopen(f"{BASE_URL}/api/courses", timeout=15) as resp:
        courses = json.loads(resp.read().decode("utf-8"))
    names = [
        c.get("course_name") or c.get("COURSE_NAME") for c in courses
    ]
    return [n for n in names if n]


def test_unknown_subject_shows_empty_state_not_error(driver):
    """
    Edge case: /practice-test/<subject> does not validate the subject, so a
    subject with no upload folder (here also containing spaces) must render
    the normal empty-state page instead of a 500.
    """
    subject = "Nonexistent Subject 99"
    url = f"{BASE_URL}/practice-test/{urllib.parse.quote(subject)}"
    assert http_status(url) < 500

    driver.get(url)
    wait_loaded(driver)

    heading = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located((By.TAG_NAME, "h2"))
    )
    assert subject in heading.text

    no_files = driver.find_element(By.CLASS_NAME, "no-files")
    assert "No practice tests uploaded yet." in no_files.text
    assert not driver.find_elements(By.CSS_SELECTOR, "li a")


def test_subject_with_html_characters_is_escaped(driver):
    """
    Security/edge case: the subject comes straight from the URL, so HTML in it
    must be shown as literal text and never become a real element.
    """
    payload = "<b id='xss-probe'>x</b>"
    driver.get(
        f"{BASE_URL}/practice-test/{urllib.parse.quote(payload, safe='')}"
    )
    wait_loaded(driver)

    heading = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located((By.TAG_NAME, "h2"))
    )
    assert payload in heading.text          # shown as literal text
    assert "&lt;b id=" in driver.page_source  # and escaped in the HTML source
    assert not driver.find_elements(By.ID, "xss-probe")  # never a real element


def test_practice_test_file_links_open(driver):
    """
    Positive/data check: every listed file link must point at
    /uploads/practice-tests/<subject>/ and actually be downloadable.
    Skips if none of the first 10 subjects has an uploaded file yet.
    """
    for subject in get_course_names()[:10]:
        driver.get(f"{BASE_URL}/practice-test/{urllib.parse.quote(subject)}")
        wait_loaded(driver)
        links = driver.find_elements(By.CSS_SELECTOR, "li a")
        if links:
            break
    else:
        pytest.skip("No subject has practice test files uploaded yet.")

    for link in links[:3]:
        href = link.get_attribute("href")
        assert "/uploads/practice-tests/" in href, href
        assert http_status(href) == 200, f"File link is broken: {href}"


def test_invalid_practice_file_requests_are_rejected():
    """
    Error handling/security: a missing file and a path-traversal attempt must
    never return content (no 200) and must not crash the server (no 5xx).
    """
    subject = urllib.parse.quote(get_test_subject())
    for name in ("does-not-exist-xyz.pdf", "..%2F..%2Fapp.py"):
        status = http_status(
            f"{BASE_URL}/uploads/practice-tests/{subject}/{name}"
        )
        assert status in (400, 403, 404), (
            f"Expected a client-error status for '{name}', got {status}."
        )


