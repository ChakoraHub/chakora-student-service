import os
import tempfile

import pytest
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = os.getenv(
    "CHAKORAHUB_BASE_URL",
    os.getenv("BASE_URL", "https://www.chakorahub.com")
).rstrip("/")

UPLOAD_URL = f"{BASE_URL}/admin/upload"
WAIT = 25

# Confirmed from upload.html: the 5 buttons that switch the upload type.
UPLOAD_ACTIONS = {"ppt", "interview", "syllabus", "code", "practice"}


@pytest.fixture
def driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,1600")

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


def safe_click(browser, element):
    """
    Click robustly: centre the element (keeps it clear of the sticky header and
    of layout shifts), wait until it is clickable, and fall back to a JS click
    if something still intercepts the pointer event.
    """
    browser.execute_script(
        "arguments[0].scrollIntoView({block: 'center', inline: 'nearest'});",
        element,
    )
    try:
        WebDriverWait(browser, 5).until(EC.element_to_be_clickable(element)).click()
    except ElementClickInterceptedException:
        browser.execute_script("arguments[0].click();", element)


def login_employee(browser):
    """Same login steps as test_blogger_smoke.py."""
    emp_id = os.getenv("UPLOAD_EMPLOYEE_ID")
    password = os.getenv("UPLOAD_EMPLOYEE_PASSWORD")
    if not emp_id or not password:
        pytest.skip(
            "Configure UPLOAD_EMPLOYEE_ID and UPLOAD_EMPLOYEE_PASSWORD "
            "for Admin Upload tests."
        )

    browser.get(BASE_URL)
    wait_loaded(browser)
    Select(browser.find_element(By.ID, "login_type")).select_by_value("employee")
    WebDriverWait(browser, WAIT).until(
        EC.visibility_of_element_located((By.ID, "employee_id"))
    )
    browser.find_element(By.ID, "employee_id").send_keys(emp_id)
    browser.find_element(By.ID, "password").send_keys(password)
    browser.find_element(By.CSS_SELECTOR, "form button[type='submit']").click()
    wait_loaded(browser)


def open_upload_page(browser):
    login_employee(browser)
    browser.get(UPLOAD_URL)
    wait_loaded(browser)
    WebDriverWait(browser, WAIT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "page-title"))
    )
    # The Existing Courses panel and category dropdowns fill in asynchronously
    # from /api/courses and shift the layout; wait for that to settle so
    # clicks land where the elements actually are.
    WebDriverWait(browser, WAIT).until(
        lambda d: not d.find_elements(
            By.CSS_SELECTOR, "#existingCoursesList .fa-spinner"
        )
    )


def test_admin_upload_requires_login(driver):
    """Without a session, /admin/upload must be rejected (403 Access denied)."""
    driver.get(UPLOAD_URL)
    wait_loaded(driver)
    assert "Access denied" in driver.page_source


def test_admin_upload_page_loads_for_employee(driver):
    """Verify the Admin Upload page and its main upload form are present."""
    open_upload_page(driver)

    assert driver.find_element(By.CLASS_NAME, "page-title").text == "Upload Center"
    assert driver.find_element(By.ID, "unifiedUploadForm").is_displayed()
    assert driver.find_element(By.ID, "unifiedCategory").is_displayed()

    buttons = driver.find_elements(
        By.CSS_SELECTOR, "#uploadActionSwitch .upload-action-btn"
    )
    assert {b.get_attribute("data-action") for b in buttons} == UPLOAD_ACTIONS


def test_maintenance_notice_visible_when_expected(driver):
    """
    Check the maintenance banner when the workflow expects it to be enabled.
    Skip this check during normal operation.

    The banner comes from admin_upload_maintenance_notice.html, which asks
    the browser to read /api/student/maintenance/status and then reveals
    #admin-upload-maintenance-notice when maintenance_mode is true.
    """
    expected = os.getenv(
        "EXPECT_MAINTENANCE_NOTICE",
        "false"
    ).lower() == "true"

    if not expected:
        pytest.skip("Maintenance notice is not expected for this run.")

    open_upload_page(driver)

    notice = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.ID, "admin-upload-maintenance-notice")
        )
    )

    assert "Scheduled maintenance notice" in notice.text


# ---------------------------------------------------------------------------
# Additional coverage (validation, boundaries, permissions, error handling)
# ---------------------------------------------------------------------------

ORG_DOC_TYPES = ("hr_policy", "legal", "finance", "operations")

# Endpoint each upload-action button must point the unified form at.
ACTION_ENDPOINTS = {
    "ppt": "/admin/upload_file/ppt",
    "interview": "/admin/upload_file/interview",
    "syllabus": "/admin/upload_file/syllabus",
    "code": "/admin/upload_file/code",
    "practice": "/upload_practice_test",
}

WRITE_TESTS = os.getenv("RUN_WRITE_TESTS", "false").lower() == "true"


def select_first_category(browser):
    """Wait for /api/courses to fill #unifiedCategory, then pick the first real option."""
    select = Select(browser.find_element(By.ID, "unifiedCategory"))
    WebDriverWait(browser, WAIT).until(
        lambda d: len(Select(d.find_element(By.ID, "unifiedCategory")).options) > 1
    )
    select.select_by_index(1)


def click_action(browser, action):
    safe_click(browser, browser.find_element(
        By.CSS_SELECTOR,
        f"#uploadActionSwitch .upload-action-btn[data-action='{action}']",
    ))


def make_temp_file(suffix):
    handle = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    handle.write(b"smoke test file")
    handle.close()
    return handle.name


def submit_and_get_flash(browser, form_css="#unifiedUploadForm", via_button=False):
    """
    Submit a form and return the flash message shown after the redirect.
    via_button=False calls form.submit(), which skips the browser's native
    validation so the SERVER-side checks can be exercised.
    """
    form = browser.find_element(By.CSS_SELECTOR, form_css)
    if via_button:
        safe_click(browser, form.find_element(By.CSS_SELECTOR, "button[type='submit']"))
    else:
        browser.execute_script("arguments[0].submit();", form)
    WebDriverWait(browser, WAIT).until(EC.staleness_of(form))
    wait_loaded(browser)
    return WebDriverWait(browser, WAIT).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "flash-msg"))
    ).text


def test_add_course_form_fields_and_fee_boundaries(driver):
    """Add Course form: fields are present and required; Course Fee enforces min=0 / step=0.01."""
    open_upload_page(driver)
    form = driver.find_element(By.CSS_SELECTOR, "form[action$='/add_course']")

    for field_id in ("courseName", "courseCode", "registrationCategory", "courseFee"):
        field = form.find_element(By.ID, field_id)
        assert field.get_attribute("required"), f"#{field_id} should be required"

    fee = form.find_element(By.ID, "courseFee")

    def fee_is_valid(value):
        driver.execute_script("arguments[0].value = arguments[1];", fee, value)
        return driver.execute_script("return arguments[0].validity.valid;", fee)

    assert fee_is_valid("") is False        # required
    assert fee_is_valid("-1") is False      # below minimum
    assert fee_is_valid("0") is True        # lower boundary
    assert fee_is_valid("15000") is True    # typical value
    assert fee_is_valid("10.555") is False  # finer than step 0.01


def test_org_document_forms_post_to_correct_endpoints(driver):
    """Each Organisation Document form must POST to its own doc type and require a file."""
    open_upload_page(driver)
    forms = driver.find_elements(By.CSS_SELECTOR, "form.org-upload-form")
    assert len(forms) == len(ORG_DOC_TYPES)

    actions = [f.get_attribute("action") for f in forms]
    for doc_type in ORG_DOC_TYPES:
        assert any(a.endswith(f"/upload/org/{doc_type}") for a in actions), (
            f"No Organisation Document form posts to /upload/org/{doc_type}"
        )

    for form in forms:
        assert form.get_attribute("method").lower() == "post"
        assert form.find_element(
            By.CSS_SELECTOR, "input[type='file']"
        ).get_attribute("required")


def test_upload_action_switch_updates_form_state(driver):
    """Clicking each upload type must update active button, hidden action, accept, target endpoint, and clear the chosen file."""
    open_upload_page(driver)
    form = driver.find_element(By.ID, "unifiedUploadForm")
    file_input = driver.find_element(By.ID, "unifiedFileInput")
    buttons = driver.find_elements(
        By.CSS_SELECTOR, "#uploadActionSwitch .upload-action-btn"
    )
    temp_path = make_temp_file(".txt")

    try:
        for action in sorted(UPLOAD_ACTIONS):
            file_input.send_keys(temp_path)
            click_action(driver, action)

            for btn in buttons:
                is_active = "active" in btn.get_attribute("class").split()
                assert is_active == (btn.get_attribute("data-action") == action)

            selected = next(
                b for b in buttons if b.get_attribute("data-action") == action
            )
            assert driver.find_element(By.ID, "hiddenUploadAction").get_attribute("value") == action
            assert file_input.get_attribute("accept") == selected.get_attribute("data-accept")
            assert form.get_attribute("action").endswith(ACTION_ENDPOINTS[action])
            assert file_input.get_attribute("value") == "", (
                "Switching upload type should clear the previously chosen file."
            )
    finally:
        os.remove(temp_path)


def test_native_validation_blocks_incomplete_upload(driver):
    """Validation: the form is only valid once BOTH a category and a file are chosen."""
    open_upload_page(driver)
    category = driver.find_element(By.ID, "unifiedCategory")
    file_input = driver.find_element(By.ID, "unifiedFileInput")

    def is_valid(el):
        return driver.execute_script("return arguments[0].checkValidity();", el)

    assert not is_valid(category)
    assert not is_valid(file_input)

    select_first_category(driver)
    assert is_valid(category)
    assert not is_valid(file_input)

    temp_path = make_temp_file(".pptx")
    try:
        file_input.send_keys(temp_path)
        assert is_valid(driver.find_element(By.ID, "unifiedUploadForm"))
    finally:
        os.remove(temp_path)


def test_server_rejects_incomplete_upload_when_client_checks_bypassed(driver):
    """Server-side validation (no writes): missing file, then missing category."""
    open_upload_page(driver)

    flash = submit_and_get_flash(driver)
    assert "No file selected" in flash

    temp_path = make_temp_file(".pptx")
    try:
        driver.find_element(By.ID, "unifiedFileInput").send_keys(temp_path)
        flash = submit_and_get_flash(driver)
    finally:
        os.remove(temp_path)
    assert "select a technology/category" in flash


def test_unsupported_file_extension_is_rejected(driver):
    """Error handling (no writes): a .png sent as a PPT is rejected by the student service."""
    open_upload_page(driver)
    select_first_category(driver)

    temp_path = make_temp_file(".png")
    try:
        driver.find_element(By.ID, "unifiedFileInput").send_keys(temp_path)
        flash = submit_and_get_flash(driver, via_button=True)
    finally:
        os.remove(temp_path)

    assert "❌" in flash
    assert "Invalid file type" in flash


def test_employee_cannot_use_admin_only_upload_routes(driver):
    """
    Permission: an employee login opens the page but is not an admin user, so
    Practice Test uploads and Organisation Document uploads must be denied.
    Both are rejected before any file is read or stored.
    """
    open_upload_page(driver)

    click_action(driver, "practice")
    flash = submit_and_get_flash(driver)
    assert "Access denied" in flash

    flash = submit_and_get_flash(
        driver, form_css="form.org-upload-form[action$='/upload/org/hr_policy']"
    )
    assert "Access denied" in flash



@pytest.mark.skipif(
    not WRITE_TESTS,
    reason="Writes real files. Set RUN_WRITE_TESTS=true (test environment only).",
)
def test_valid_upload_succeeds_for_ppt_interview_and_code(driver):
    """
    Positive end-to-end (WRITES real files to storage; there is no delete for
    employees, so run against a test environment only).
    """
    open_upload_page(driver)

    for action, suffix in (("ppt", ".pptx"), ("interview", ".pdf"), ("code", ".py")):
        select_first_category(driver)
        click_action(driver, action)

        temp_path = make_temp_file(suffix)
        try:
            driver.find_element(By.ID, "unifiedFileInput").send_keys(temp_path)
            flash = submit_and_get_flash(driver, via_button=True)
        finally:
            os.remove(temp_path)

        assert "✅" in flash, f"{action} upload did not succeed: {flash}"
