import os
from uuid import uuid4

import pytest
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_URL = os.getenv(
    "CHAKORAHUB_BASE_URL",
    "https://www.chakorahub.com",
).rstrip("/")

REGISTRATION_URL = f"{BASE_URL}/registration"
WAIT = 30


@pytest.fixture
def driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1440,1100")

    browser = webdriver.Chrome(options=options)
    browser.set_page_load_timeout(60)
    yield browser
    browser.quit()


def wait_loaded(driver):
    WebDriverWait(driver, WAIT).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def wait_for_element(driver, by, value):
    return WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located((by, value))
    )


def write_tests_enabled():
    return os.getenv("RUN_WRITE_TESTS", "false").lower() == "true"


def select_first_real_option(select_element):
    for index, option in enumerate(Select(select_element).options):
        if (option.get_attribute("value") or "").strip():
            Select(select_element).select_by_index(index)
            return option
    return None


def select_free_course(driver):
    course_select = wait_for_element(driver, By.ID, "courseSelect")

    for index, option in enumerate(Select(course_select).options):
        value = (option.get_attribute("value") or "").strip()
        fee = (option.get_attribute("data-fee") or "0").strip()

        if not value:
            continue

        try:
            fee_value = float(fee)
        except ValueError:
            continue

        if fee_value <= 0:
            Select(course_select).select_by_index(index)
            return option

    return None


def fill_common_fields(driver, unique):
    driver.find_element(By.ID, "firstName").send_keys("Selenium")
    driver.find_element(By.ID, "lastName").send_keys(f"Student{unique}")
    driver.find_element(
        By.ID, "email"
    ).send_keys(f"selenium.student.{unique}@gmail.com")
    driver.find_element(By.ID, "phone").send_keys("9000000001")
    driver.find_element(By.ID, "location").send_keys("Hyderabad")


# ============================================================
# SMOKE TESTS
# ============================================================

def test_registration_page_loads(driver):
    driver.get(REGISTRATION_URL)
    wait_loaded(driver)

    # Common fields are visible when the page first loads.
    # Course and Language are hidden until a registration type is selected.
    for element_id in (
        "adminRegisterForm",
        "registrationType",
        "firstName",
        "lastName",
        "email",
        "phone",
        "location",
        "submitBtn",
    ):
        assert driver.find_element(By.ID, element_id).is_displayed()

    # Select Live Course and verify the category-dependent fields become visible.
    registration_type = driver.find_element(By.ID, "registrationType")
    Select(registration_type).select_by_value("student_co")

    WebDriverWait(driver, WAIT).until(
        lambda d: "active" in d.find_element(
            By.ID, "courseCard"
        ).get_attribute("class")
    )

    assert driver.find_element(By.ID, "courseSelect").is_displayed()
    assert driver.find_element(By.ID, "languageSelect").is_displayed()


def test_registration_form_data(driver):
    """
    /registration internally loads student-service form-data.
    Verify the browser receives and renders the resulting data.
    """
    driver.get(REGISTRATION_URL)
    wait_loaded(driver)

    Select(
        wait_for_element(driver, By.ID, "registrationType")
    ).select_by_value("student_co")

    WebDriverWait(driver, WAIT).until(
        lambda d: len(
            d.find_element(By.ID, "courseSelect").find_elements(
                By.TAG_NAME, "option"
            )
        ) > 1
    )

    course_options = Select(
        driver.find_element(By.ID, "courseSelect")
    ).options

    language_options = Select(
        driver.find_element(By.ID, "languageSelect")
    ).options

    assert len(course_options) > 1
    assert len(language_options) > 1

    real_courses = [
        option for option in course_options
        if (option.get_attribute("value") or "").strip()
    ]

    assert real_courses

    for option in real_courses:
        assert option.get_attribute("data-offering-id")
        assert option.get_attribute("data-fee") is not None


def test_registration_category_switching(driver):
    driver.get(REGISTRATION_URL)
    wait_loaded(driver)

    registration_type = wait_for_element(
        driver, By.ID, "registrationType"
    )

    course_card = driver.find_element(By.ID, "courseCard")
    placement_card = driver.find_element(By.ID, "placementCard")

    Select(registration_type).select_by_value("student_co")

    WebDriverWait(driver, WAIT).until(
        lambda d: "active" in course_card.get_attribute("class")
    )

    Select(registration_type).select_by_value("student_pl")

    WebDriverWait(driver, WAIT).until(
        lambda d: "active" in placement_card.get_attribute("class")
    )

    for element_id in (
        "qualification",
        "college",
        "branch",
        "passingYear",
        "experience",
        "resume",
    ):
        assert driver.find_element(By.ID, element_id).is_displayed()

    Select(registration_type).select_by_value("student_ws")

    WebDriverWait(driver, WAIT).until(
        lambda d: "active" in course_card.get_attribute("class")
    )


def test_registration_client_validation(driver):
    driver.get(REGISTRATION_URL)
    wait_loaded(driver)

    Select(
        wait_for_element(driver, By.ID, "registrationType")
    ).select_by_value("student_co")

    WebDriverWait(driver, WAIT).until(
        lambda d: len(
            d.find_element(By.ID, "courseSelect").find_elements(
                By.TAG_NAME, "option"
            )
        ) > 1
    )

    driver.find_element(By.ID, "firstName").send_keys("Selenium")
    driver.find_element(By.ID, "lastName").send_keys("Validation")
    driver.find_element(By.ID, "email").send_keys("invalid-email")
    driver.find_element(By.ID, "phone").send_keys("123")
    driver.find_element(By.ID, "location").send_keys("Hyderabad")

    location = driver.find_element(By.ID, "location")

    driver.execute_script(
        "arguments[0].focus(); arguments[0].blur();",
        location,
    )

    assert (
        driver.find_element(By.ID, "emailError").text
        or driver.find_element(By.ID, "phoneError").text
    )

    assert not driver.find_element(By.ID, "submitBtn").is_enabled()


# ============================================================
# MAINTENANCE MODE
# ============================================================

def test_student_registration_maintenance_notice(driver):
    """Verify the Student Registration maintenance notice when deployment expects it."""
    expected = os.getenv("EXPECT_MAINTENANCE_NOTICE", "false").lower() == "true"
    if not expected:
        pytest.skip("Maintenance notice is not expected for this run.")

    driver.get(REGISTRATION_URL)
    wait_loaded(driver)

    notice = WebDriverWait(driver, WAIT).until(
        EC.visibility_of_element_located(
            (By.ID, "student-registration-maintenance-notice")
        )
    )

    assert "Scheduled maintenance notice" in notice.text


# ============================================================
# PRODUCTION WRITE
# ============================================================

@pytest.mark.production_write
def test_student_course_registration(driver):
    """
    Real write:
        Selenium -> /registration
                 -> app.py
                 -> student_service /api/student/registration

    A FREE course is required so Razorpay is not opened.
    """
    if not write_tests_enabled():
        pytest.skip(
            "Set RUN_WRITE_TESTS=true to intentionally create "
            "a Student registration."
        )

    driver.get(REGISTRATION_URL)
    wait_loaded(driver)

    unique = uuid4().hex[:8]
    fill_common_fields(driver, unique)

    Select(
        wait_for_element(driver, By.ID, "registrationType")
    ).select_by_value("student_co")

    WebDriverWait(driver, WAIT).until(
        lambda d: len(
            d.find_element(By.ID, "courseSelect").find_elements(
                By.TAG_NAME, "option"
            )
        ) > 1
    )

    course = select_free_course(driver)

    if course is None:
        pytest.skip(
            "No FREE Live Course offering is available. "
            "Skipping to avoid starting a real Razorpay payment."
        )

    language = wait_for_element(driver, By.ID, "languageSelect")

    if select_first_real_option(language) is None:
        pytest.fail("No language option is available.")

    # Trigger the same change handlers used by registration.html.
    driver.execute_script(
        "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
        course,
    )
    driver.execute_script(
        "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
        language,
    )

    submit_button = WebDriverWait(driver, WAIT).until(
        EC.element_to_be_clickable((By.ID, "submitBtn"))
    )
    submit_button.click()

    proceed_button = WebDriverWait(driver, WAIT).until(
        EC.element_to_be_clickable((By.ID, "offcanvasProceedBtn"))
    )

    assert "Confirm" in proceed_button.text
    proceed_button.click()

    WebDriverWait(driver, WAIT).until(
        lambda d: (
            "Registration Successful" in d.find_element(By.TAG_NAME, "body").text
            or "Registration ID" in d.find_element(By.TAG_NAME, "body").text
            or "Registration failed" in d.find_element(By.TAG_NAME, "body").text
            or "Error:" in d.find_element(By.TAG_NAME, "body").text
        )
    )

    body_text = driver.find_element(By.TAG_NAME, "body").text

    assert "Registration Successful" in body_text, (
        "Student registration failed.\n"
        f"Page text:\n{body_text}"
    )

    assert "Registration ID" in body_text
    assert f"selenium.student.{unique}@gmail.com" in body_text


@pytest.mark.production_write
def test_student_placement_resume_upload(driver, tmp_path):
    """
    Real write to the configured S3 resume location through:
        /registration/upload-resume
    """
    if not write_tests_enabled():
        pytest.skip(
            "Set RUN_WRITE_TESTS=true to test the real "
            "Placement resume upload."
        )

    driver.get(REGISTRATION_URL)
    wait_loaded(driver)

    Select(
        wait_for_element(driver, By.ID, "registrationType")
    ).select_by_value("student_pl")

    WebDriverWait(driver, WAIT).until(
        lambda d: "active" in d.find_element(
            By.ID, "placementCard"
        ).get_attribute("class")
    )

    pdf_path = tmp_path / "selenium_test_resume.pdf"
    pdf_path.write_bytes(
        b"%PDF-1.4\n"
        b"1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj\n"
        b"2 0 obj << /Type /Pages /Kids [] /Count 0 >> endobj\n"
        b"trailer << /Root 1 0 R >>\n"
        b"%%EOF\n"
    )

    resume = driver.find_element(By.ID, "resume")
    resume.send_keys(str(pdf_path))

    result = driver.execute_async_script(
        """
        const input = arguments[0];
        const done = arguments[arguments.length - 1];

        const formData = new FormData();
        formData.append("resume", input.files[0]);
        formData.append("registration_type", "student_pl");

        fetch("/registration/upload-resume", {
            method: "POST",
            body: formData
        })
        .then(async response => {
            let data = {};
            try { data = await response.json(); } catch (e) {}

            done({
                ok: response.ok,
                status: response.status,
                data: data
            });
        })
        .catch(error => done({
            ok: false,
            status: 0,
            data: {message: error.toString()}
        }));
        """,
        resume,
    )

    assert result["ok"], result
    assert result["data"].get("success") is True, result
    assert result["data"].get("resume_file_name"), result
    assert result["data"].get("resume_s3_key"), result
    assert result["data"]["resume_file_name"] == "selenium_test_resume.pdf"


@pytest.mark.production_write
def test_registration_rejects_invalid_email(driver):
    """
    Route-level negative test. The backend must reject an email
    outside @gmail.com / @chakorahub.com.
    """
    if not write_tests_enabled():
        pytest.skip(
            "Set RUN_WRITE_TESTS=true to run registration validation."
        )

    driver.get(REGISTRATION_URL)
    wait_loaded(driver)

    result = driver.execute_async_script(
        """
        const done = arguments[arguments.length - 1];

        fetch("/registration", {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify({
                registration_type: "student_co",
                first_name: "Selenium",
                last_name: "InvalidEmail",
                email: "selenium.invalid@example.com",
                phone: "9000000002",
                location: "Hyderabad",
                course: "invalid-course",
                offering_id: "invalid-offering",
                language: "invalid-language",
                payment_id: "FREE",
                order_id: "FREE",
                signature: "FREE"
            })
        })
        .then(async response => {
            let data = {};
            try { data = await response.json(); } catch (e) {}

            done({
                status: response.status,
                data: data
            });
        })
        .catch(error => done({
            status: 0,
            data: {message: error.toString()}
        }));
        """
    )

    assert result["status"] in (400, 422), result
