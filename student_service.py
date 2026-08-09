"""
student_service.py  ─  Student FastAPI Microservice
Port : 8001
Run  : uvicorn student_service:app --host 0.0.0.0 --port 8001

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Architecture : [User] → [Flask Proxy app.py] → [This Service] → [Oracle]
"""
import uvicorn
import traceback
import oracledb
import os, json, uuid, httpx
import boto3
import mimetypes
import shutil
import requests
import uuid
from kafka import KafkaConsumer, KafkaProducer
from threading import Thread
from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi import responses
from fastapi.responses import JSONResponse, HTMLResponse  # <-- added HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, EmailStr
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from decimal import Decimal
from boto3.dynamodb.conditions import Attr
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename
from dotenv import load_dotenv
load_dotenv()
from fastapi import Query
from fastapi.responses import JSONResponse

# ================= SERVICE URLS =================

HOME_SERVICE_URL = os.getenv("HOME_SERVICE_URL","http://localhost:5001")
MEETING_SERVICE_URL = os.getenv("MEETING_SERVICE_URL","http://localhost:9000")
CHATBOT_SERVICE_URL = os.getenv("CHATBOT_SERVICE_URL","http://localhost:7600")
ASSET_SERVICE_URL = os.getenv("ASSET_SERVICE_URL","http://localhost:8090")
INTERNSHIP_SERVICE_URL = os.getenv("INTERNSHIP_SERVICE_URL","http://localhost:5050")
MS365_SERVICE_URL = os.getenv("MS365_SERVICE_URL","http://localhost:7700")
EMPLOYEE_SERVICE_URL = os.getenv("EMPLOYEE_SERVICE_URL","http://localhost:8002")
BLOGGER_SERVICE_URL = os.getenv("BLOGGER_SERVICE_URL","http://localhost:7500")
BRS_SERVICE_URL = os.getenv("BRS_SERVICE_URL","http://localhost:8020")
LAMBDA_URL = 'https://lwug4xhfz27whiuu3acjfwsgtm0ttwja.lambda-url.eu-north-1.on.aws/'
STATIC_CDN = "https://d1pjjckqswt5z7.cloudfront.net"

CANONICAL_HOST = os.getenv("CANONICAL_HOST","www.chakorahub.com").strip().lower()
INTERNSHIP_PUBLIC_HOST = os.getenv("INTERNSHIP_PUBLIC_HOST","api.chakorahub.com").strip().lower()
FEEDBACK_BASE_URL = os.getenv("FEEDBACK_BASE_URL", "http://127.0.0.1:8282/feedback").rstrip("/")

#_get_runtime_env_value


app = FastAPI(title="Student Service", version="3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.chakorahub.com", "https://chakorahub.com",
        "http://www.chakorahub.com",  "http://chakorahub.com",
    ],
    allow_origin_regex=r"https?://(localhost|127\.0\.0\.1)(:\d+)?",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
REGISTRATION_STREAM_NAME = os.getenv("REGISTRATION_STREAM_NAME", "registration_stream")

# TTL constants
TTL_SESSION      = 604_800   # 7 days
TTL_PROFILE      = 1_800     # 30 min
TTL_AUTH         = 3_600     # 1 hr
TTL_DASHBOARD    = 300       # 5 min
TTL_RESOURCES    = 7_200     # 2 hr
TTL_OFFERS       = 3_600     # 1 hr
TTL_FESTIVALS    = 86_400    # 24 hr
TTL_FEEDBACKS    = 600       # 10 min
TTL_REGISTRATION = 300       # 5 min

ORACLE_HOST = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
ORACLE_USER = os.getenv("ORACLE_USER", "SUPPORT")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "Welcome123")

# Compatibility marker used by existing conn.cursor(DictCursor) call sites.
DictCursor = object()

# ── Kafka Config ─────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Producer — publishes student.lookup.completed
kafka_producer = None
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3
    )
    print("✅ student_service Kafka producer ready")
except Exception as e:
    print(f"⚠️  Kafka producer unavailable @ {KAFKA_BOOTSTRAP_SERVERS}: {e}")
    kafka_producer = None


def kafka_publish(topic: str, payload: dict) -> None:
    if kafka_producer is None:
        print(f"⚠️  Kafka publish skipped [{topic}] because producer is unavailable")
        return
    try:
        print(f"📤 Kafka publish request → {topic} | keys={list(payload.keys())}")
        kafka_producer.send(topic, value=payload)
        kafka_producer.flush(timeout=2)
        print(f"📤 Kafka → {topic}: {payload}")
    except Exception as e:
        print(f"⚠️  Kafka publish failed [{topic}]: {e}")

# ─────────────────────────────────────────────
# AWS SES  (email — same pattern as meeting_service.py)
# ─────────────────────────────────────────────
AWS_REGION  = os.getenv("AWS_REGION", "eu-north-1")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@chakorahub.com")
CHARSET     = "UTF-8"
ses         = boto3.client("ses", region_name=AWS_REGION)

# Rate limit rules  {endpoint: {limit, window}}
RATE_LIMITS: Dict[str, Dict] = {
    "/api/student/login":    {"limit": 10, "window": 60},
    "/api/student/feedback": {"limit": 5,  "window": 60},
    "/api/student/enquiry":  {"limit": 5,  "window": 60},
}

# Course name normalisation map  (URL slug → DB name)
COURSE_NAME_MAP = {
    "informatica":                "Informatica",
    "unix":                       "Unix",
    "oracle":                     "Oracle(SQL & PLSQL)",
    "iics":                       "IICS",
    "python for web development": "Python for Web Development",
    "informatica mdm":            "MDM",
    "informatica bdm":            "BDM",
    "python for automation":      "Python for Automation",
    "snowflake":                  "Snowflake",
}

COURSE_CARD_IMAGE_COLUMN_CANDIDATES = [
    "IMAGE_S3_URL",
    "COURSE_IMAGE_URL",
    "IMAGE_URL",
    "THUMBNAIL_URL",
    "COURSE_THUMBNAIL_URL",
    "S3_URL",
    "COURSE_URL",
    "URL",
]

SERVICE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_ROOT = os.path.join(SERVICE_DIR, "uploads")
PRACTICE_TESTS_FOLDER = os.path.join(UPLOAD_ROOT, "practice_tests")
SYLLABUS_FOLDER = os.path.join(UPLOAD_ROOT, "syllabus")

os.makedirs(UPLOAD_ROOT, exist_ok=True)
os.makedirs(PRACTICE_TESTS_FOLDER, exist_ok=True)
os.makedirs(SYLLABUS_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {
    "syllabus": {"pdf", "docx"},
    "practice_test": {"pdf", "txt", "docx", "xlsx"},
    "ppt": {"ppt", "pptx"},
    "interview": {"pdf", "txt", "docx"},
    "code": {
        "py", "java", "cpp", "c", "txt", "xml", "json", "properties", "sh",
        "sql", "conf", "map", "xiwz", "ksh", "bash", "csv", "yml", "html",
        "js", "css", "avro", "parquet"
    },
}

# ─────────────────────────────────────────────
# CACHE BACKEND (local no-op compatible stub)
# ─────────────────────────────────────────────
_LOCAL_CACHE: Dict[str, Any] = {}


def _cache_set(key: str, value: Any) -> None:
    _LOCAL_CACHE[key] = value


def _cache_get(key: str) -> Optional[Any]:
    return _LOCAL_CACHE.get(key)


def _cache_delete(key: str) -> None:
    _LOCAL_CACHE.pop(key, None)


def _json_default(value):
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _resolve_course_card_image_column(cursor) -> Optional[str]:
    """Resolve the optional course image URL column added to NRM_COURSES."""
    try:
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = 'NRM_COURSES'
            """
        )
        user_cols = {(row.get("COLUMN_NAME") or "").upper() for row in (cursor.fetchall() or [])}
    except Exception:
        user_cols = set()

    if not user_cols:
        try:
            cursor.execute(
                """
                SELECT COLUMN_NAME
                FROM ALL_TAB_COLUMNS
                WHERE OWNER = 'CHAKORA' AND TABLE_NAME = 'NRM_COURSES'
                """
            )
            user_cols = {(row.get("COLUMN_NAME") or "").upper() for row in (cursor.fetchall() or [])}
        except Exception:
            user_cols = set()

    for col in COURSE_CARD_IMAGE_COLUMN_CANDIDATES:
        if col in user_cols:
            return col
    return None


# Kafka Consumer helper function

def _run_lookup_consumer():
    """
    Consumes student.lookup.requested events.
    Mirrors the logic already in /student-lookup endpoint.
    Publishes result to student.lookup.completed.
    """
    try:
        consumer = KafkaConsumer(
            "student.lookup.requested",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            group_id="student-service-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        print("✅ Kafka consumer listening on student.lookup.requested")
    except Exception as e:
        print(f"⚠️  Kafka consumer unavailable @ {KAFKA_BOOTSTRAP_SERVERS}: {e}")
        return

    for msg in consumer:
        print(f"📥 Kafka consume ← {msg.topic} | partition={msg.partition} offset={msg.offset}")
        event = msg.value
        correlation_id  = event.get("correlation_id", str(uuid.uuid4()))
        identity        = event.get("identity", "")
        identity_type   = event.get("identity_type", "")

        print(f"📥 Kafka ← student.lookup.requested | id={correlation_id} | {identity}")

        result_payload = {
            "success":        True,
            "correlation_id": correlation_id,
            "identity":       identity,
            "identity_type":  identity_type,
            "exists":         False,
            "source":         None,
            "student_email":  None,
            "student_phone":  None,
            "student_name":   None,
            "total_bookings": 0,
            "ml_suggestion":  None,
            "timestamp":      datetime.utcnow().isoformat()
        }

        try:
            normalized = identity.lower().strip()

            # Cache is disabled; perform direct lookup.
            dynamodb = boto3.resource(
                "dynamodb",
                region_name=os.getenv("AWS_REGION", "eu-north-1"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY"),
            )
            table    = dynamodb.Table(MEETING_BOOKINGS_TABLE)
            filter_attr = (
                Attr("student_email").eq(normalized)
                if identity_type == "email"
                else Attr("student_phone").eq(normalized)
            )
            response = table.scan(FilterExpression=filter_attr)
            bookings = sorted(
                response.get("Items", []),
                key=lambda x: x.get("booking_date", ""),
                reverse=True
            )

            if bookings:
                latest = bookings[0] if bookings else {}
                result_payload.update({
                    "exists":        True,
                    "source":        "dynamodb",
                    "student_email": latest.get("student_email"),
                    "student_phone": latest.get("student_phone"),
                    "student_name":  latest.get("student_name"),
                    "total_bookings": len(bookings),
                    "ml_suggestion": generate_ml_suggestion(bookings)
                })
            # if not found, result_payload["exists"] stays False

        except Exception as lookup_err:
            print(f"❌ consumer lookup error: {lookup_err}")
            result_payload["error"] = str(lookup_err)

        # ── STEP 3: Publish result ──────────────────────────────
        kafka_publish("student.lookup.completed", result_payload)

# ─────────────────────────────────────────────
# CACHE HELPERS
# ─────────────────────────────────────────────

# ── 1. Session  (key: session:{user_id}) ──────────────────
def cache_session_set(user_id: int, data: dict) -> None:
    _cache_set(f"session:{user_id}", data)

def cache_session_get(user_id: int) -> Optional[dict]:
    value = _cache_get(f"session:{user_id}")
    return value if isinstance(value, dict) else None

def cache_session_delete(user_id: int) -> None:
    _cache_delete(f"session:{user_id}")

def cache_session_refresh(user_id: int) -> None:
    """No-op for local cache backend."""
    _ = user_id


# ── 2. User Profile  (key: user:{user_id}) ────────────────
def cache_profile_set(user_id: int, data: dict) -> None:
    _cache_set(f"user:{user_id}", data)

def cache_profile_get(user_id: int) -> Optional[dict]:
    value = _cache_get(f"user:{user_id}")
    return value if isinstance(value, dict) else None

def cache_profile_delete(user_id: int) -> None:
    _cache_delete(f"user:{user_id}")


# ── 3. Auth / Roles  (key: roles:{user_id}) ──────────────
def cache_auth_set(user_id: int, roles: list, usertype: str) -> None:
    _cache_set(f"roles:{user_id}", {"roles": roles, "usertype": usertype})

def cache_auth_get(user_id: int) -> Optional[dict]:
    value = _cache_get(f"roles:{user_id}")
    return value if isinstance(value, dict) else None

def cache_auth_delete(user_id: int) -> None:
    _cache_delete(f"roles:{user_id}")


# ── 4. Frequent Data  (caller-defined keys, DB 3) ────────
def cache_freq_set(key: str, data: Any, ttl: int) -> None:
    _ = ttl
    _cache_set(key, data)

def cache_freq_get(key: str) -> Optional[Any]:
    return _cache_get(key)

def cache_freq_delete(key: str) -> None:
    _cache_delete(key)


# ── 5. Rate Limit check ──────────────────────────────────
def rate_limit_ok(identifier: str, endpoint: str) -> bool:
    """Returns True = allowed (rate limiting disabled)."""
    _ = (identifier, endpoint)
    return True


# ── 6. Dashboard/API Response Cache  (key: student:dashboard:{user_id}) ─
def cache_dashboard_set(user_id: int, data: Any) -> None:
    _cache_set(f"student:dashboard:{user_id}", data)

def cache_dashboard_get(user_id: int) -> Optional[Any]:
    return _cache_get(f"student:dashboard:{user_id}")

def cache_dashboard_delete(user_id: int) -> None:
    _cache_delete(f"student:dashboard:{user_id}")


# ─────────────────────────────────────────────
# RATE LIMIT MIDDLEWARE
# ─────────────────────────────────────────────
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    if request.url.path in RATE_LIMITS:
        ip = request.client.host if request.client else "unknown"
        if not rate_limit_ok(ip, request.url.path):
            return JSONResponse(
                status_code=429,
                content={"success": False, "message": "Too many requests. Please try again later."},
            )
    return await call_next(request)


# ─────────────────────────────────────────────
# ORACLE CONNECTION
# ─────────────────────────────────────────────
class _OracleCursorCompat:
    def __init__(self, cursor, dict_mode=False):
        self._cursor = cursor
        self._dict_mode = dict_mode

    @staticmethod
    def _rewrite_positional_binds(sql: str, params):
        if params is None or isinstance(params, dict) or "%s" not in sql:
            return sql
        rewritten = sql
        try:
            param_count = len(params)
        except Exception:
            param_count = 0
        for idx in range(1, param_count + 1):
            rewritten = rewritten.replace("%s", f":{idx}", 1)
        return rewritten

    def execute(self, sql, params=None):
        rewritten_sql = self._rewrite_positional_binds(sql, params)
        if params is None:
            self._cursor.execute(rewritten_sql)
        else:
            self._cursor.execute(rewritten_sql, params)

        if self._dict_mode and self._cursor.description:
            columns = [d[0] for d in self._cursor.description]
            self._cursor.rowfactory = lambda *vals, cols=columns: dict(zip(cols, vals))
        return self

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _OracleConnectionCompat:
    def __init__(self, conn):
        self._conn = conn

    def cursor(self, *args, **kwargs):
        dict_mode = bool(args)
        return _OracleCursorCompat(self._conn.cursor(), dict_mode=dict_mode)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_db_connection():
    try:
        dsn = oracledb.makedsn(
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME,
        )
        conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn,
        )

        cur = conn.cursor()
        cur.execute("ALTER SESSION SET CURRENT_SCHEMA = CHAKORA")
        cur.close()

        return _OracleConnectionCompat(conn)
    except Exception as e:
        print(f"❌ Oracle connection error: {e}")
        return None


# ─────────────────────────────────────────────
# PYDANTIC MODELS
# ─────────────────────────────────────────────
class LoginRequest(BaseModel):
    username: str          # email or phone
    password: str
    login_type: str = "user"

class LoginResponse(BaseModel):
    success: bool
    message: str
    user_id: Optional[int] = None
    username: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    usertype: Optional[str] = None
    profile_pic: Optional[str] = None
    is_admin: Optional[bool] = False
    # Session token is the user_id itself (key: session:{user_id})
    # Flask proxy reads it from here and stores in its own session cookie.

class DashboardRequest(BaseModel):
    user_id: int

class ProfileRequest(BaseModel):
    user_id: int

class UpdateProfileRequest(BaseModel):
    user_id: int
    address: Optional[str] = None
    phone: Optional[str] = None
    profile_pic: Optional[str] = None

class FeedbackRequest(BaseModel):
    name: str
    email: EmailStr
    phone: str
    feedback_text: str
    rating: Optional[int] = None   # 1–5 stars
    meeting_id: Optional[str] = None  # NEW: optional meeting ID
    request_id: Optional[str] = None

class EnquiryRequest(BaseModel):
    name: str
    email: EmailStr
    phone: str
    enquiry_text: str
    user_id: Optional[int] = None

class LogoutRequest(BaseModel):
    user_id: int

class StudentRegistrationRequest(BaseModel):
    registration_type: str
    first_name: str
    last_name: str
    email: str
    phone: str
    location: str
    course: int
    offering_id: int
    language: int
    start_date: Optional[str] = None
    payment_id: Optional[str] = None
    order_id: Optional[str] = None
    signature: Optional[str] = None
    gateway_payment_id: Optional[str] = None
    gateway_order_id: Optional[str] = None
    gateway_signature: Optional[str] = None
    razorpay_payment_id: Optional[str] = None
    razorpay_order_id: Optional[str] = None
    razorpay_signature: Optional[str] = None
    qualification: Optional[str] = None
    college: Optional[str] = None
    branch: Optional[str] = None
    passing_year: Optional[int] = None
    experience: Optional[str] = None
    resume_file_name: Optional[str] = None
    resume_s3_key: Optional[str] = None

# ── NEW models for feedback generation ──────────────────
class FeedbackActivity(BaseModel):
    activity_type: str
    activity_id: str
    label: str

class FeedbackGenerateRequest(BaseModel):
    student_email: EmailStr
    student_name: str
    activities: List[FeedbackActivity]
    expiry_hours: float = 24.0

class FeedbackFormRequest(BaseModel):
    request_id: Optional[str] = None
    meeting_id: Optional[str] = None
    name: str
    email: EmailStr
    phone: str
    feedback_text: str
    rating: Optional[int] = None

# ── NEW: Jinja2 templates setup ─────────────────────────
# We'll use an in-memory HTML template string for simplicity,
# but we can also use Jinja2Templates if we have a templates folder.
# For this implementation, we'll render HTML directly.
# We'll keep a simple HTML form as a string.

FEEDBACK_FORM_HTML = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>ChakoraHub - Feedback</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 600px; margin: 40px auto; padding: 20px; background: #f9f9f9; }
        .card { background: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .header { border-bottom: 2px solid #00897b; padding-bottom: 15px; margin-bottom: 20px; }
        h2 { color: #00897b; margin: 0; }
        .subtitle { color: #666; font-size: 14px; }
        .form-group { margin-bottom: 20px; }
        label { display: block; font-weight: 600; margin-bottom: 6px; }
        .star-rating { display: flex; flex-direction: row-reverse; justify-content: flex-end; gap: 8px; }
        .star-rating input { display: none; }
        .star-rating label { font-size: 28px; color: #ccc; cursor: pointer; }
        .star-rating label:hover,
        .star-rating label:hover ~ label,
        .star-rating input:checked ~ label { color: #f5b342; }
        textarea, input[type="tel"] { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; }
        .btn { background: #00897b; color: #fff; border: none; padding: 12px 24px; border-radius: 4px; font-size: 16px; cursor: pointer; width: 100%; }
        .btn:disabled { background: #a0d4c4; }
        .error { color: #d32f2f; background: #ffebee; padding: 10px; border-radius: 4px; margin-bottom: 15px; display: none; }
        .success { text-align: center; padding: 20px; }
        .success .icon { font-size: 48px; color: #4caf50; }
    </style>
</head>
<body>
    <div class="card" id="form-container">
        <div class="header">
            <h2>Share Your Feedback</h2>
            <p class="subtitle">Meeting ID: <strong>{{ meeting_id }}</strong></p>
        </div>
        <p>Hello <strong>{{ student_name }}</strong>, we appreciate your feedback!</p>
        <form id="feedback-form">
            <input type="hidden" name="meeting_id" value="{{ meeting_id }}">
            <div class="form-group">
                <label>Rating *</label>
                <div class="star-rating">
                    <input type="radio" id="star5" name="rating" value="5" /><label for="star5" title="Excellent">★</label>
                    <input type="radio" id="star4" name="rating" value="4" /><label for="star4" title="Good">★</label>
                    <input type="radio" id="star3" name="rating" value="3" /><label for="star3" title="Average">★</label>
                    <input type="radio" id="star2" name="rating" value="2" /><label for="star2" title="Poor">★</label>
                    <input type="radio" id="star1" name="rating" value="1" /><label for="star1" title="Very Poor">★</label>
                </div>
            </div>
            <div class="form-group">
                <label for="comments">Comments *</label>
                <textarea id="comments" name="feedback_text" rows="4" required maxlength="1000"></textarea>
            </div>
            <div class="form-group">
                <label for="phone">Phone (optional)</label>
                <input type="tel" id="phone" name="phone" value="{{ student_phone|default('') }}">
            </div>
            <div id="error" class="error"></div>
            <button type="submit" class="btn">Submit Feedback</button>
        </form>
    </div>
    <div class="card success" id="success-container" style="display:none;">
        <div class="icon">✓</div>
        <h2>Thank You!</h2>
        <p>Your feedback has been submitted successfully.</p>
    </div>
    <script>
        document.getElementById('feedback-form').addEventListener('submit', async function(e) {
            e.preventDefault();
            const form = this;
            const rating = document.querySelector('input[name="rating"]:checked');
            if (!rating) {
                showError('Please select a rating.');
                return;
            }
            const data = {
                meeting_id: form.meeting_id.value,
                rating: parseInt(rating.value),
                feedback_text: form.feedback_text.value,
                phone: form.phone.value || null,
                name: '{{ student_name }}',
                email: '{{ student_email }}'
            };
            const btn = form.querySelector('.btn');
            btn.disabled = true;
            btn.textContent = 'Submitting...';
            hideError();
            try {
                const resp = await fetch('/api/student/feedback', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(data)
                });
                const result = await resp.json();
                if (resp.ok) {
                    document.getElementById('form-container').style.display = 'none';
                    document.getElementById('success-container').style.display = 'block';
                } else {
                    showError(result.detail || 'Submission failed. Please try again.');
                    btn.disabled = false;
                    btn.textContent = 'Submit Feedback';
                }
            } catch (err) {
                showError('Network error. Please check your connection.');
                btn.disabled = false;
                btn.textContent = 'Submit Feedback';
            }
        });
        function showError(msg) {
            const el = document.getElementById('error');
            el.textContent = msg;
            el.style.display = 'block';
        }
        function hideError() {
            document.getElementById('error').style.display = 'none';
        }
    </script>
</body>
</html>
"""

def _upload_student_content_to_s3(local_path: str, file_type: str, category: str) -> Optional[str]:
    """Best-effort mirror of student uploads into S3."""
    try:
        bucket = (
            os.getenv("STUDENT_CONTENT_S3_BUCKET")
            or os.getenv("STUDENT_S3_BUCKET")
            or "chakorahub-student-s3"
        ).strip()
        if not bucket:
            return None

        prefix_map = {
            "ppt": "PPTs",
            "interview": "Interview_Questions",
            "code": "Code",
            "practice_test": "Practice_Tests",
            "syllabus": "Syllabus",
            "resume": "Resumes",
        }
        prefix = prefix_map.get(file_type, file_type)

        category_segment = str(category or "General").strip().strip("/") or "General"
        object_name = os.path.basename(local_path)
        s3_key = f"{prefix}/{category_segment}/{object_name}"

        region = (os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "eu-north-1").strip()
        access_key = (
            os.getenv("AWS_ACCESS_KEY_ID")
            or os.getenv("AWS_ACCESS_KEY")
            or ""
        ).strip()
        secret_key = (
            os.getenv("AWS_SECRET_ACCESS_KEY")
            or os.getenv("AWS_SECRET_KEY")
            or ""
        ).strip()

        if access_key and secret_key:
            s3 = boto3.client(
                "s3",
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
        else:
            s3 = boto3.client("s3", region_name=region)

        content_type = mimetypes.guess_type(local_path)[0] or "application/octet-stream"
        s3.upload_file(local_path, bucket, s3_key, ExtraArgs={"ContentType": content_type})
        return s3_key
    except Exception as e:
        print(
            "⚠️ Student content S3 upload failed "
            f"(bucket={os.getenv('STUDENT_CONTENT_S3_BUCKET') or os.getenv('STUDENT_S3_BUCKET') or 'chakorahub-student-s3'}, "
            f"file_type={file_type}, category={category}, local_path={local_path}): {e}"
        )
        return None


def _save_upload_file(upload_file: UploadFile, destination_path: str) -> None:
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    with open(destination_path, "wb") as out_file:
        upload_file.file.seek(0)
        shutil.copyfileobj(upload_file.file, out_file)


def _build_student_registration_id(cursor, course_id: int, course_code: str, first_name: str, last_name: str, start_date: datetime) -> str:
    code_len = len(course_code)
    cursor.execute(
        """
        SELECT COALESCE(MAX(CAST(SUBSTR(REGISTRATION_ID, %s, 3) AS INT)), 0)
        FROM NRM_REGISTRATIONS
        WHERE COURSE_ID = %s
        """,
        (code_len + 3, course_id),
    )
    row = cursor.fetchone()
    print(row)
    seq = int(list(row.values())[0] if row else 0) + 1
    first_initial = (first_name or "X").strip()[:1].upper() or "X"
    last_initial = (last_name or "X").strip()[:1].upper() or "X"
    return f"{course_code}{first_initial}{last_initial}{str(seq).zfill(3)}{start_date.strftime('%d%m')}"

# ── NEW: send feedback link email ─────────────────────────────────
def send_feedback_link_email(to_email: str, student_name: str, meeting_id: str, feedback_url: str, expiry: datetime):
    """Send an email with the feedback link."""
    subject = "We value your feedback – ChakoraHub"
    body_html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background: #f9f9f9;">
        <div style="background: #fff; padding: 30px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1);">
            <h2 style="color: #00897b;">Dear {student_name},</h2>
            <p>Thank you for meeting with us! We would love to hear your feedback about the session.</p>
            <p>Please click the link below to share your experience:</p>
            <p><a href="{feedback_url}" style="color: #00897b; font-weight: bold;">{feedback_url}</a></p>
            <p>This link will expire on <strong>{expiry.strftime('%Y-%m-%d %H:%M UTC')}</strong>.</p>
            <p>Your feedback helps us improve.</p>
            <p>Best regards,<br>ChakoraHub Team</p>
        </div>
    </body>
    </html>
    """
    try:
        response = ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={
                "ToAddresses": [to_email]
            },
            Message={
                "Subject": {
                    "Data": subject,
                    "Charset": "UTF-8"
                },
                "Body": {
                    "Html": {
                        "Data": body_html,
                        "Charset": "UTF-8"
                    }
                }
            }
        )

        print("=" * 70)
        print("AWS SES SEND EMAIL SUCCESS")
        print(f"FROM : {ADMIN_EMAIL}")
        print(f"TO   : {to_email}")
        print("SES RESPONSE:")
        print(response)
        print("=" * 70)

        return response
    except Exception as e:
        print(f"⚠️ Failed to send feedback link email: {e}")
        raise


# ═════════════════════════════════════════════════════════
# HEALTH
# ═════════════════════════════════════════════════════════
@app.get("/health")
async def health_check():
    conn = get_db_connection()
    db_ok = conn is not None
    if conn:
        conn.close()
    return {
        "status":    "healthy",
        "service":   "student-service",
        "version":   "3.0",
        "database":  "connected" if db_ok else "disconnected",
        "cache":     "disabled",
        "timestamp": datetime.now().isoformat(),
    }


# ═════════════════════════════════════════════════════════
# REGISTRATION FORM DATA
# Used by Website-Git/templates/registration.html
# ═════════════════════════════════════════════════════════
@app.get("/api/student/registration/form-data")
async def registration_form_data():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT ID, COURSE_NAME, COURSE_CODE
            FROM NRM_COURSES
            ORDER BY COURSE_NAME
            """
        )
        course_rows = cursor.fetchall() or []
        courses = [
            {
                "ID": row["ID"],
                "COURSE_NAME": row["COURSE_NAME"],
                "COURSE_CODE": row["COURSE_CODE"],
            }
            for row in course_rows
        ]

        cursor.execute(
            """
            SELECT ID, LANGUAGE
            FROM NRM_LANGUAGES
            ORDER BY LANGUAGE
            """
        )
        language_rows = cursor.fetchall() or []
        languages = [
            {
                "ID": row["ID"],
                "LANGUAGE": row["LANGUAGE"],
            }
            for row in language_rows
        ]

        return {
            "success": True,
            "courses": courses,
            "languages": languages,
            "payment_key_id": os.getenv("RZP_KEY_ID", ""),
            "departments": [],
            "designations": [],
            "managers": [],
            "work_locations": [],
        }
    except Exception as e:
        print(f"❌ Registration form-data error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/api/student/admin/courses")
async def admin_courses():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT
                c.ID,
                c.COURSE_NAME,
                c.COURSE_CODE,
                CAST(ROUND(COALESCE(MAX(CASE WHEN o.REGISTRATION_CATEGORY = 'student_co' THEN o.COURSE_FEE END), 0)) AS INT) AS STUDENT_CO_FEE,
                CAST(ROUND(COALESCE(MAX(CASE WHEN o.REGISTRATION_CATEGORY = 'student_pl' THEN o.COURSE_FEE END), 0)) AS INT) AS STUDENT_PL_FEE,
                CAST(ROUND(COALESCE(MAX(CASE WHEN o.REGISTRATION_CATEGORY = 'student_ws' THEN o.COURSE_FEE END), 0)) AS INT) AS STUDENT_WS_FEE,
                CAST(ROUND(COALESCE(MAX(pl.PRICE), 0)) AS INT) AS SHOPCOURSE_FEE
            FROM NRM_COURSES c
            LEFT JOIN NRM_COURSE_OFFERINGS o
                ON c.ID = o.COURSE_ID AND (o.IS_ACTIVE = 'Y' OR o.IS_ACTIVE IS NULL)
            LEFT JOIN PRICING_LOOKUP pl
                ON c.ID = pl.COURSE_ID
            GROUP BY c.ID, c.COURSE_NAME, c.COURSE_CODE
            ORDER BY c.COURSE_NAME
            """
        )
        return cursor.fetchall() or []
    except Exception as e:
        print(f"❌ admin_courses error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/api/student/resources-courses")
async def resources_courses():
    """Return active course cards for Website resources grid.

    Uses the optional 4th URL/image column from NRM_COURSES when available.
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        image_col = _resolve_course_card_image_column(cursor)
        print(f"[resources_courses] resolved image column: {image_col or 'NONE'}")

        if image_col:
            query = f"""
                SELECT
                    c.ID,
                    c.COURSE_NAME,
                    c.COURSE_CODE,
                    c.{image_col} AS IMAGE_URL
                FROM NRM_COURSES c
                ORDER BY c.COURSE_NAME
            """
        else:
            query = """
                SELECT
                    c.ID,
                    c.COURSE_NAME,
                    c.COURSE_CODE,
                    '' AS IMAGE_URL
                FROM NRM_COURSES c
                ORDER BY c.COURSE_NAME
            """

        cursor.execute(query)
        rows = cursor.fetchall() or []

        courses = []
        for idx, row in enumerate(rows, start=1):
            course_name = str(row.get("COURSE_NAME") or "").strip()
            if not course_name:
                continue

            courses.append(
                {
                    "id": row.get("ID"),
                    "course_code": row.get("COURSE_CODE") or "",
                    "subject": course_name,
                    "sub_id": f"sub{idx}",
                    "image_url": (row.get("IMAGE_URL") or "").strip(),
                }
            )

        return {
            "success": True,
            "courses": courses,
            "image_column": image_col or "",
        }
    except Exception as e:
        print(f"❌ resources_courses error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.get("/api/student/admin/course-offerings")
async def admin_course_offerings():
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT
                o.OFFERING_ID,
                o.COURSE_ID,
                c.COURSE_NAME,
                c.COURSE_CODE,
                o.REGISTRATION_CATEGORY,
                CAST(ROUND(COALESCE(o.COURSE_FEE, 0)) AS INT) AS COURSE_FEE,
                o.IS_ACTIVE
            FROM NRM_COURSE_OFFERINGS o
            JOIN NRM_COURSES c ON c.ID = o.COURSE_ID
            WHERE o.IS_ACTIVE = 'Y' OR o.IS_ACTIVE IS NULL
            ORDER BY o.REGISTRATION_CATEGORY, c.COURSE_NAME
            """
        )
        return cursor.fetchall() or []
    except Exception as e:
        print(f"❌ admin_course_offerings error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.post("/api/student/admin/courses")
async def admin_save_course(request: Request):
    payload = await request.json()
    course_name = str(payload.get("course_name") or "").strip()
    course_code = str(payload.get("course_code") or "").strip().upper()
    registration_category_input = str(payload.get("registration_category") or "").strip().lower()
    is_active = 'Y' if str(payload.get("is_active") or '').strip().upper() == 'Y' else 'N'

    aliases = {
        "student_co": "Live Course",
        "live course": "Live Course",
        "student_pl": "Placement Preparation",
        "placement preparation": "Placement Preparation",
        "student_ws": "Workshop",
        "workshop": "Workshop",
    }
    registration_category = aliases.get(registration_category_input, "")

    category_variants = {
        "Live Course": ["Live Course", "student_co", "live course"],
        "Placement Preparation": ["Placement Preparation", "student_pl", "placement preparation"],
        "Workshop": ["Workshop", "student_ws", "workshop"],
    }
    registration_category_candidates = category_variants.get(registration_category, [])

    if not course_name or not course_code or not registration_category:
        raise HTTPException(status_code=400, detail="course_name, course_code and registration_category are required")

    try:
        course_fee = float(payload.get("course_fee") or 0)
    except Exception:
        raise HTTPException(status_code=400, detail="course_fee must be a valid number")

    if course_fee < 0:
        raise HTTPException(status_code=400, detail="course_fee cannot be negative")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        def _is_course_fee_invalid_identifier(exc: Exception) -> bool:
            try:
                err_obj = exc.args[0] if getattr(exc, "args", None) else None
                err_code = getattr(err_obj, "code", None)
                err_msg = str(getattr(err_obj, "message", exc)).upper()
                return err_code == 904 and "COURSE_FEE" in err_msg
            except Exception:
                return "ORA-00904" in str(exc).upper() and "COURSE_FEE" in str(exc).upper()

        print(
            "[admin_save_course] payload",
            {
                "course_name": course_name,
                "course_code": course_code,
                "registration_category_input": registration_category_input,
                "registration_category": registration_category,
                "course_fee": course_fee,
                "is_active": is_active,
            },
        )

        cursor.execute("SELECT USER AS DB_USER FROM DUAL")
        db_user_row = cursor.fetchone() or {}
        print(f"[admin_save_course] db_user={db_user_row.get('DB_USER')}")

        cursor.execute(
            """
            SELECT COUNT(*) AS CNT
            FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = 'NRM_COURSE_OFFERINGS'
              AND COLUMN_NAME = 'COURSE_FEE'
            """
        )
        user_tab_cnt = int((cursor.fetchone() or {}).get("CNT") or 0)

        cursor.execute(
            """
            SELECT COUNT(*) AS CNT
            FROM ALL_TAB_COLUMNS
            WHERE TABLE_NAME = 'NRM_COURSE_OFFERINGS'
              AND COLUMN_NAME = 'COURSE_FEE'
            """
        )
        all_tab_cnt = int((cursor.fetchone() or {}).get("CNT") or 0)

        has_offering_course_fee = (user_tab_cnt > 0) or (all_tab_cnt > 0)
        print(
            "[admin_save_course] COURSE_FEE column check",
            {
                "user_tab_cnt": user_tab_cnt,
                "all_tab_cnt": all_tab_cnt,
                "has_offering_course_fee": has_offering_course_fee,
            },
        )

        cursor.execute(
            "SELECT ID, COURSE_NAME, COURSE_CODE FROM NRM_COURSES WHERE UPPER(COURSE_CODE) = UPPER(%s)",
            (course_code,),
        )
        course_by_code = cursor.fetchone()

        cursor.execute(
            "SELECT ID, COURSE_NAME, COURSE_CODE FROM NRM_COURSES WHERE UPPER(COURSE_NAME) = UPPER(%s)",
            (course_name,),
        )
        course_by_name = cursor.fetchone()

        if course_by_code and course_by_name and int(course_by_code["ID"]) != int(course_by_name["ID"]):
            raise HTTPException(status_code=400, detail="Course name and course code point to different existing courses")

        if course_by_code and str(course_by_code.get("COURSE_NAME") or "").strip().lower() != course_name.lower():
            raise HTTPException(status_code=400, detail="Course code already exists for another course")

        if course_by_name and str(course_by_name.get("COURSE_CODE") or "").strip().upper() != course_code:
            raise HTTPException(status_code=400, detail="Course name already exists with a different course code")

        course_row = course_by_code or course_by_name
        created_course = False

        if course_row:
            course_id = int(course_row["ID"])
        else:
            cursor.execute(
                "INSERT INTO NRM_COURSES (COURSE_NAME, COURSE_CODE) VALUES (%s, %s)",
                (course_name, course_code),
            )
            cursor.execute(
                "SELECT ID FROM NRM_COURSES WHERE UPPER(COURSE_CODE) = UPPER(%s)",
                (course_code,),
            )
            inserted_row = cursor.fetchone()
            course_id = int(inserted_row["ID"]) if inserted_row else 0
            created_course = True

        if not course_id:
            raise HTTPException(status_code=500, detail="Could not resolve course id after save")

        action_label = "updated"
        cursor.execute(
            """
            SELECT OFFERING_ID
            FROM NRM_COURSE_OFFERINGS
                        WHERE COURSE_ID = %s
                            AND (
                                LOWER(REGISTRATION_CATEGORY) = LOWER(%s)
                                OR LOWER(REGISTRATION_CATEGORY) = LOWER(%s)
                                OR LOWER(REGISTRATION_CATEGORY) = LOWER(%s)
                            )
            FETCH FIRST 1 ROWS ONLY
            """,
                        (
                                course_id,
                                registration_category_candidates[0],
                                registration_category_candidates[1],
                                registration_category_candidates[2],
                        ),
        )
        offering_row = cursor.fetchone()
        print(
            "[admin_save_course] offering lookup",
            {
                "course_id": course_id,
                "registration_category": registration_category,
                "offering_found": bool(offering_row),
                "has_offering_course_fee": has_offering_course_fee,
            },
        )

        if offering_row:
            print("[admin_save_course] updating offering with COURSE_FEE")
            try:
                cursor.execute(
                    """
                    UPDATE NRM_COURSE_OFFERINGS
                    SET COURSE_FEE = %s,
                        IS_ACTIVE = %s,
                        UPDATED_AT = CURRENT_TIMESTAMP
                    WHERE OFFERING_ID = %s
                    """,
                    (course_fee, is_active, int(offering_row["OFFERING_ID"])),
                )
            except Exception as fee_update_exc:
                if _is_course_fee_invalid_identifier(fee_update_exc):
                    print("[admin_save_course] retry update on CHAKORA.NRM_COURSE_OFFERINGS")
                    cursor.execute(
                        """
                        UPDATE CHAKORA.NRM_COURSE_OFFERINGS
                        SET COURSE_FEE = %s,
                            IS_ACTIVE = %s,
                            UPDATED_AT = CURRENT_TIMESTAMP
                        WHERE OFFERING_ID = %s
                        """,
                        (course_fee, is_active, int(offering_row["OFFERING_ID"])),
                    )
                else:
                    raise
            action_label = "updated"
        else:
            print("[admin_save_course] creating offering with COURSE_FEE")
            try:
                cursor.execute(
                    """
                    INSERT INTO NRM_COURSE_OFFERINGS
                        (COURSE_ID, REGISTRATION_CATEGORY, COURSE_FEE, IS_ACTIVE, CREATED_AT, UPDATED_AT)
                    VALUES
                        (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """,
                    (course_id, registration_category, course_fee, is_active),
                )
            except Exception as fee_insert_exc:
                if _is_course_fee_invalid_identifier(fee_insert_exc):
                    print("[admin_save_course] retry insert on CHAKORA.NRM_COURSE_OFFERINGS")
                    cursor.execute(
                        """
                        INSERT INTO CHAKORA.NRM_COURSE_OFFERINGS
                            (COURSE_ID, REGISTRATION_CATEGORY, COURSE_FEE, IS_ACTIVE, CREATED_AT, UPDATED_AT)
                        VALUES
                            (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        """,
                        (course_id, registration_category, course_fee, is_active),
                    )
                else:
                    raise
            action_label = "created"

        conn.commit()
        cache_freq_delete("shop:courses")

        message = f"Course created and {registration_category} offering {action_label} successfully." if created_course else f"Course offering {action_label} successfully."
        return {"success": True, "message": message, "course_id": course_id}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ admin_save_course error: {e}")
        err_obj = e.args[0] if getattr(e, "args", None) else None
        print(
            "[admin_save_course] error_details",
            {
                "type": type(e).__name__,
                "code": getattr(err_obj, "code", None),
                "message": str(getattr(err_obj, "message", e)),
            },
        )
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.delete("/api/student/admin/courses/{course_id}")
async def admin_delete_course(course_id: int):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            "SELECT ID, COURSE_NAME, COURSE_CODE FROM NRM_COURSES WHERE ID = %s",
            (course_id,),
        )
        course_row = cursor.fetchone()
        if not course_row:
            raise HTTPException(status_code=404, detail="Course not found")

        cursor.execute("SELECT COUNT(*) AS CNT FROM NRM_REGISTRATIONS WHERE COURSE_ID = %s", (course_id,))
        registrations_count = int((cursor.fetchone() or {}).get("CNT") or 0)
        if registrations_count > 0:
            raise HTTPException(status_code=400, detail="Cannot delete course because registrations already exist")

        cursor.execute("SELECT COUNT(*) AS CNT FROM NRM_BATCH_SCHEDULE WHERE COURSE_ID = %s", (course_id,))
        batch_count = int((cursor.fetchone() or {}).get("CNT") or 0)
        if batch_count > 0:
            raise HTTPException(status_code=400, detail="Cannot delete course because it is linked to batch schedules")

        cursor.execute("DELETE FROM NRM_COURSE_OFFERINGS WHERE COURSE_ID = %s", (course_id,))
        cursor.execute("DELETE FROM PRICING_LOOKUP WHERE COURSE_ID = %s", (course_id,))
        cursor.execute("DELETE FROM NRM_SYLLABUS WHERE COURSE_ID = %s", (course_id,))
        cursor.execute("DELETE FROM NRM_COURSE_FILES WHERE COURSE_ID = %s", (course_id,))
        cursor.execute("DELETE FROM NRM_COURSES WHERE ID = %s", (course_id,))

        if cursor.rowcount <= 0:
            raise HTTPException(status_code=404, detail="Course not found")

        conn.commit()
        cache_freq_delete("shop:courses")
        course_name = str(course_row.get("COURSE_NAME") or "").strip()
        course_code = str(course_row.get("COURSE_CODE") or "").strip()

        if course_name.lower() in {"none", "null", "nan"}:
            course_name = ""
        if course_code.lower() in {"none", "null", "nan"}:
            course_code = ""

        course_label = course_name or course_code or f"ID {course_id}"
        return {"success": True, "message": f"Course deleted successfully: {course_label}"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        print(f"❌ admin_delete_course error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()

# ═════════════════════════════════════════════════════════
# SHOP COURSE CATALOGUE  (live pricing for ChakoraHub shop)
# Cache: shop:courses  30 min
# ═════════════════════════════════════════════════════════
TTL_SHOP_COURSES = 1_800   # 30 min


@app.get("/api/student/shop-courses")
async def get_shop_courses():
    """
    Returns the active course catalogue with live pricing from Oracle PRICING_LOOKUP.
    Called by app.py /api/shop/courses to replace the hardcoded PRODUCTS array.
    Cached for 30 minutes under key shop:courses.
    """
    CACHE_KEY = "shop:courses"
    cached = cache_freq_get(CACHE_KEY)
    if isinstance(cached, list) and len(cached) > 0:
        print("🎯 shop:courses cache HIT")
        return {"success": True, "courses": cached}
    if cached == []:
        print("ℹ️ shop:courses cache had 0 rows; refreshing from Oracle")

    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")

        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT
                c.ID,
                c.COURSE_CODE,
                c.COURSE_NAME,
                CAST(ROUND(COALESCE(MAX(pl.PRICE), MAX(c.COURSE_FEE), 0)) AS INT) AS PRICE
            FROM NRM_COURSES c
            LEFT JOIN PRICING_LOOKUP pl ON c.ID = pl.COURSE_ID
            GROUP BY c.ID, c.COURSE_CODE, c.COURSE_NAME
            ORDER BY c.COURSE_NAME
            """
        )
        rows = cursor.fetchall() or []
        courses = [
            {
                "ID": row["ID"],
                "COURSE_CODE": row["COURSE_CODE"],
                "COURSE_NAME": row["COURSE_NAME"],
                "PRICE": int(row.get("PRICE") or 0),
            }
            for row in rows
        ]

        cache_freq_set(CACHE_KEY, courses, TTL_SHOP_COURSES)
        print(f"💾 shop:courses cached ({len(courses)} courses)")
        return {"success": True, "courses": courses}
    except Exception as e:
        print(f"❌ get_shop_courses error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ═════════════════════════════════════════════════════════
# DASHBOARD RESOURCES (public shared data for resources page)
# ═════════════════════════════════════════════════════════
@app.get("/api/student/dashboard-resources")
async def dashboard_resources():
    today = datetime.today().strftime("%Y-%m-%d")
    print(f"🏠 [dashboard-resources] request start | today={today}")

    offers_key = f"offers:{today}"
    offers = cache_freq_get(offers_key)
    festival_key = f"festival:{today}"
    festival = cache_freq_get(festival_key)
    print(
        "🏠 [dashboard-resources] cache snapshot | "
        f"offers_found={offers is not None} festival_found={festival is not None}"
    )

    conn = None
    cursor = None
    try:
        if offers is None or festival is None:
            conn = get_db_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            cursor = conn.cursor(DictCursor)
            print("🏠 [dashboard-resources] DB connection opened for cache miss path")

            if offers is None:
                print(f"🔎 [dashboard-resources] querying offers | key={offers_key}")
                cursor.execute(
                    """
                    SELECT c.COURSE_NAME, c.COURSE_FEE, o.DISCOUNT_PERCENTAGE
                    FROM NRM_OFFERS o
                    JOIN NRM_COURSES c ON c.ID = o.COURSE_ID
                    WHERE o.IS_ACTIVE = TRUE
                      AND (%s BETWEEN o.VALID_FROM AND o.VALID_TO
                           OR o.VALID_FROM IS NULL OR o.VALID_TO IS NULL)
                    """,
                    (today,),
                )
                offer_rows = cursor.fetchall() or []
                offers = {}
                for row in offer_rows:
                    fee = float(row.get("COURSE_FEE") or 0)
                    disc = float(row.get("DISCOUNT_PERCENTAGE") or 0)
                    offers[row["COURSE_NAME"]] = {
                        "original_fee": int(fee),
                        "discounted_fee": int(fee - fee * disc / 100),
                        "discount_percentage": int(disc),
                    }
                cache_freq_set(offers_key, offers, TTL_OFFERS)
                print(f"✅ [dashboard-resources] offers cached | count={len(offers) if isinstance(offers, dict) else '?'}")

            if festival is None:
                print(f"🔎 [dashboard-resources] querying festival | key={festival_key}")
                cursor.execute(
                    "SELECT FESTIVAL_NAME FROM NRM_FESTIVALS WHERE FESTIVAL_DATE = %s",
                    (today,),
                )
                festival_row = cursor.fetchone()
                festival = festival_row["FESTIVAL_NAME"] if festival_row else None
                cache_freq_set(festival_key, festival or "", TTL_FESTIVALS)
                print(f"✅ [dashboard-resources] festival cached | value={festival!r}")

        greeting = f"Happy {festival}" if festival else None
        print(
            "✅ [dashboard-resources] response ready | "
            f"offers_keys={list((offers or {}).keys())[:5]} festival={festival!r}"
        )
        return {
            "success": True,
            "offers": offers or {},
            "festival_today": festival,
            "greeting": greeting,
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Dashboard resources error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.post("/api/student/resources")
async def get_dashboard_legacy(payload: DashboardRequest):
    """Backward-compatible alias for older clients still posting to /api/student/resources."""
    print("↩️ [api/student/resources] routed to /api/student/dashboard")
    return await get_dashboard(payload)


# ═════════════════════════════════════════════════════════
# LOGIN
# Cache: writes session:{user_id}, user:{user_id}, roles:{user_id}
# ═════════════════════════════════════════════════════════
@app.post("/api/student/login", response_model=LoginResponse)
async def student_login(payload: LoginRequest):
    print(f"👤 Login attempt: {payload.username}")
    print(
        "👤 [student_login] payload snapshot | "
        f"username={payload.username!r} login_type={getattr(payload, 'login_type', None)!r}"
    )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        print("👤 [student_login] DB connection opened")
        cursor = conn.cursor(DictCursor)
        print("👤 [student_login] executing user lookup query")
        cursor.execute("""
            SELECT
                u.ID          AS USER_ID,
                u.EMAIL,
                u.PHONE,
                u.USERTYPE,
                u.PROFILE_PIC,
                l.PASSWORD
            FROM NRM_USERS u
            JOIN NRM_LOGINS l ON u.ID = l.USER_ID
            WHERE LOWER(TRIM(u.EMAIL)) = LOWER(TRIM(%s))
               OR TRIM(u.PHONE)        = TRIM(%s)
            ORDER BY l.CREATED_AT DESC
            FETCH FIRST 1 ROWS ONLY
        """, (payload.username, payload.username))

        user = cursor.fetchone()
        print(f"👤 [student_login] lookup result | found={bool(user)}")
        if not user:
            print(f"❌ [student_login] user not found | username={payload.username}")
            return LoginResponse(success=False, message="User not found")

        # ── Password check ─────────────────────────────────────────
        db_pw = user.get("PASSWORD") or ""
        if db_pw.startswith(("scrypt:", "$2a$", "$2b$", "pbkdf2:")):
            valid = check_password_hash(db_pw, payload.password)
        else:
            valid = (db_pw == payload.password)
        print(
            "👤 [student_login] password check | "
            f"user_id={user['USER_ID']} valid={valid} hash_prefix={db_pw[:12]!r}"
        )

        if not valid:
            print(f"❌ [student_login] incorrect password | user_id={user['USER_ID']}")
            return LoginResponse(success=False, message="Incorrect password")

        # ── Mark login active ───────────────────────────────────────
        cursor.execute("""
            UPDATE NRM_LOGINS
               SET IS_ACTIVE = 'Y', LAST_LOGIN = CURRENT_TIMESTAMP
             WHERE USER_ID = %s
        """, (user["USER_ID"],))
        conn.commit()
        print(f"✅ [student_login] login marked active | user_id={user['USER_ID']}")

        user_id  = int(user["USER_ID"])
        usertype = (user.get("USERTYPE") or "student").strip().lower()
        email    = user.get("EMAIL") or ""
        phone    = user.get("PHONE") or ""
        pic      = user.get("PROFILE_PIC") or "profile_photo/defaultpicture.jpg"
        is_admin = usertype in {"admin", "superadmin"}

        # ── Fetch roles from NRM_USER_ROLES if it exists ────────────
        roles = [usertype]
        try:
            cursor.execute("""
                SELECT ROLE_NAME FROM NRM_USER_ROLES WHERE USER_ID = %s
            """, (user_id,))
            role_rows = cursor.fetchall() or []
            if role_rows:
                roles = [r["ROLE_NAME"] for r in role_rows]
            print(f"👤 [student_login] roles resolved | roles={roles}")
        except Exception:
            pass   # table may not exist yet; fall back to usertype as role

        # ──────────────────────────────────────────────────────────────
        # WRITE ALL THREE CACHES on successful login
        # ──────────────────────────────────────────────────────────────
        print(
            "📦 [student_login] writing resources cache | "
            f"user_id={user_id} email={email or phone} usertype={usertype}"
        )
        session_data = {
            "user_id": user_id, "email": email, "phone": phone,
            "usertype": usertype, "is_admin": is_admin,
            "login_ts": datetime.now().isoformat(),
        }
        cache_session_set(user_id, session_data)     # session:{user_id}  7 days

        profile_data = {
            "user_id": user_id, "email": email, "phone": phone,
            "usertype": usertype, "profile_pic": pic, "is_admin": is_admin,
        }
        cache_profile_set(user_id, profile_data)     # user:{user_id}     30 min
        # Resources page profile cache
        _cache_set(
            f"resources:session:{user_id}",
            {
                "user_id": str(user_id),
                "username": email or phone,
                "usertype": usertype,
                "profile_pic": pic,
            },
        )
        cache_auth_set(user_id, roles, usertype)     # roles:{user_id}    1 hr

        print(f"✅ Login OK: user_id={user_id} usertype={usertype} roles={roles}")

        return LoginResponse(
            success=True, message="Login successful",
            user_id=user_id, username=email or phone,
            email=email, phone=phone,
            usertype=usertype, profile_pic=pic, is_admin=is_admin,
        )

    except Exception as e:
        print(f"❌ Login error: {e}")
        raise HTTPException(status_code=500, detail=f"Login failed: {e}")
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# LOGOUT
# Cache: deletes session:{user_id}, user:{user_id}, roles:{user_id},
#        student:dashboard:{user_id}
# ═════════════════════════════════════════════════════════
@app.post("/api/student/logout")
async def student_logout(payload: LogoutRequest):
    user_id = payload.user_id
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cursor = conn.cursor()
        cursor.execute("UPDATE NRM_LOGINS SET IS_ACTIVE = 'N' WHERE USER_ID = %s", (user_id,))
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"⚠️  Logout DB update failed: {e}")
    finally:
        conn.close()

    # Clear all caches for this user
    cache_session_delete(user_id)
    cache_profile_delete(user_id)
    cache_auth_delete(user_id)
    cache_dashboard_delete(user_id)
    print(f"🗑️  All caches cleared: user_id={user_id}")

    return {"success": True, "message": "Logged out successfully"}


# ═════════════════════════════════════════════════════════
# STUDENT REGISTRATION
# Used by Website-Git /registration POST proxy
# ═════════════════════════════════════════════════════════
@app.post("/api/student/registration/upload-resume")
async def upload_registration_resume(
    resume: UploadFile = File(...),
    registration_type: Optional[str] = Form("student_pl"),
):
    reg_type = str(registration_type or "student_pl").strip().lower()
    if reg_type != "student_pl":
        raise HTTPException(status_code=400, detail="Resume upload is only allowed for placement preparation.")

    if not resume or not resume.filename:
        raise HTTPException(status_code=400, detail="Resume file is required.")

    ext = os.path.splitext(resume.filename)[1].lower()
    if ext not in {".pdf", ".doc", ".docx"}:
        raise HTTPException(status_code=400, detail="Allowed resume formats: PDF, DOC, DOCX.")

    resume.file.seek(0, os.SEEK_END)
    file_size = resume.file.tell()
    resume.file.seek(0)
    if file_size > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Resume file must be under 5MB.")

    safe_name = secure_filename(resume.filename)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    stored_name = f"{timestamp}_{uuid.uuid4().hex[:8]}_{safe_name}"
    file_path = os.path.join(UPLOAD_ROOT, "resumes", stored_name)

    try:
        _save_upload_file(resume, file_path)
        s3_key = _upload_student_content_to_s3(file_path, "resume", "Placement")
        if not s3_key:
            raise HTTPException(status_code=500, detail="Unable to upload resume to S3.")
        return {
            "success": True,
            "resume_file_name": safe_name,
            "resume_s3_key": s3_key,
        }
    finally:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass


@app.post("/api/student/registration")
async def student_registration(payload: StudentRegistrationRequest):
    registration_type = str(payload.registration_type or "").strip().lower()
    first_name = str(payload.first_name or "").strip()
    last_name = str(payload.last_name or "").strip()
    email = str(payload.email or "").strip().lower()
    phone = str(payload.phone or "").strip()
    location_val = str(payload.location or "").strip()
    course_id = payload.course
    offering_id = payload.offering_id
    language_id = payload.language
    start_date_raw = str(payload.start_date or "").strip()
    payment_id = str(payload.payment_id or payload.gateway_payment_id or payload.razorpay_payment_id or "").strip()
    order_id = str(payload.order_id or payload.gateway_order_id or payload.razorpay_order_id or "").strip()
    signature = str(payload.signature or payload.gateway_signature or payload.razorpay_signature or "").strip()
    qualification = str(payload.qualification or "").strip()
    college = str(payload.college or "").strip()
    branch = str(payload.branch or "").strip()
    experience = str(payload.experience or "").strip()
    resume_file_name = str(payload.resume_file_name or "").strip()
    resume_s3_key = str(payload.resume_s3_key or "").strip()
    passing_year = payload.passing_year

    if registration_type not in {"student_co", "student_pl", "student_ws"}:
        raise HTTPException(status_code=400, detail="Invalid registration type.")

    if not first_name or not last_name:
        raise HTTPException(status_code=400, detail="First name and last name are required.")
    if not email:
        raise HTTPException(status_code=400, detail="Email is required.")
    if not (email.endswith("@gmail.com") or email.endswith("@chakorahub.com")):
        raise HTTPException(status_code=400, detail="Email must end with @gmail.com or @chakorahub.com")
    if not phone.isdigit() or len(phone) != 10:
        raise HTTPException(status_code=400, detail="Phone number must be exactly 10 digits.")
    if not location_val:
        raise HTTPException(status_code=400, detail="Location is required.")

    if registration_type == "student_pl":
        if not qualification or not college or not branch or not experience:
            raise HTTPException(status_code=400, detail="Placement fields are required.")
        try:
            passing_year = int(passing_year)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Invalid passing year.")
        if passing_year < 1980 or passing_year > datetime.now().year + 5:
            raise HTTPException(status_code=400, detail="Invalid passing year.")
        if not resume_file_name or not resume_s3_key:
            raise HTTPException(status_code=400, detail="Resume upload is required for placement preparation.")
    else:
        qualification = None
        college = None
        branch = None
        passing_year = None
        experience = None
        resume_file_name = None
        resume_s3_key = None

    try:
        course_id = int(course_id)
        language_id = int(language_id)
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="Invalid course or language selection.")

    if not start_date_raw:
        # Backward compatibility: older website payloads do not send start_date.
        start_date = datetime.now()
    else:
        try:
            start_date = datetime.strptime(start_date_raw, "%Y-%m-%d")
            if start_date.date() < datetime.now().date():
                raise HTTPException(status_code=400, detail="Start date cannot be in the past.")
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            "SELECT COURSE_CODE, COURSE_NAME FROM NRM_COURSES WHERE ID = %s",
            (course_id,),
        )
        course_row = cursor.fetchone()
        if not course_row:
            raise HTTPException(status_code=400, detail="Invalid course selected.")
        course_code = course_row["COURSE_CODE"]
        course_name = course_row["COURSE_NAME"]

        cursor.execute(
            """
            SELECT OFFERING_ID, COALESCE(COURSE_FEE, 0) AS COURSE_FEE
            FROM NRM_COURSE_OFFERINGS
                        WHERE OFFERING_ID = %s
                            AND COURSE_ID = %s
                            AND LOWER(TRIM(REGISTRATION_CATEGORY)) IN (%s, %s)
              AND (IS_ACTIVE = 'Y' OR IS_ACTIVE IS NULL)
            FETCH FIRST 1 ROWS ONLY
            """,
                        (
                            offering_id,
                            course_id,
                            registration_type,
                            {
                                "student_co": "live course",
                                "student_pl": "placement preparation",
                                "student_ws": "workshop",
                            }.get(registration_type, registration_type),
                        ),
        )
        offering_row = cursor.fetchone()
        if not offering_row:
            raise HTTPException(status_code=400, detail="Course offering not found for selected registration category.")

        offering_id = int(offering_row["OFFERING_ID"])
        course_fee = int(offering_row.get("COURSE_FEE") or 0)

        if course_fee > 0 and not all([payment_id, order_id, signature]):
            raise HTTPException(status_code=400, detail="Payment required. Please complete Razorpay payment.")
        if course_fee <= 0:
            payment_id = payment_id or "FREE"
            order_id = order_id or "FREE"
            signature = signature or "FREE"

        cursor.execute("SELECT LANGUAGE FROM NRM_LANGUAGES WHERE ID = %s", (language_id,))
        language_row = cursor.fetchone()
        if not language_row:
            raise HTTPException(status_code=400, detail="Invalid language selected.")
        language_name = language_row["LANGUAGE"]

        cursor.execute("SELECT ID FROM NRM_STATUSES WHERE UPPER(STATUS) = %s FETCH FIRST 1 ROWS ONLY", ("ACTIVE",))
        status_row = cursor.fetchone()
        if not status_row:
            raise HTTPException(status_code=400, detail="Active status not found in database.")
        active_id = status_row["ID"]

        cursor.execute("SELECT ID FROM NRM_USERS WHERE UPPER(EMAIL) = UPPER(%s) ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY", (email,))
        existing_user = cursor.fetchone()

        if existing_user:
            user_id = int(existing_user["ID"])
            cursor.execute("SELECT ID FROM NRM_STUDENTS WHERE USER_ID = %s ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY", (user_id,))
            existing_student = cursor.fetchone()
            if existing_student:
                student_id = int(existing_student["ID"])
            else:
                cursor.execute(
                    "INSERT INTO NRM_STUDENTS (LOCATION, REGISTRATION_SOURCE, USER_ID) VALUES (%s, %s, %s)",
                    (location_val, "website", user_id),
                )
                cursor.execute("SELECT ID FROM NRM_STUDENTS WHERE USER_ID = %s ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY", (user_id,))
                student_id = int(cursor.fetchone()["ID"])
        else:
            full_name = f"{first_name} {last_name}".strip()
            cursor.execute(
                """
                INSERT INTO NRM_USERS (USERNAME, EMAIL, PHONE, PROFILE_PIC, USERTYPE, REGISTRATION_SOURCE)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (full_name, email, phone, "default.jpg", registration_type, "website"),
            )
            cursor.execute("SELECT ID FROM NRM_USERS WHERE UPPER(EMAIL) = UPPER(%s) ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY", (email,))
            user_id = int(cursor.fetchone()["ID"])

            cursor.execute(
                "INSERT INTO NRM_STUDENTS (LOCATION, REGISTRATION_SOURCE, USER_ID) VALUES (%s, %s, %s)",
                (location_val, "website", user_id),
            )
            cursor.execute("SELECT ID FROM NRM_STUDENTS WHERE USER_ID = %s ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY", (user_id,))
            student_id = int(cursor.fetchone()["ID"])

            hashed_pwd = generate_password_hash("changeme123")
            cursor.execute(
                "INSERT INTO NRM_LOGINS (USER_ID, PASSWORD, IS_ACTIVE) VALUES (%s, %s, %s)",
                (user_id, hashed_pwd, "Y"),
            )

        reg_id = _build_student_registration_id(cursor, course_id, course_code, first_name, last_name, start_date)
        cursor.execute(
            """
            INSERT INTO NRM_REGISTRATIONS
            (REGISTRATION_ID, STUDENT_ID, COURSE_ID, LANGUAGE_ID, STATUS_ID, CREATED_DT, OFFERING_ID,
             QUALIFICATION, COLLEGE, BRANCH, PASSING_YEAR, EXPERIENCE, RESUME_FILE_NAME, RESUME_S3_KEY, RESUME_UPLOADED_AT)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s,
                    %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            (
                reg_id,
                student_id,
                course_id,
                language_id,
                active_id,
                offering_id,
                qualification,
                college,
                branch,
                passing_year,
                experience,
                resume_file_name,
                resume_s3_key,
            ),
        )
        conn.commit()

        kafka_publish("registration.completed", {
            "event_id": str(uuid.uuid4()),
            "event_type": "registration.completed",
            "timestamp": datetime.utcnow().isoformat(),
            "registration_id": reg_id,
            "user_id": user_id,
            "student_name": f"{first_name} {last_name}".strip(),
            "student_email": email,
            "course_name": course_name,
            "course_id": course_id,
            "offering_id": offering_id,
            "language_id": language_id,
            "payment_id": payment_id,
            "order_id": order_id,
            "payment_amount": course_fee,
            "registration_type": registration_type,
        })
        print(f"✅ Published registration.completed | reg_id={reg_id} user_id={user_id}")

        return {
            "success": True,
            "message": "✅ Student registered successfully!",
            "registration_id": reg_id,
            "course": course_name,
            "email": email,
            "language": language_name,
        }
    except HTTPException:
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        print("❌ Student registration error FULL TRACE:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# DASHBOARD  (formerly /api/student/resources)
# Cache: reads/writes student:dashboard:{user_id}  5 min
#        also reads  user:{user_id}, offers:{date}, festival:{date}
# ═════════════════════════════════════════════════════════
@app.post("/api/student/dashboard")
async def get_dashboard(payload: DashboardRequest):
    user_id = payload.user_id

    # ── Cart isolation: student_sc users have no course access ───────────────
    auth_cached = cache_auth_get(user_id)
    if auth_cached:
        ut = (auth_cached.get("usertype") or "").lower()
        if ut == "student_sc":
            raise HTTPException(
                status_code=403,
                detail="Your account is in shopping cart state. "
                       "Please complete enrollment to access course resources."
            )

    # ── L1: Full dashboard response cache (5 min) ────────────────
    cached = cache_dashboard_get(user_id)
    if cached:
        print(f"🎯 Dashboard cache HIT: user_id={user_id}")
        return cached

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)

        # ── L2: Profile cache (30 min) ───────────────────────────
        profile = cache_profile_get(user_id)
        if not profile:
            cursor.execute("""
                SELECT ID, EMAIL, PHONE, USERTYPE, PROFILE_PIC
                FROM NRM_USERS WHERE ID = %s FETCH FIRST 1 ROWS ONLY
            """, (user_id,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="User not found")
            profile = {
                "user_id":     int(row["ID"]),
                "email":       row.get("EMAIL") or "",
                "phone":       row.get("PHONE") or "",
                "usertype":    (row.get("USERTYPE") or "student").lower(),
                "profile_pic": row.get("PROFILE_PIC") or "profile_photo/defaultpicture.jpg",
            }
            cache_profile_set(user_id, profile)
        else:
            print(f"🎯 Profile cache HIT: user_id={user_id}")

        # ── L2: Auth cache (1 hr) ────────────────────────────────
        auth = cache_auth_get(user_id)
        if not auth:
            auth = {"roles": [profile["usertype"]], "usertype": profile["usertype"]}
            cache_auth_set(user_id, auth["roles"], auth["usertype"])
        else:
            print(f"🎯 Auth cache HIT: user_id={user_id}")

        today = datetime.today().strftime("%Y-%m-%d")

        # ── L3: Offers (1 hr) ────────────────────────────────────
        offers_key = f"offers:{today}"
        offers = cache_freq_get(offers_key)
        if offers is None:
            cursor.execute("""
                SELECT c.COURSE_NAME, c.COURSE_FEE, o.DISCOUNT_PERCENTAGE
                FROM NRM_OFFERS o
                JOIN NRM_COURSES c ON c.ID = o.COURSE_ID
                WHERE o.IS_ACTIVE = TRUE
                  AND (%s BETWEEN o.VALID_FROM AND o.VALID_TO
                       OR o.VALID_FROM IS NULL OR o.VALID_TO IS NULL)
            """, (today,))
            rows = cursor.fetchall() or []
            offers = {}
            for r in rows:
                fee  = float(r.get("COURSE_FEE") or 0)
                disc = float(r.get("DISCOUNT_PERCENTAGE") or 0)
                offers[r["COURSE_NAME"]] = {
                    "original_fee":        int(fee),
                    "discounted_fee":      int(fee - fee * disc / 100),
                    "discount_percentage": int(disc),
                }
            cache_freq_set(offers_key, offers, TTL_OFFERS)
            print(f"💾 Offers cached for {today}")
        else:
            print(f"🎯 Offers cache HIT: {today}")

        # ── L3: Festival (24 hr) ─────────────────────────────────
        festival_key = f"festival:{today}"
        festival = cache_freq_get(festival_key)
        if festival is None:
            cursor.execute(
                "SELECT FESTIVAL_NAME FROM NRM_FESTIVALS WHERE FESTIVAL_DATE = %s",
                (today,)
            )
            fr = cursor.fetchone()
            festival = fr["FESTIVAL_NAME"] if fr else ""
            cache_freq_set(festival_key, festival, TTL_FESTIVALS)
        else:
            print(f"🎯 Festival cache HIT: {today}")

        # ── Build response ───────────────────────────────────────
        response = {
            "success":       True,
            "user":          profile,
            "roles":         auth.get("roles", []),
            "is_admin":      auth.get("usertype", "") in {"admin", "superadmin"},
            "offers":        offers,
            "festival_today": festival or None,
            "cached_at":     datetime.now().isoformat(),
        }

        cache_dashboard_set(user_id, response)    # student:dashboard:{user_id}  5 min
        return response

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Dashboard error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# GET PROFILE
# Cache: reads user:{user_id}
# ═════════════════════════════════════════════════════════
@app.post("/api/student/profile")
async def get_profile(payload: ProfileRequest):
    user_id = payload.user_id

    # Try full profile from cache first
    cached = cache_profile_get(user_id)
    if cached and "address" in cached:
        print(f"🎯 Profile+address cache HIT: user_id={user_id}")
        return {"success": True, "profile": cached}

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)

        cursor.execute("""
            SELECT u.ID, u.EMAIL, u.PHONE, u.USERTYPE, u.PROFILE_PIC,
                   s.ADDRESS, s.FIRST_NAME, s.LAST_NAME
            FROM NRM_USERS u
            LEFT JOIN NRM_STUDENTS s ON s.USER_ID = u.ID
            WHERE u.ID = %s
            FETCH FIRST 1 ROWS ONLY
        """, (user_id,))
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="User not found")

        profile = {
            "user_id":     int(row["ID"]),
            "email":       row.get("EMAIL") or "",
            "phone":       row.get("PHONE") or "",
            "usertype":    (row.get("USERTYPE") or "student").lower(),
            "profile_pic": row.get("PROFILE_PIC") or "profile_photo/defaultpicture.jpg",
            "first_name":  row.get("FIRST_NAME") or "",
            "last_name":   row.get("LAST_NAME") or "",
            "address":     row.get("ADDRESS") or "",
        }

        cache_profile_set(user_id, profile)    # update cache with address
        return {"success": True, "profile": profile}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Profile fetch error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# UPDATE PROFILE
# Cache: invalidates user:{user_id}, student:dashboard:{user_id}
# ═════════════════════════════════════════════════════════
@app.put("/api/student/profile")
async def update_profile(payload: UpdateProfileRequest):
    user_id = payload.user_id
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute("SELECT EMAIL FROM NRM_USERS WHERE ID = %s", (user_id,))
        user = cursor.fetchone()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        email = user["EMAIL"]

        if payload.address is not None:
            cursor.execute(
                "UPDATE NRM_STUDENTS SET ADDRESS = %s WHERE USER_ID = %s",
                (payload.address, user_id)
            )
        if payload.phone is not None:
            cursor.execute(
                "UPDATE NRM_USERS SET PHONE = %s WHERE ID = %s",
                (payload.phone, user_id)
            )
        if payload.profile_pic is not None:
            cursor.execute(
                "UPDATE NRM_USERS SET PROFILE_PIC = %s WHERE ID = %s",
                (payload.profile_pic, user_id)
            )
        conn.commit()

        # Invalidate stale caches
        cache_profile_delete(user_id)       # user:{user_id}
        cache_dashboard_delete(user_id)     # student:dashboard:{user_id}
        print(f"🗑️  Profile + dashboard cache invalidated: user_id={user_id}")

        return {"success": True, "message": "Profile updated successfully"}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Profile update error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# COURSE VIDEOS
# Cache: resources:{subject}:{lang}  DB 3  2 hr
# ═════════════════════════════════════════════════════════
@app.get("/api/student/course-videos")
async def get_course_videos(subject: str, lang: str = "telugu"):
    subject          = subject.strip()
    lang             = (lang.strip() or "telugu").lower()
    real_course_name = COURSE_NAME_MAP.get(subject.lower().strip(), subject)
    cache_key        = f"resources:{real_course_name}:{lang}"

    cached = cache_freq_get(cache_key)
    if cached is not None:
        print(f"🎯 Videos cache HIT: {cache_key}")
        return {"success": True, "videos": cached, "from_cache": True}

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)

        cursor.execute(
            "SELECT ID FROM NRM_COURSES WHERE LOWER(COURSE_NAME) = LOWER(%s) FETCH FIRST 1 ROWS ONLY",
            (real_course_name,)
        )
        course = cursor.fetchone()
        if not course:
            return {"success": True, "videos": [], "from_cache": False}
        course_id = course["ID"]

        cursor.execute(
            "SELECT ID FROM NRM_LANGUAGES WHERE LOWER(LANGUAGE) = LOWER(%s) FETCH FIRST 1 ROWS ONLY",
            (lang,)
        )
        lang_row = cursor.fetchone()

        if lang_row:
            cursor.execute("""
                SELECT YOUTUBE_ID, TITLE, SESSION_NUMBER
                FROM NRM_VIDEO_SESSIONS
                WHERE COURSE_ID = %s AND LANGUAGE_ID = %s
                ORDER BY SESSION_NUMBER ASC
            """, (course_id, lang_row["ID"]))
            rows = cursor.fetchall() or []

        # Fallback: any language
        if not lang_row or not rows:
            cursor.execute("""
                SELECT YOUTUBE_ID, TITLE, SESSION_NUMBER
                FROM NRM_VIDEO_SESSIONS
                WHERE COURSE_ID = %s
                ORDER BY SESSION_NUMBER ASC
            """, (course_id,))
            rows = cursor.fetchall() or []

        videos = [
            {
                "youtube_id":     r["YOUTUBE_ID"],
                "youtube_url":    f"https://www.youtube.com/watch?v={r['YOUTUBE_ID']}",
                "title":          r.get("TITLE") or "",
                "session_number": r.get("SESSION_NUMBER"),
            }
            for r in rows
        ]

        cache_freq_set(cache_key, videos, TTL_RESOURCES)
        print(f"💾 Videos cached: {cache_key} ({len(videos)} sessions)")
        return {"success": True, "videos": videos, "from_cache": False}

    except Exception as e:
        print(f"❌ Course videos error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# COURSE RESOURCES  (PPTs, Code, Interview Questions, Syllabus)
# Cache: resources:{subject}:files  DB 3  2 hr
# ═════════════════════════════════════════════════════════
@app.get("/api/student/course-resources")
async def get_course_resources(
    subject: str,
    user_id: int,
    usertype: str
):
    subject          = subject.strip()
    real_course_name = COURSE_NAME_MAP.get(subject.lower().strip(), subject)
    cache_key        = f"resources:{real_course_name}:files"

    cached = cache_freq_get(cache_key)
    if cached is not None:
        print(f"🎯 Resources cache HIT: {cache_key}")
        return {**cached, "from_cache": True}

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute("""
            SELECT cf.FILE_TYPE, cf.FILE_TITLE, cf.FILE_LABEL, cf.FILE_URL
            FROM NRM_COURSE_FILES cf
            JOIN NRM_COURSES c ON c.ID = cf.COURSE_ID
            WHERE LOWER(c.COURSE_NAME) = LOWER(%s)
              AND cf.IS_ACTIVE = TRUE
            ORDER BY cf.FILE_TYPE, cf.FILE_ID ASC
        """, (real_course_name,))
        rows = cursor.fetchall() or []

        if usertype == "student_ht":
            allowed = {
                "Informatica MDM",
                "Unix"
            }
        elif usertype == "student_mi":
            allowed = {
                "Python for web development",
                "AI & ML"
            }
        elif usertype == "student":
            allowed = None
        else:
            allowed = set()

        if allowed is not None:
            if real_course_name not in allowed:
                return {
                    "success": True,
                    "subject": subject,
                    "ppts": [],
                    "code": [],
                    "interview": [],
                    "syllabus": []
                }

        ppts, code, interview, syllabus = [], [], [], []
        for r in rows:
            item  = {
                "title": r.get("FILE_TITLE") or "",
                "label": r.get("FILE_LABEL") or "",
                "url":   r.get("FILE_URL")   or "",
            }
            ftype = (r.get("FILE_TYPE") or "").upper()
            if   ftype == "PPT":       ppts.append(item)
            elif ftype == "CODE":      code.append(item)
            elif ftype == "INTERVIEW": interview.append(item)
            elif ftype == "SYLLABUS":  syllabus.append(item)

        response = {
            "success":   True,
            "subject":   subject,
            "ppts":      ppts,
            "code":      code,
            "interview": interview,
            "syllabus":  syllabus,
        }
        cache_freq_set(cache_key, response, TTL_RESOURCES)
        print(f"💾 Resources cached: {cache_key}")
        return {**response, "from_cache": False}

    except Exception as e:
        print(f"❌ Course resources error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# FEEDBACKS  (public — no auth needed)
# Cache: feedbacks:latest  DB 3  10 min
# ═════════════════════════════════════════════════════════
@app.get("/api/student/feedbacks")
async def get_feedbacks():
    cached = cache_freq_get("feedbacks:latest")
    if cached is not None:
        print("🎯 Feedbacks cache HIT")
        return {"success": True, "feedbacks": cached, "from_cache": True}

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute("""
            SELECT f.FEEDBACK_MESSAGE,
                   COALESCE(u.USERNAME, s.FIRST_NAME, 'Anonymous') AS DISPLAY_NAME,
                   f.SUBMITTED_AT
            FROM NRM_FEEDBACK f
            LEFT JOIN NRM_STUDENTS s ON f.STUDENT_ID = s.ID
            LEFT JOIN NRM_USERS    u ON s.USER_ID    = u.ID
            WHERE f.FEEDBACK_MESSAGE IS NOT NULL
              AND TRIM(f.FEEDBACK_MESSAGE) != ''
            ORDER BY f.SUBMITTED_AT DESC
            FETCH FIRST 20 ROWS ONLY
        """)
        feedbacks = cursor.fetchall() or []
        cache_freq_set("feedbacks:latest", feedbacks, TTL_FEEDBACKS)
        return {"success": True, "feedbacks": feedbacks, "from_cache": False}

    except Exception as e:
        print(f"❌ Feedbacks error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# FEEDBACK EMAIL HELPER
# Sends two emails on every feedback submission:
#   1. Student  — "Thank you for your feedback!"
#   2. Admin    — "New Feedback Received" notification
# ═════════════════════════════════════════════════════════
def send_feedback_email(
    student_email: str,
    student_name: str,
    feedback_text: str,
    phone: str = "",
    rating: int = None,
):
    """Send feedback confirmation emails via AWS SES. Non-fatal — caller swallows exceptions."""
    try:
        TEAL       = "#00897b"
        TEAL_DARK  = "#00695c"
        TEAL_LIGHT = "#e0f2f1"
        GRAY_BG    = "#f5f5f5"
        BORDER     = "#e0e0e0"

        def star_display(r):
            if not r:
                return "Not rated"
            filled = "&#9733;" * int(r)
            empty  = "&#9734;" * (5 - int(r))
            return (
                f'<span style="color:#f59e0b;font-size:18px;">{filled}</span>'
                f'<span style="color:#d1d5db;font-size:18px;">{empty}</span>'
                f'&nbsp;<span style="font-size:13px;color:#616161;">({r}/5)</span>'
            )

        def row(label, value, last=False):
            bb = "" if last else f"border-bottom:1px solid {BORDER};"
            return (
                f'<tr>'
                f'<td style="width:35%;padding:11px 14px;background:{GRAY_BG};'
                f'font-weight:600;font-size:13px;color:#424242;{bb}">{label}</td>'
                f'<td style="width:65%;padding:11px 14px;background:#fff;'
                f'font-size:13px;color:#212121;{bb}">{value}</td>'
                f'</tr>'
            )

        def section_header(text):
            return (
                f'<tr><th colspan="2" style="background:{TEAL};color:#fff;'
                f'padding:10px 14px;text-align:left;font-size:13px;'
                f'font-weight:600;">{text}</th></tr>'
            )

        def table_wrap(inner_rows):
            return (
                f'<table style="width:100%;border-collapse:collapse;'
                f'border:1px solid {BORDER};border-radius:4px;overflow:hidden;'
                f'margin-bottom:20px;">{inner_rows}</table>'
            )

        def header_block(title, subtitle="", icon="&#128172;"):
            sub = f'<p style="margin:4px 0 0;font-size:13px;color:#ccf2ee;">{subtitle}</p>' if subtitle else ""
            return (
                f'<div style="background-color:{TEAL};color:#ffffff;padding:28px 24px;text-align:center;">'
                f'<div style="font-size:28px;margin-bottom:8px;">{icon}</div>'
                f'<h2 style="margin:0;font-size:22px;font-weight:700;color:#ffffff;">{title}</h2>{sub}'
                f'</div>'
            )

        def footer_block():
            return (
                f'<div style="background:{GRAY_BG};padding:18px 24px;text-align:center;border-top:1px solid {BORDER};">'
                f'<p style="margin:0 0 4px;font-size:13px;color:#424242;">Regards, <strong>ChakoraHub Team</strong></p>'
                f'<p style="margin:0;font-size:11px;color:#9e9e9e;">This is an automated message &mdash; please do not reply.</p>'
                f'</div>'
            )

        def wrap(content):
            return (
                '<!DOCTYPE html><html><head><meta charset="UTF-8">'
                '<meta name="viewport" content="width=device-width,initial-scale=1"></head>'
                '<body style="margin:0;padding:0;background:#eeeeee;font-family:Arial,Helvetica,sans-serif;">'
                '<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#eeeeee;padding:20px 0;">'
                '<tr><td align="center">'
                '<table width="580" cellpadding="0" cellspacing="0" border="0"'
                ' style="background:#ffffff;border:1px solid #e0e0e0;max-width:580px;width:100%;">'
                f'<tr><td>{content}</td></tr></table></td></tr></table></body></html>'
            )

        # ── Student "Thank You" email ──────────────────────────────────────
        student_html = wrap(
            header_block("Thank You for Your Feedback!", "ChakoraHub \u2014 We appreciate your time", "&#127775;")
            + f'<div style="padding:24px;">'
            + f'<p style="margin:0 0 16px;font-size:14px;color:#424242;">Dear <strong>{student_name}</strong>,</p>'
            + f'<p style="margin:0 0 20px;font-size:14px;color:#424242;line-height:1.6;">'
            + f'Thank you for sharing your feedback! Your thoughts help us continuously improve '
            + f'our programs and provide a better learning experience for all students.</p>'
            + table_wrap(
                section_header("&#128203; Your Submission")
                + row("Name",     student_name)
                + row("Email",    student_email)
                + (row("Rating",  star_display(rating)) if rating else "")
                + row("Feedback", f'<em>&ldquo;{feedback_text}&rdquo;</em>', last=True)
            )
            + f'<div style="background:{TEAL_LIGHT};border-left:4px solid {TEAL};border-radius:4px;padding:14px 16px;">'
            + f'<p style="margin:0 0 8px;font-weight:700;color:{TEAL_DARK};font-size:13px;">&#128204; What&rsquo;s Next?</p>'
            + f'<ul style="margin:0;padding-left:18px;">'
            + f'<li style="margin-bottom:7px;font-size:13px;">Our team reviews every feedback personally.</li>'
            + f'<li style="margin-bottom:7px;font-size:13px;">Your input directly shapes our upcoming improvements.</li>'
            + f'<li style="font-size:13px;">Visit <a href="https://www.chakorahub.com" style="color:{TEAL};">chakorahub.com</a> for more resources.</li>'
            + f'</ul></div>'
            + footer_block()
            + '</div>'
        )

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={"ToAddresses": [student_email]},
            Message={
                "Subject": {"Data": f"Thank You for Your Feedback, {student_name}! \U0001f31f", "Charset": CHARSET},
                "Body": {
                    "Html": {"Data": student_html, "Charset": CHARSET},
                    "Text": {"Data": f"Dear {student_name},\n\nThank you for your feedback!\n\nYour message: {feedback_text}\n\nRegards,\nChakoraHub Team", "Charset": CHARSET},
                },
            },
        )
        print(f"✅ Student thank-you email sent to {student_email}")

        # ── Admin "New Feedback" notification ─────────────────────────────
        admin_html = wrap(
            header_block("New Feedback Received", "ChakoraHub \u2014 Student Feedback Notification", "&#128172;")
            + f'<div style="padding:24px;">'
            + f'<p style="margin:0 0 16px;font-size:14px;color:#424242;">A new feedback submission has been received.</p>'
            + table_wrap(
                section_header("&#128100; Student Details")
                + row("Name",  student_name)
                + row("Email", f'<a href="mailto:{student_email}" style="color:{TEAL};">{student_email}</a>')
                + row("Phone", phone if phone else "N/A", last=True)
            )
            + table_wrap(
                section_header("&#128172; Feedback Message")
                + (row("Rating", star_display(rating)) if rating else "")
                + row("Message", f'<em>&ldquo;{feedback_text}&rdquo;</em>', last=True)
            )
            + footer_block()
            + '</div>'
        )

        ses.send_email(
            Source=ADMIN_EMAIL,
            Destination={"ToAddresses": [ADMIN_EMAIL]},
            Message={
                "Subject": {"Data": f"New Feedback Received \u2014 {student_name}", "Charset": CHARSET},
                "Body": {
                    "Html": {"Data": admin_html, "Charset": CHARSET},
                    "Text": {"Data": f"New Feedback\n\nFrom: {student_name}\nEmail: {student_email}\nPhone: {phone}\nMessage: {feedback_text}\n\nChakoraHub Team", "Charset": CHARSET},
                },
            },
        )
        print(f"✅ Admin feedback notification sent to {ADMIN_EMAIL}")

    except Exception as e:
        print(f"⚠️  send_feedback_email error: {e}")
        raise


# ═════════════════════════════════════════════════════════
# SUBMIT FEEDBACK
# Cache: invalidates feedbacks:latest
#
# FIX: NRM_STUDENTS does NOT have EMAIL/PHONE/FIRST_NAME/LAST_NAME columns.
# Those live in NRM_USERS. We look up the student via NRM_USERS → NRM_STUDENTS
# using USER_ID. If the user isn't registered, we still save the feedback with
# STUDENT_ID = NULL so the submission never fails.
# ═════════════════════════════════════════════════════════
@app.post("/api/student/feedback")
async def submit_feedback(payload: FeedbackRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)

        # ── NEW: If request_id or meeting_id is provided, validate request ──────────────
        request_id = payload.request_id or payload.meeting_id
        if request_id:
            cursor.execute(
                """
                SELECT request_id, meeting_id, expiry_time, feedback_sent
                FROM NRM_FEEDBACK_REQUESTS
                WHERE request_id = %s OR meeting_id = %s
                """,
                (request_id, request_id)
            )
            req = cursor.fetchone()
            if not req:
                raise HTTPException(status_code=400, detail="Invalid feedback request ID")
            expiry = req["EXPIRY_TIME"]
            if datetime.utcnow() > expiry:
                raise HTTPException(status_code=400, detail="Feedback window expired")
            # FEEDBACK_SENT is stored as NUMBER(1) in Oracle (0 = False, 1 = True)
            sent_val = req.get("FEEDBACK_SENT")
            if sent_val == 1 or sent_val is True:
                raise HTTPException(status_code=400, detail="Feedback already submitted")

        # ── Step 1: Find student_id via NRM_USERS → NRM_STUDENTS (USER_ID) ──
        # NRM_STUDENTS only has: ID, LOCATION, REGISTRATION_SOURCE, USER_ID
        # Email and phone live in NRM_USERS.
        student_id = None
        cursor.execute(
            """
            SELECT s.ID AS STUDENT_ID
            FROM NRM_USERS u
            JOIN NRM_STUDENTS s ON s.USER_ID = u.ID
            WHERE LOWER(TRIM(u.EMAIL)) = LOWER(TRIM(%s))
               OR TRIM(u.PHONE) = TRIM(%s)
            ORDER BY s.ID DESC
            FETCH FIRST 1 ROWS ONLY
            """,
            (payload.email, payload.phone),
        )
        row = cursor.fetchone()
        if row:
            student_id = row["STUDENT_ID"]
            print(f"✅ Found student_id={student_id} for email={payload.email}")
        else:
            print(f"ℹ️  No registered student found for email={payload.email} — saving feedback with STUDENT_ID=NULL")

        # ── Step 2: Insert feedback (STUDENT_ID can be NULL for guest submitters) ──
        # Also store meeting_id and request_id if present
        cursor.execute(
            """
            INSERT INTO NRM_FEEDBACK (STUDENT_ID, NAME, EMAIL, PHONE, FEEDBACK_MESSAGE, RATING, MEETING_ID, REQUEST_ID, SUBMITTED_AT)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """,
            (student_id, payload.name, payload.email, payload.phone, payload.feedback_text, payload.rating, request_id, request_id),
        )
        conn.commit()

        # ── Step 3: If request_id exists, mark request as sent ────────────────────────────
        if request_id:
            cursor.execute(
                """
                UPDATE NRM_FEEDBACK_REQUESTS
                SET feedback_sent = 1
                WHERE request_id = %s OR meeting_id = %s
                """,
                (request_id, request_id)
            )
            conn.commit()
            print(f"✅ Marked feedback as sent for request {request_id}")

        # ── Step 4: Invalidate feedbacks cache ────────────────────────────
        
        cache_freq_delete("feedbacks:latest")
        print("🗑️  feedbacks:latest cache invalidated")

        # ── Step 5: Send confirmation emails (non-fatal) ──────────────────
        try:
            send_feedback_email(
                student_email=payload.email,
                student_name=payload.name,
                feedback_text=payload.feedback_text,
                phone=payload.phone,
                rating=payload.rating,
            )
        except Exception as email_err:
            print(f"⚠️  Feedback email failed (non-fatal): {email_err}")

        return {"success": True, "message": "Feedback submitted successfully"}

    except Exception as e:
        print(f"❌ Feedback submission error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# ENQUIRY  (rate-limited)
# ═════════════════════════════════════════════════════════
@app.post("/api/student/enquiry")
async def submit_enquiry(payload: EnquiryRequest):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO NRM_ENQUIRIES (NAME, EMAIL, PHONE, ENQUIRY_TEXT, USER_ID, SUBMITTED_AT)
            VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """, (payload.name, payload.email, payload.phone, payload.enquiry_text, payload.user_id))
        conn.commit()
        return {"success": True, "message": "Enquiry submitted successfully"}

    except Exception as e:
        print(f"❌ Enquiry error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# REGISTRATION STATUS CHECK
# Cache: registration:{user_id}  DB 3  5 min
# ═════════════════════════════════════════════════════════
@app.get("/api/student/access/registration-complete")
async def registration_complete(user_id: int):
    cache_key = f"registration:{user_id}"

    cached = cache_freq_get(cache_key)
    if cached is not None:
        print(f"🎯 Registration cache HIT: user_id={user_id}")
        return {"success": True, "registration_complete": cached}

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute("""
            SELECT COUNT(*) AS CNT
            FROM NRM_REGISTRATIONS r
            JOIN NRM_STUDENTS s ON r.STUDENT_ID = s.ID
            JOIN NRM_USERS    u ON s.USER_ID    = u.ID
            WHERE u.ID = %s
        """, (user_id,))
        row = cursor.fetchone()
        registered = bool(row and int(row.get("CNT", 0)) > 0)

        cache_freq_set(cache_key, registered, TTL_REGISTRATION)
        return {"success": True, "registration_complete": registered}

    except Exception as e:
        print(f"❌ Registration check error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# SESSION VERIFY  (used by Flask proxy to validate requests)
# Reads session:{user_id} and slides TTL if found.
# ═════════════════════════════════════════════════════════
@app.get("/api/student/session/verify")
async def verify_session(user_id: int):
    session = cache_session_get(user_id)
    if session:
        cache_session_refresh(user_id)   # slide 7-day TTL
        return {"success": True, "valid": True, "session": session}
    return {"success": True, "valid": False}


# ═════════════════════════════════════════════════════════
# AUTH CHECK  (used by Flask proxy for role-based access)
# Reads roles:{user_id} — no DB hit if cache is warm.
# ═════════════════════════════════════════════════════════
@app.get("/api/student/auth/roles")
async def get_roles(user_id: int):
    auth = cache_auth_get(user_id)
    if auth:
        print(f"🎯 Roles cache HIT: user_id={user_id}")
        return {"success": True, "found": True, **auth}

    # Cache miss — read from DB and re-warm
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute("SELECT USERTYPE FROM NRM_USERS WHERE ID = %s FETCH FIRST 1 ROWS ONLY", (user_id,))
        row = cursor.fetchone()
        if not row:
            return {"success": True, "found": False}
        usertype = (row.get("USERTYPE") or "student").lower()
        roles    = [usertype]
        try:
            cursor.execute("SELECT ROLE_NAME FROM NRM_USER_ROLES WHERE USER_ID = %s", (user_id,))
            role_rows = cursor.fetchall() or []
            if role_rows:
                roles = [r["ROLE_NAME"] for r in role_rows]
        except Exception:
            pass
        cache_auth_set(user_id, roles, usertype)
        return {"success": True, "found": True, "roles": roles, "usertype": usertype}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


@app.post("/api/student/admin/upload-file/{file_type}")
async def admin_upload_file(
    file_type: str,
    file: UploadFile = File(...),
    category: Optional[str] = Form(None),
    course_id: Optional[int] = Form(None),
):
    file_type = (file_type or "").strip().lower()
    filename = secure_filename(file.filename or "")
    if not filename:
        raise HTTPException(status_code=400, detail="No file selected")

    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    allowed = ALLOWED_EXTENSIONS.get(file_type)
    if not allowed or ext not in allowed:
        raise HTTPException(status_code=400, detail="Invalid file type or extension")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        if file_type == "syllabus":
            if not course_id:
                raise HTTPException(status_code=400, detail="course_id is required for syllabus")

            cursor.execute("SELECT id, course_name FROM nrm_courses WHERE id = %s", (course_id,))
            course = cursor.fetchone()
            if not course:
                raise HTTPException(status_code=400, detail="Invalid course selected")

            course_name = course.get("COURSE_NAME") or course.get("course_name")
            course_folder = secure_filename(str(course_name)).replace("-", "_") or f"course_{course_id}"
            stored_filename = f"{course_id}_{filename}"
            file_path = os.path.join(SYLLABUS_FOLDER, course_folder, stored_filename)
            _save_upload_file(file, file_path)

            s3_key = _upload_student_content_to_s3(file_path, "syllabus", course_folder)
            if not s3_key:
                if os.path.exists(file_path):
                    os.remove(file_path)
                raise HTTPException(status_code=502, detail="S3 upload failed for syllabus")

            public_path = f"/uploads/syllabus/{course_folder}/{stored_filename}"

            cursor.execute("SELECT id, file_path FROM nrm_syllabus WHERE course_id = %s", (course_id,))
            existing = cursor.fetchone()
            if existing:
                old_file_path = existing.get("FILE_PATH") or existing.get("file_path")
                if old_file_path and old_file_path != public_path:
                    old_relative = old_file_path.lstrip("/")
                    old_abs = os.path.join(SERVICE_DIR, old_relative)
                    if os.path.exists(old_abs):
                        os.remove(old_abs)
                cursor.execute("UPDATE nrm_syllabus SET file_path = %s WHERE course_id = %s", (public_path, course_id))
            else:
                cursor.execute("INSERT INTO nrm_syllabus (course_id, file_path) VALUES (%s, %s)", (course_id, public_path))

            conn.commit()
            return {
                "success": True,
                "message": f"Syllabus uploaded successfully for {course_name}",
                "s3_key": s3_key,
                "file_path": public_path,
            }

        category_name = (category or "").strip()
        if not category_name:
            raise HTTPException(status_code=400, detail="Technology/category is required")

        folder_name_map = {"interview": "interview_questions"}
        actual_folder = folder_name_map.get(file_type, file_type)
        upload_folder = os.path.join(UPLOAD_ROOT, actual_folder, category_name)
        file_path = os.path.join(upload_folder, filename)
        _save_upload_file(file, file_path)

        s3_key = _upload_student_content_to_s3(file_path, file_type, category_name)
        if not s3_key:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise HTTPException(status_code=502, detail=f"S3 upload failed for {file_type}")

        return {
            "success": True,
            "message": f"{file_type.upper()} uploaded successfully",
            "s3_key": s3_key,
            "file_path": file_path,
        }
    finally:
        cursor.close()
        conn.close()


@app.post("/api/student/admin/upload-practice-test")
async def admin_upload_practice_test(
    file: UploadFile = File(...),
    subject: str = Form(...),
):
    filename = secure_filename(file.filename or "")
    if not filename:
        raise HTTPException(status_code=400, detail="No file selected")

    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    if ext not in ALLOWED_EXTENSIONS["practice_test"]:
        raise HTTPException(status_code=400, detail="Invalid file type for practice test")

    subject = (subject or "").strip()
    if not subject:
        raise HTTPException(status_code=400, detail="Subject is required")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute("SELECT course_name FROM nrm_courses")
        rows = cursor.fetchall() or []
        valid_subjects = {
            (row.get("COURSE_NAME") or row.get("course_name") or "").strip()
            for row in rows
            if (row.get("COURSE_NAME") or row.get("course_name"))
        }
    finally:
        cursor.close()
        conn.close()

    if subject not in valid_subjects:
        raise HTTPException(status_code=400, detail="Invalid subject")

    upload_folder = os.path.join(PRACTICE_TESTS_FOLDER, subject)
    file_path = os.path.join(upload_folder, filename)
    _save_upload_file(file, file_path)
    s3_key = _upload_student_content_to_s3(file_path, "practice_test", subject)
    if not s3_key:
        if os.path.exists(file_path):
            os.remove(file_path)
        raise HTTPException(status_code=502, detail="S3 upload failed for practice test")

    return {
        "success": True,
        "message": "Practice test uploaded successfully",
        "s3_key": s3_key,
        "file_path": file_path,
    }

# For meeting Intelligence

MEETING_BOOKINGS_TABLE = os.getenv("MEETING_BOOKINGS_TABLE", os.getenv("BOOKINGS_TABLE", "Bookings"))
PRICING_HISTORY_TABLE = os.getenv("PRICING_HISTORY_TABLE", "CHAKORAHUB_PRICING_HISTORY")
_meeting_dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
_meeting_bookings_table = _meeting_dynamodb.Table(MEETING_BOOKINGS_TABLE)
_pricing_dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
_pricing_history_table = _pricing_dynamodb.Table(PRICING_HISTORY_TABLE)


def query_dynamo_bookings(query: str) -> List[Dict[str, Any]]:
    """Fetch recent bookings for a user identity from DynamoDB."""
    lookup = (query or "").strip()
    if not lookup:
        return []

    is_email = "@" in lookup
    filter_expr = Attr("student_email").eq(lookup) if is_email else (
        Attr("created_by").eq(lookup) | Attr("studentId").eq(lookup)
    )

    try:
        scan_kwargs = {"FilterExpression": filter_expr}
        response = _meeting_bookings_table.scan(**scan_kwargs)
        items = response.get("Items", [])

        while "LastEvaluatedKey" in response:
            scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]
            response = _meeting_bookings_table.scan(**scan_kwargs)
            items.extend(response.get("Items", []))

        active = [
            item for item in items
            if (item.get("status") or "").upper() not in {"CANCELLED", "REJECTED"}
        ]
        active.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return active[:50]
    except Exception as e:
        print(f"⚠️ DynamoDB query failed for student-lookup ({lookup}): {e}")
        return []

@app.get("/student-lookup")
async def student_lookup(
    query: str = Query(...),
    type: str = Query(...)
):
    """
    Student identity lookup
    Flow:
    Direct DynamoDB lookup
    """
    try:
        normalized_query = query.strip().lower()
        query_type = type.strip().lower()
        if query_type not in ["email", "phone"]:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "exists": False,
                    "message": "Invalid query type"
                }
            )

        # Cache is disabled; always query DB.
        bookings = query_dynamo_bookings(normalized_query)

        # STEP 3 — NEW USER
        if not bookings:
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "exists": False,
                    "cache_hit": False,
                    "source": "dynamodb",
                    "query_type": query_type,
                    "total_bookings": 0,
                    "bookings": [],
                    "ml_suggestion": None
                }
            )

        # Existing user response
        latest_booking = bookings[0] if bookings else {}

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "exists": True,
                "cache_hit": False,
                "source": "dynamodb",
                "query_type": query_type,
                "student_email": latest_booking.get("student_email"),
                "student_phone": latest_booking.get("student_phone"),
                "student_name": latest_booking.get("student_name"),
                "total_bookings": len(bookings),
                "bookings": bookings,
                "ml_suggestion": generate_ml_suggestion(bookings)
            }
        )

    except Exception as db_error:
        print(f"❌ student_lookup failed: {db_error}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "exists": False,
                "message": str(db_error)
            }
        )


# ═════════════════════════════════════════════════════════
# NEW ENDPOINTS FOR FEEDBACK GENERATION (triggered by meeting_service)
# ═════════════════════════════════════════════════════════

@app.post("/api/student/feedback/generate")
async def generate_feedback_link(payload: FeedbackGenerateRequest):
    """
    Generate feedback links for one or more student activities.
    - Stores expiry and request_id in Oracle (NRM_FEEDBACK_REQUESTS)
    - Sends email to student with the links
    """
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = None
    try:
        cursor = conn.cursor(DictCursor)
        expiry_time = datetime.utcnow() + timedelta(hours=payload.expiry_hours)
        student_email = payload.student_email
        student_name = payload.student_name

        generated_links = []
        for act in payload.activities:
            import uuid
            # Generate a secure request token / request_id
            request_id = f"REQ-{uuid.uuid4().hex[:12].upper()}"
            feedback_url = f"{FEEDBACK_BASE_URL}?request_id={request_id}"

            # Insert into NRM_FEEDBACK_REQUESTS
            # MEETING_ID acts as the PRIMARY KEY legacy mapping, REQUEST_ID holds the new dedicated request token
            cursor.execute(
                """
                INSERT INTO NRM_FEEDBACK_REQUESTS
                (MEETING_ID, REQUEST_ID, STUDENT_EMAIL, STUDENT_NAME, EXPIRY_TIME, CREATED_AT, FEEDBACK_SENT, FEEDBACK_URL, ACTIVITY_TYPE, ACTIVITY_ID)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 0, %s, %s, %s)
                """,
                (request_id, request_id, student_email, student_name, expiry_time, feedback_url, act.activity_type, act.activity_id)
            )

            generated_links.append({
                "activity_type": act.activity_type,
                "activity_id": act.activity_id,
                "label": act.label,
                "request_id": request_id,
                "feedback_url": feedback_url
            })

            # Send email
            try:
                send_feedback_link_email(
                    to_email=student_email,
                    student_name=student_name,
                    meeting_id=request_id,
                    feedback_url=feedback_url,
                    expiry=expiry_time
                )
            except Exception as e:
                print(f"[WARNING] Email send failed for request {request_id}: {e}")

        # Invalidate student activities cache
        try:
            cursor.execute(
                """
                SELECT r.REGISTRATION_ID 
                FROM NRM_REGISTRATIONS r 
                JOIN NRM_STUDENTS s ON r.STUDENT_ID = s.ID 
                JOIN NRM_USERS u ON s.USER_ID = u.ID 
                WHERE LOWER(TRIM(u.EMAIL)) = LOWER(TRIM(%s))
                """, 
                (student_email,)
            )
            reg_row = cursor.fetchone()
            if reg_row:
                reg_id = reg_row["REGISTRATION_ID"]
                cache_key = f"chakorahub:student:activities:{reg_id}"
                print(f"[cache] Evicted activities cache for key: {cache_key} (feedback generated)")
        except Exception as e:
            print(f"[WARNING] Failed to evict activities cache: {e}")

        conn.commit()
        return {
            "success": True,
            "message": f"Successfully generated {len(generated_links)} feedback request(s)",
            "requests": generated_links
        }

    except Exception as e:
        import traceback
        print("Feedback generation failed")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.get("/api/student/feedback/form", response_class=HTMLResponse)
async def feedback_form(request: Request, request_id: Optional[str] = None, meeting_id: Optional[str] = None):
    """
    Renders the feedback form for a given request_id or meeting_id.
    Checks expiry and duplicate submission.
    """
    lookup_id = request_id or meeting_id
    if not lookup_id:
        raise HTTPException(status_code=400, detail="Missing request_id or meeting_id")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(
            """
            SELECT request_id, meeting_id, student_email, student_name, expiry_time, feedback_sent
            FROM NRM_FEEDBACK_REQUESTS
            WHERE request_id = %s OR meeting_id = %s
            """,
            (lookup_id, lookup_id)
        )
        record = cursor.fetchone()
        if not record:
            # Return simple error page
            return HTMLResponse(content="""
            <html><body><h2>Invalid feedback link</h2><p>No feedback request found.</p></body></html>
            """, status_code=404)

        # Check expiry
        expiry = record["EXPIRY_TIME"]
        if datetime.utcnow() > expiry:
            return HTMLResponse(content="""
            <html><body><h2>Feedback link expired</h2><p>This feedback link has expired.</p></body></html>
            """, status_code=400)

        # Check if already submitted (0 = False, 1 = True in Oracle)
        sent_val = record.get("FEEDBACK_SENT")
        if sent_val == 1 or sent_val is True:
            return HTMLResponse(content="""
            <html><body><h2>Feedback already submitted</h2><p>Thank you, your feedback has been recorded.</p></body></html>
            """, status_code=400)

        # Valid: render form using the embedded HTML template
        from jinja2 import Template
        template = Template(FEEDBACK_FORM_HTML)
        html = template.render(
            meeting_id=record["REQUEST_ID"] or record["MEETING_ID"] or lookup_id,
            student_name=record["STUDENT_NAME"],
            student_email=record["STUDENT_EMAIL"],
            student_phone=""
        )
        return HTMLResponse(content=html)

    except Exception as e:
        print(f"❌ Feedback form error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()


# ═════════════════════════════════════════════════════════
# SQL DDL for new tables (to be run manually)
# ═════════════════════════════════════════════════════════
# CREATE TABLE IF NOT EXISTS NRM_FEEDBACK_REQUESTS (
#     meeting_id VARCHAR(255) PRIMARY KEY,
#     student_email VARCHAR(255),
#     student_name VARCHAR(255),
#     expiry_time TIMESTAMP,
#     feedback_sent BOOLEAN DEFAULT FALSE,
#     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
# );
# 
# ALTER TABLE NRM_FEEDBACK ADD COLUMN MEETING_ID VARCHAR(255);
# ═════════════════════════════════════════════════════════


def generate_ml_suggestion(bookings):
    """
    Generate ML-based suggestions from booking history
    Returns: { complexity, duration, preferred_time }
    """
    if not bookings:
        return None
    
    # Simple ML logic (you can enhance this)
    complexities = [b.get('complexity', 'Medium') for b in bookings]
    durations = [b.get('duration_minutes', 30) for b in bookings]
    
    # Most common complexity
    most_common_complexity = max(set(complexities), key=complexities.count)
    
    # Average duration
    avg_duration = int(sum(durations) // len(durations)) if durations else 30
    
    return {
        "complexity": most_common_complexity,
        "duration": avg_duration,
        "total_previous_bookings": len(bookings)
    }

# ═════════════════════════════════════════════════════════
# KAFKA CONSUMER — order.confirmed
# Triggered by billing_service after Razorpay payment succeeds.
# Auto-creates REGISTRATION_IDs so there is zero manual gap
# between payment completion and enrollment.
# ═════════════════════════════════════════════════════════
import json as _json_mod

def ensure_shop_student(
    cursor,first_name,last_name,email,phone,
    location="Online"
):
    """
    Creates NRM_USERS, NRM_STUDENTS and NRM_LOGINS
    for Hub Store customers.
    If the user already exists, only returns USER_ID.
    """
    # -------------------------------------------------------
    # Check whether user already exists
    # -------------------------------------------------------
    cursor.execute("""
        SELECT ID
        FROM NRM_USERS
        WHERE UPPER(EMAIL)=UPPER(%s)
        ORDER BY ID DESC
        FETCH FIRST 1 ROWS ONLY
    """, (email,))
    row = cursor.fetchone()
    if row:
        return int(row["ID"])
    full_name = f"{first_name} {last_name}".strip()
    # -------------------------------------------------------
    # Create User
    # -------------------------------------------------------
    cursor.execute("""
        INSERT INTO NRM_USERS
        (USERNAME, EMAIL, PHONE, PROFILE_PIC, USERTYPE, REGISTRATION_SOURCE)
        VALUES
        (%s,%s,%s,%s,%s,%s)
    """,
    (full_name,email,phone,"default.jpg","student_ht","hubstore"))

    cursor.execute("""
        SELECT ID FROM NRM_USERS
        WHERE UPPER(EMAIL)=UPPER(%s)
        ORDER BY ID DESC
        FETCH FIRST 1 ROWS ONLY
    """, (email,))
    user_id = int(cursor.fetchone()["ID"])
    # -------------------------------------------------------
    # Create Student
    # -------------------------------------------------------
    cursor.execute("""
        INSERT INTO NRM_STUDENTS
        (LOCATION, REGISTRATION_SOURCE, USER_ID) VALUES
        (%s,%s,%s)""",
    (location,"hubstore",user_id))
    # -------------------------------------------------------
    # Create Login
    # -------------------------------------------------------
    hashed_pwd = generate_password_hash("changeme123")
    cursor.execute("""
        INSERT INTO NRM_LOGINS
        (USER_ID,PASSWORD,IS_ACTIVE) VALUES (%s,%s,'Y') """,
    (user_id,hashed_pwd))
    print(f"✅ Hub Store user created : {email}")
    return user_id

def _handle_order_confirmed(payload: dict) -> None:
    """
    On receipt of order.confirmed:
      1. Look up each COURSE_ID from ORDER_ITEMS.
      2. Build a REGISTRATION_ID for each course (fast, atomic).
      3. Insert into NRM_REGISTRATIONS with STATUS = ACTIVE.
      4. Bust the user's dashboard + registration caches.
    """
    order_id = payload.get("order_id")
    user_id  = payload.get("user_id")
    print(f"📩 order.confirmed | order_id={order_id} user_id={user_id}")

    conn = get_db_connection()
    if not conn:
        print(f"⚠️  order.confirmed: DB unavailable, skipping order_id={order_id}")
        return

    cursor = conn.cursor(DictCursor)
    try:
        # 1. Fetch order items with course + user details
        cursor.execute("""
        SELECT oi.COURSE_ID, oi.QUANTITY, c.COURSE_CODE, c.COURSE_NAME, o.USER_ID, o.BILLING_FULL_NAME, o.BILLING_EMAIL, o.BILLING_MOBILE
        FROM ORDER_ITEMS oi
        JOIN NRM_COURSES c
            ON c.ID = oi.COURSE_ID
        JOIN ORDERS o
            ON o.ORDER_ID = oi.ORDER_ID
        WHERE oi.ORDER_ID=%s
        """,(order_id,))
        items = cursor.fetchall()
        first_item = items[0]
        email = first_item["BILLING_EMAIL"]
        phone = first_item["BILLING_MOBILE"]
        customer_name = first_item["BILLING_FULL_NAME"].strip()
        parts = customer_name.split()
        if len(parts) == 1:
            first_name = parts[0]
            last_name = ""
        else:
            first_name = parts[0]
            last_name = " ".join(parts[1:])
        user_id = ensure_shop_student(cursor,first_name,last_name,email,phone)
        if not items:
            print(f"⚠️  order.confirmed: no items for order_id={order_id}")
            return


        # 2. Ensure student record exists
        cursor.execute(
            "SELECT ID FROM NRM_STUDENTS WHERE USER_ID=%s ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY",
            (user_id,)
        )
        student_row = cursor.fetchone()
        if student_row:
            student_id = int(student_row["ID"])
        else:
            cursor.execute(
                "INSERT INTO NRM_STUDENTS (LOCATION, REGISTRATION_SOURCE, USER_ID) VALUES (%s,%s,%s)",
                ("online", "shop_checkout", user_id)
            )
            cursor.execute(
                "SELECT ID FROM NRM_STUDENTS WHERE USER_ID=%s ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY",
                (user_id,)
            )
            student_id = int(cursor.fetchone()["ID"])

        # 3. Resolve ACTIVE status_id
        cursor.execute("SELECT ID FROM NRM_STATUSES WHERE UPPER(STATUS)='ACTIVE' FETCH FIRST 1 ROWS ONLY")
        status_row = cursor.fetchone()
        if not status_row:
            print("❌ order.confirmed: ACTIVE status not found in NRM_STATUSES")
            return
        active_id = status_row["ID"]

        # 4. Default language (Telugu)
        cursor.execute("SELECT ID FROM NRM_LANGUAGES WHERE LOWER(LANGUAGE)='telugu' FETCH FIRST 1 ROWS ONLY")
        lang_row = cursor.fetchone()
        language_id = lang_row["ID"] if lang_row else 1

        start_date = datetime.now()

        # 5. Register each course
        for item in items:
            course_id   = item["COURSE_ID"]
            course_code = item["COURSE_CODE"]
            username    = (item.get("USERNAME") or "").strip()

            # Skip if already registered
            cursor.execute("""
                SELECT REGISTRATION_ID FROM NRM_REGISTRATIONS
                WHERE STUDENT_ID=%s AND COURSE_ID=%s FETCH FIRST 1 ROWS ONLY
            """, (student_id, course_id))
            if cursor.fetchone():
                print(f"ℹ️  Already registered: student_id={student_id} course_id={course_id}")
                continue

            first = (username.split()[0] if " " in username else username)[:1].upper() or "X"
            last  = (username.split()[-1] if " " in username else "X")[:1].upper() or "X"

            reg_id = _build_student_registration_id(
                cursor, course_id, course_code, first, last, start_date
            )

            cursor.execute("""
                INSERT INTO NRM_REGISTRATIONS
                (REGISTRATION_ID, STUDENT_ID, COURSE_ID, LANGUAGE_ID,
                 STATUS_ID, CREATED_DT)
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (reg_id, student_id, course_id, language_id, active_id))

            print(f"✅ Auto-registered: reg_id={reg_id} order_id={order_id} course_id={course_id}")

        conn.commit()

        # 6. Bust caches
        cache_freq_delete(f"registration:{user_id}")
        cache_dashboard_delete(user_id)

    except Exception as e:
        conn.rollback()
        print(f"❌ order.confirmed handler error: {e}")
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()


def _run_order_confirmed_consumer():
    """
    Consumes order.confirmed events to auto-generate enrolments.

    Retry policy: exponential backoff (5s -> 60s cap), retries forever.
    Never permanently gives up — a Kafka blip at boot (or a bad
    KAFKA_BOOTSTRAP_SERVERS in .env) no longer disables auto-enrollment
    for the rest of the process's life; it just keeps retrying quietly
    in the background until Kafka becomes reachable.
    """
    import time

    backoff = 5
    MAX_BACKOFF = 60

    while True:                                    # outer: reconnect loop
        consumer = None
        try:
            consumer = KafkaConsumer(
                "order.confirmed",
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                group_id="student-service-enrollment",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: _json_mod.loads(v.decode("utf-8")),
                session_timeout_ms=30000,
                request_timeout_ms=40000,
            )
            print(f"✅ student_service order.confirmed consumer connected to {KAFKA_BOOTSTRAP_SERVERS}")
            backoff = 5                             # reset after a real connect

            while True:                             # inner: poll loop
                try:
                    records = consumer.poll(timeout_ms=1000)
                    for tp, messages in records.items():
                        for msg in messages:
                            try:
                                _handle_order_confirmed(msg.value)
                                consumer.commit()
                            except Exception as e:
                                print(f"⚠️  order.confirmed message error: {e}")
                except Exception as e:
                    print(f"⚠️  order.confirmed consumer poll error: {e}")
                    break                            # drop to outer loop to reconnect

        except Exception as e:
            print(
                f"⚠️  order.confirmed consumer cannot connect to "
                f"{KAFKA_BOOTSTRAP_SERVERS}: {e}. Retrying in {backoff}s…"
            )
        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception:
                    pass

        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)


_enrollment_consumer_thread = Thread(target=_run_order_confirmed_consumer, daemon=True)
_enrollment_consumer_thread.start()
# ── Start Kafka consumer thread on service boot ──────────────
_consumer_thread = Thread(target=_run_lookup_consumer, daemon=True)
_consumer_thread.start()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)


# ==========================================
# ORG DOCUMENTS → S3 (org-complaince-docs)
# ==========================================
ORG_S3_BUCKET = "org-complaince-docs"
ORG_S3_REGION = "eu-north-1"
ORG_S3_BASE_URL = f"https://{ORG_S3_BUCKET}.s3.{ORG_S3_REGION}.amazonaws.com"

ORG_DOC_FOLDERS = {
    "hr":         "hr-policy/",
    "legal":      "legal-compliance/",
    "finance":    "finance-accounts/",
    "operations": "internal-operations/",
}

@app.post("/api/admin/upload-org-doc/{doc_type}")
async def upload_org_doc(
    doc_type: str,
    file: UploadFile = File(...),
    org_doc_category: str = Form(...),
    uploaded_by: Optional[str] = Form(None),
):
    if doc_type not in ORG_DOC_FOLDERS:
        raise HTTPException(status_code=400, detail=f"Invalid doc_type '{doc_type}'")

    file_bytes = await file.read()
    size_mb = len(file_bytes) / (1024 * 1024)
    if size_mb > 20:
        raise HTTPException(status_code=413, detail=f"File too large ({size_mb:.1f} MB). Max 20 MB.")

    safe_category = org_doc_category.replace(" ", "_").replace("/", "-")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    original_name = file.filename or "document"
    folder_prefix = ORG_DOC_FOLDERS[doc_type]
    s3_key = f"{folder_prefix}{safe_category}/{timestamp}_{original_name}"

    region = os.getenv("AWS_REGION", "eu-north-1")
    access_key = (os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY") or "").strip()
    secret_key = (os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY") or "").strip()

    try:
        s3 = boto3.client("s3", region_name=region,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)
        s3.put_object(
            Bucket=ORG_S3_BUCKET,
            Key=s3_key,
            Body=file_bytes,
            ContentType=file.content_type or "application/octet-stream",
        )
    except Exception as e:
        print(f"❌ Org doc S3 upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {str(e)}")

    file_url = f"{ORG_S3_BASE_URL}/{s3_key}"
    print(f"✅ Org doc uploaded: {file_url}")
    return {
        "success": True,
        "message": "Document uploaded successfully",
        "s3_key": s3_key,
        "file_url": file_url,
        "size_mb": round(size_mb, 2),
    }

# ==========================================
# ORG DOCUMENTS → S3 (org-complaince-docs)
# ==========================================
ORG_S3_BUCKET = "org-complaince-docs"
ORG_S3_REGION = "eu-north-1"
ORG_S3_BASE_URL = f"https://{ORG_S3_BUCKET}.s3.{ORG_S3_REGION}.amazonaws.com"

ORG_DOC_FOLDERS = {
    "hr":         "hr-policy/",
    "legal":      "legal-compliance/",
    "finance":    "finance-accounts/",
    "operations": "internal-operations/",
}

@app.post("/api/admin/upload-org-doc/{doc_type}")
async def upload_org_doc(
    doc_type: str,
    file: UploadFile = File(...),
    org_doc_category: str = Form(...),
    uploaded_by: Optional[str] = Form(None),
):
    if doc_type not in ORG_DOC_FOLDERS:
        raise HTTPException(status_code=400, detail=f"Invalid doc_type '{doc_type}'")

    file_bytes = await file.read()
    size_mb = len(file_bytes) / (1024 * 1024)
    if size_mb > 20:
        raise HTTPException(status_code=413, detail=f"File too large ({size_mb:.1f} MB). Max 20 MB.")

    safe_category = org_doc_category.replace(" ", "_").replace("/", "-")
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    original_name = file.filename or "document"
    folder_prefix = ORG_DOC_FOLDERS[doc_type]
    s3_key = f"{folder_prefix}{safe_category}/{timestamp}_{original_name}"

    region = os.getenv("AWS_REGION", "eu-north-1")
    access_key = (os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("AWS_ACCESS_KEY") or "").strip()
    secret_key = (os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("AWS_SECRET_KEY") or "").strip()

    try:
        s3 = boto3.client("s3", region_name=region,
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)
        s3.put_object(
            Bucket=ORG_S3_BUCKET,
            Key=s3_key,
            Body=file_bytes,
            ContentType=file.content_type or "application/octet-stream",
        )
    except Exception as e:
        print(f"❌ Org doc S3 upload failed: {e}")
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {str(e)}")

    file_url = f"{ORG_S3_BASE_URL}/{s3_key}"
    print(f"✅ Org doc uploaded: {file_url}")
    return {
        "success": True,
        "message": "Document uploaded successfully",
        "s3_key": s3_key,
        "file_url": file_url,
        "size_mb": round(size_mb, 2),
    }