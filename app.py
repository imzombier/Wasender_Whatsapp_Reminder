from flask import Flask, render_template, request, redirect, url_for, Response, session
import re, pandas as pd, requests, os, time, threading, random
from io import BytesIO
from datetime import datetime, timezone, timedelta
from functools import wraps

# ---------------- CONFIG ----------------
WASENDER_URL = os.getenv("WASENDER_URL", "https://wasenderapi.com/api/send-message")
API_KEY = os.getenv("WASENDER_API_KEY", "")
PAYMENT_LINK = os.getenv("PAYMENT_LINK", "https://websitepayments.veritasfin.in")

# Your personal WhatsApp number for notifications
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "+918096091809")

# Login credentials from environment
LOGIN_USER = os.getenv("APP_USERNAME", "")
LOGIN_PASS = os.getenv("APP_PASSWORD", "")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "supersecretkey")

logs = []

# Global state
stop_sending = False
task_running = False

# ---------------- TIMEZONE ----------------
IST = timezone(timedelta(hours=5, minutes=30))

def now_ist():
    """Return current IST time string"""
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

# ---------------- AUTH DECORATOR ----------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function

# ---------------- LOGIN ROUTES ----------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        if username == LOGIN_USER and password == LOGIN_PASS:
            session["user"] = username
            return redirect(url_for("index"))
        else:
            return render_template("login.html", error="Invalid username or password")

    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ----------- Helper Functions ------------
def normalize_columns(cols):
    normalized = []
    for c in cols:
        c = str(c).strip().upper()
        c = re.sub(r'[^A-Z0-9 /]', '', c)
        normalized.append(c)
    return normalized

def get_value(row, possible_names):
    for name in possible_names:
        if name.upper() in row.index:
            return row[name.upper()]
    return None

def build_msg_dynamic(row, name, loan_no, advance, edi, overdue, payable):
    """Build Telugu WhatsApp message based on BUCKET AGING ranges"""
    try:
        days_pending = int(float(get_value(row, ["BUCKET AGING", "BUCKETAGING", "DAYS PENDING", "DPDS"]) or 0))
    except:
        days_pending = 0

    if days_pending == 0:
        template = (
            "👋 ప్రియమైన {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "💰 EMI బకాయి: ₹{payable}\n\n"
            "⚠️ ఈరోజే చెల్లించండి, లేట్ ఫైన్ & CIBIL స్కోర్ ప్రభావం నివారించండి.\n\n"
            "💳 చెల్లించండి: {paylink}"
        )
    elif 1 <= days_pending <= 13:
        template = (
            "👋 ప్రియమైన {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "💰 EMI OVERDUE: ₹{payable}\n"
            "⏳ {days} రోజులుగా పెండింగ్‌లో ఉంది.\n\n"
            "⚠️ దయచేసి వెంటనే చెల్లించండి.\n\n"
            "💳 చెల్లించండి: {paylink}"
        )
    elif 14 <= days_pending <= 30:
        template = (
            "⚠️ హెచ్చరిక {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "⏳ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
            "💸 మొత్తం OVERDUE: ₹{payable}\n\n"
            "⚠️ వెంటనే చెల్లించండి.\n\n"
            "💳 చెల్లించండి: {paylink}"
        )
    elif 31 <= days_pending <= 60:
        template = (
            "🚨 ACTION REQUIRED - {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "❌ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
            "💸 మొత్తం OVERDUE: ₹{payable}\n\n"
            "⚠️ వెంటనే చెల్లించకపోతే లీగల్ యాక్షన్ వస్తుంది.\n\n"
            "💳 తక్షణం చెల్లించండి: {paylink}"
        )
    elif 61 <= days_pending <= 90:
        template = (
            "🛑 LEGAL WARNING – {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "❌ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
            "💸 మొత్తం OVERDUE: ₹{payable}\n\n"
            "⚠️ తక్షణం చెల్లించకపోతే లీగల్ యాక్షన్ వస్తుంది.\n\n"
            "💳 వెంటనే చెల్లించండి: {paylink}"
        )
    else:
        template = (
            "⚖️ LEGAL NOTICE – {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "❌ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
            "💸 మొత్తం OVERDUE: ₹{payable}\n\n"
            "⚖️ కోర్టు లీగల్ ప్రాసెస్ ప్రారంభమవుతుంది.\n\n"
            "💳 వెంటనే చెల్లించండి: {paylink}"
        )

    return template.format(
        name=name, loan_no=loan_no, advance=advance, edi=edi,
        overdue=overdue, payable=payable, days=int(days_pending), paylink=PAYMENT_LINK
    )

def send_whatsapp(mobile, message):
    mobile_str = str(mobile).strip()
    if not mobile_str.startswith("+"):
        mobile_str = f"+91{mobile_str}"

    payload = {"to": mobile_str, "text": message}
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    try:
        res = requests.post(WASENDER_URL, json=payload, headers=headers)
        return res.status_code == 200
    except Exception as e:
        print("Error:", e)
        return False

def notify_admin(message):
    if ADMIN_WHATSAPP:
        send_whatsapp(ADMIN_WHATSAPP, message)

# ----------- Background sending function ------------
def process_messages(file, skip_loans_input, sleep_min, sleep_max):
    global logs, stop_sending, task_running
    df = pd.read_excel(file)
    df.columns = normalize_columns(df.columns)
    skip_loans = [ln.strip().upper() for ln in re.split(r'[,\s]+', skip_loans_input) if ln.strip()]

    total = len(df)
    sent_count = 0

    milestone_percents = [20, 40, 60, 80, 100]
    next_milestone_idx = 0

    notify_admin(f"🚀 Message sending started.\nTotal records: {total}")

    for idx, row in df.iterrows():
        if stop_sending:
            logs.append(f"[{now_ist()}] ⏹ Sending stopped by user.")
            notify_admin("🛑 Sending stopped manually.")
            break

        name = get_value(row, ["CUSTOMER NAME", "CUSTOMERNAME", "NAME"])
        loan_no = str(get_value(row, ["LOAN A/C NO", "LOANA/CNO", "LOAN AC NO", "LOAN NO"]) or "").upper()
        mobile_raw = get_value(row, ["MOBILE NO", "MOBILENO", "PHONE", "MOBILENUMBER"])

        if pd.notna(mobile_raw):
            mobile = str(int(mobile_raw)) if isinstance(mobile_raw, float) else str(mobile_raw).strip()
        else:
            mobile = ""

        edi = float(get_value(row, ["EDI AMOUNT", "EDIAMOUNT", "EDI"]) or 0)
        overdue = float(get_value(row, ["OVER DUE", "OVERDUE"]) or 0)
        advance = float(get_value(row, ["ADVANCE", "ADV"]) or 0)
        payable = (edi + overdue) - advance

        if not name or not mobile:
            logs.append(f"[{now_ist()}] ⚠️ Skipped row – Missing Name or Mobile")
            continue

        if loan_no in skip_loans:
            logs.append(f"[{now_ist()}] ⏩ Skipped {name} ({mobile}) – Loan {loan_no} in skip list")
            continue

        if payable <= 0:
            logs.append(f"[{now_ist()}] ⏩ Skipped {name} ({mobile}) – No pending amount")
            continue

        message = build_msg_dynamic(row, name, loan_no, advance, edi, overdue, payable)
        success = send_whatsapp(mobile, message)
        sent_count += 1

        logs.append(f"[{now_ist()}] ✅ Sent to {name} ({mobile})" if success else f"[{now_ist()}] ❌ Failed {name} ({mobile})")
        logs.append(f"[{now_ist()}] 📊 Progress: {sent_count} / {total}")

        progress_percent = int((sent_count / total) * 100)
        if next_milestone_idx < len(milestone_percents) and progress_percent >= milestone_percents[next_milestone_idx]:
            percent = milestone_percents[next_milestone_idx]
            notify_admin(f"📊 Progress: {percent}% ({sent_count}/{total} sent)")
            logs.append(f"[{now_ist()}] 📢 Milestone reached: {percent}% ({sent_count}/{total})")
            next_milestone_idx += 1

        wait_time = random.randint(sleep_min, sleep_max)
        logs.append(f"[{now_ist()}] ⏳ Waiting {wait_time} seconds before next message...")
        time.sleep(wait_time)

    if not stop_sending:
        logs.append(f"[{now_ist()}] 🎉 Completed sending all messages")
        notify_admin(f"✅ Completed. Sent {sent_count}/{total} messages.")

    task_running = False
    stop_sending = False

# ----------------- ROUTES -----------------
@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    global logs, stop_sending, task_running

    if "user" not in session:
        return redirect(url_for("login"))

    if request.method == "POST":
        if task_running:
            logs.append(f"[{now_ist()}] ⚠️ A sending task is already running.")
            return render_template("index.html", live=True, logs=logs)

        file = request.files.get("file")
        skip_loans_input = request.form.get("skip_loans", "").strip()
        sleep_min = int(request.form.get("sleep_min", "61"))
        sleep_max = int(request.form.get("sleep_max", "180"))

        if not file:
            return redirect(url_for("index"))

        logs = []
        stop_sending = False
        task_running = True
        file_bytes = BytesIO(file.read())

        thread = threading.Thread(
            target=process_messages,
            args=(file_bytes, skip_loans_input, sleep_min, sleep_max)
        )
        thread.start()

        return render_template("index.html",
                               skip_loans=skip_loans_input,
                               sleep_min=sleep_min, sleep_max=sleep_max,
                               live=True, logs=logs)

    return render_template("index.html",
                           skip_loans="", sleep_min=61, sleep_max=180,
                           live=task_running, logs=logs)

@app.route("/stop")
@login_required
def stop():
    global stop_sending
    stop_sending = True
    logs.append(f"[{now_ist()}] 🛑 Stop request received.")
    notify_admin("🛑 Script stopped by user.")
    return redirect(url_for("index"))

@app.route("/stream_logs")
@login_required
def stream_logs():
    def generate():
        last_index = 0
        while True:
            global logs
            if last_index < len(logs):
                for i in range(last_index, len(logs)):
                    yield f"data: {logs[i]}\n\n"
                last_index = len(logs)
            time.sleep(1)

    return Response(generate(), mimetype="text/event-stream")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
