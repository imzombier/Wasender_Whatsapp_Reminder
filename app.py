from flask import Flask, render_template, request, redirect, url_for, Response, session, send_file
import re, pandas as pd, requests, os, time, threading, random, json
from io import BytesIO
from datetime import datetime, timezone, timedelta
from functools import wraps

# ---------------- CONFIG ----------------
WASENDER_URL = os.getenv("WASENDER_URL", "https://wasenderapi.com/api/send-message")
API_KEY = os.getenv("WASENDER_API_KEY", "")
PAYMENT_LINK = os.getenv("PAYMENT_LINK", "https://websitepayments.veritasfin.in")
ADMIN_WHATSAPP = os.getenv("ADMIN_WHATSAPP", "+918096091809")

# Login credentials
LOGIN_USER = os.getenv("APP_USERNAME", "")
LOGIN_PASS = os.getenv("APP_PASSWORD", "")

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "supersecretkey")
app.permanent_session_lifetime = timedelta(minutes=30)

# ---------------- STATE ----------------
logs = []
stop_sending = False
task_running = False
sse_logs = []
report_rows = []
success_count = 0
skipped_count = 0
failed_count = 0
current_total = 0

# ---------------- TIMEZONE ----------------
IST = timezone(timedelta(hours=5, minutes=30))
def now_ist():
    return datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

# ---------------- EVENTS ----------------
def add_event(status, message, mobile="", bucket="", progress="", wait=""):
    global sse_logs, report_rows, success_count, skipped_count, failed_count
    event = {
        "time": now_ist(),
        "status": status,
        "message": message,
        "mobile": mobile,
        "bucket": bucket,
        "progress": progress,
        "wait": wait
    }
    sse_logs.append(json.dumps(event, ensure_ascii=False))
    report_rows.append(event.copy())

    if status.lower() == "success":
        success_count += 1
    elif status.lower() == "skipped":
        skipped_count += 1
    elif status.lower() == "failed":
        failed_count += 1

# ---------------- AUTH ----------------
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function

# ---------------- LOGIN ----------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        if username == LOGIN_USER and password == LOGIN_PASS:
            session["user"] = username
            session.permanent = True
            return redirect(url_for("index"))
        else:
            return render_template("login.html", error="Invalid username or password")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ---------------- SKIP LOANS ----------------
SKIP_FILE = "skip_loans.txt"
def load_skip_loans():
    if not os.path.exists(SKIP_FILE):
        return []
    with open(SKIP_FILE, "r", encoding="utf-8") as f:
        parts = re.split(r'[\r\n,]+', f.read().strip())
        return [ln.strip().upper() for ln in parts if ln.strip()]

def save_skip_loans(skip_loans_input):
    with open(SKIP_FILE, "w", encoding="utf-8") as f:
        f.write(skip_loans_input.strip())

# ---------------- HELPERS ----------------
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

def parse_bucket_value(raw_bucket):
    try:
        if pd.notna(raw_bucket) and str(raw_bucket).strip() != "":
            return int(float(str(raw_bucket).strip()))
    except Exception:
        pass
    return 0

def get_telugu_weekday():
    wk = datetime.now(IST).weekday()
    mapping = {
        0: "సోమవారం", 1: "మంగళవారం", 2: "బుధవారం",
        3: "గురువారం", 4: "శుక్రవారం", 5: "శనివారం", 6: "ఆదివారం"
    }
    return mapping.get(wk, "ఈ రోజు")

def build_msg_dynamic(row, name, loan_no, advance, edi, overdue, payable, method, emi_day="ఈ రోజు"):
    bucket_aging = parse_bucket_value(get_value(row, ["BUCKET AGING", "BUCKETAGING", "DAYS PENDING", "DPDS"]))
    # ---------------- METHOD 1 (Overdue) ----------------
    if method == "method1":
        # do not send if bucket aging is zero
        if bucket_aging == 0:
            return None

        if 1 <= bucket_aging <= 13:
            template = (
                "👋 ప్రియమైన {name} గారు,\n\n"
                "📌 లోన్ నంబర్: {loan_no}\n\n"
                "⏳ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
                "💰 TODAY EMI : ₹{edi}\n"
                "❌ OVERDUE AMOUNT : ₹{overdue}\n\n"
                "⚠️ ఈరోజే ₹{payable} చెల్లించండి, లేకపోతే అదనపు లేట్ ఫైన్ & CIBIL రిపోర్ట్‌లో నెగటివ్ ప్రభావం పడుతుంది.\n\n"
                "💳 చెల్లించండి: {paylink}"
            )
        elif 14 <= bucket_aging <= 30:
            template = (
                "⚠️ హెచ్చరిక {name} గారు,\n\n"
                "📌 లోన్ నంబర్: {loan_no}\n\n"
                "⏳ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
                "💰 TODAY EMI : ₹{edi}\n"
                "❌ OVERDUE AMOUNT : ₹{overdue}\n\n"
                "⚠️ ఈరోజే ₹{payable} చెల్లించండి, లేకపోతే అదనపు లేట్ ఫైన్ & CIBIL రిపోర్ట్‌లో నెగటివ్ ప్రభావం పడుతుంది.\n\n"
                "💳 చెల్లించండి: {paylink}"
            )
        elif 31 <= bucket_aging <= 60:
            template = (
                "🚨 ACTION REQUIRED - {name} గారు,\n\n"
                "📌 లోన్ నంబర్: {loan_no}\n\n"
                "⏳ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
                "💰 TODAY EMI : ₹{edi}\n"
                "❌ OVERDUE AMOUNT : ₹{overdue}\n\n"
                "⚠️ ఈరోజే ₹{payable} చెల్లించండి, లేకపోతే లీగల్ యాక్షన్ వస్తుంది.\n\n"
                "💳 తక్షణం చెల్లించండి: {paylink}\n\n"
                "🔹 లోన్ వివరాలు కోసం *1*\n"
                "🔹 సెటిల్మెంట్ వివరాలు కోసం *2*\n" 
                "➡️ అని రిప్లై చేయండి."
                )
        elif 61 <= bucket_aging <= 90:
            template = (
                "🛑 LEGAL WARNING – {name} గారు,\n\n"
                "📌 లోన్ నంబర్: {loan_no}\n\n"
                "⏳ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
                "💰 TODAY EMI : ₹{edi}\n"
                "❌ OVERDUE AMOUNT : ₹{overdue}\n\n"
                "⚠️ తక్షణం  ₹{payable}  చెల్లించకపోతే లీగల్ యాక్షన్ వస్తుంది, CIBIL రిపోర్ట్‌లో ప్రతికూల ప్రభావం పడుతుంది.\n\n"
                "💳 వెంటనే చెల్లించండి: {paylink}\n\n"
                "🔹 లోన్ వివరాలు కోసం *1*\n"
                "🔹 సెటిల్మెంట్ వివరాలు కోసం *2*\n" 
                "➡️ అని రిప్లై చేయండి."
            )
        elif bucket_aging > 90:
            template = (
                "⚖️ LEGAL NOTICE – {name} గారు,\n\n"
                "📌 లోన్ నంబర్: {loan_no}\n\n"
                "⏳ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
                "💰 TODAY EMI : ₹{edi}\n"
                "❌ OVERDUE AMOUNT : ₹{overdue}\n\n"
                "⚖️ తక్షణం ₹{payable} చెల్లించకపోతే కోర్టు ప్రాసెస్ ప్రారంభమవుతుంది.\n\n"
                "💳 వెంటనే చెల్లించండి: {paylink}\n\n"
                "🔹 లోన్ వివరాలు కోసం *1*\n"
                "🔹 సెటిల్మెంట్ వివరాలు కోసం *2*\n" 
                "➡️ అని రిప్లై చేయండి."
            )

        else:
            return None

    # ---------------- METHOD 2 (EMI Reminder with emi_day) ----------------
    elif method == "method2":
        # method2 should be sent only if edi != 0 (enforced in process_messages)
        template = (
            "👋 ప్రియమైన {name} గారు,\n\n"
            "📌 {emi_day} మీ లోన్ A/c {loan_no} కు ₹{edi} EMI ఉంది.\n\n"
            "💰 EMI AMOUNT: ₹{edi}\n\n"
            "⚠️ దయచేసి {emi_day} లోపు ₹{edi} చెల్లించండి, లేకపోతే అదనపు లేట్ ఫైన్ & CIBIL ప్రభావం పడుతుంది.\n\n"
            "💳 చెల్లించండి: {paylink}"
        )

    # ---------------- METHOD 3 (Bounce Reminder) ----------------
    elif method == "method3":
        # method3 should be sent only if edi != 0 (enforced in process_messages)
        template = (
            "⚠️ ప్రియమైన {name} గారు,\n\n"
            "❌ మీ లోన్ A/c {loan_no} EMI ₹{edi} బౌన్స్ అయింది.\n"
            "💸 బౌన్స్ ఛార్జీలు వర్తిస్తాయి.\n\n"
            "💰 TODAY EMI : ₹{edi}\n"
            "❌ OVERDUE AMOUNT : ₹{overdue}\n\n"
            "⚠️ వెంటనే ₹{payable} చెల్లించండి, లేకపోతే అదనపు ఫీజులు & CIBIL ప్రభావం పడుతుంది.\n\n"
            "💳 చెల్లించండి: {paylink}"
        )

    else:
        template = "ప్రియమైన {name} గారు, డేటా లోపం కారణంగా సందేశం రూపొందించబడలేదు."
        
    return template.format(name=name, loan_no=loan_no, advance=advance, edi=edi,
        overdue=overdue, payable=payable, days=int(bucket_aging), paylink=PAYMENT_LINK,
        emi_day=emi_day or get_telugu_weekday())

def send_whatsapp(mobile, message):
    mobile_str = str(mobile).strip()
    if not mobile_str.startswith("+"):
        mobile_str = f"+91{mobile_str}"
    payload = {"to": mobile_str, "text": message}
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}
    try:
        res = requests.post(WASENDER_URL, json=payload, headers=headers)
        return res.status_code == 200
    except Exception:
        return False

def notify_admin(message):
    if ADMIN_WHATSAPP:
        send_whatsapp(ADMIN_WHATSAPP, message)

def process_messages(file, skip_loans_input, sleep_min, sleep_max, method, emi_day="ఈ రోజు"):
    global stop_sending, task_running, sse_logs, report_rows, success_count, skipped_count, failed_count, current_total
    df = pd.read_excel(file)
    df.columns = normalize_columns(df.columns)
    skip_loans = load_skip_loans()
    total = len(df)
    current_total = total
    sent_count = 0
    sse_logs, report_rows = [], []
    success_count = skipped_count = failed_count = 0

    notify_admin(f"🚀 Message sending started.\nTotal records: {total}")
    time.sleep(30)

    # ✅ Milestones set (percentage thresholds)
    milestone_thresholds = [25, 50, 75, 100]
    notified = {m: False for m in milestone_thresholds}

    for idx, row in df.iterrows():
        if stop_sending:
            break

        name = get_value(row, ["CUSTOMER NAME", "CUSTOMERNAME", "NAME"])
        loan_no = str(get_value(row, ["LOAN A/C NO", "LOANA/CNO", "LOAN AC NO", "LOAN NO"]) or "").upper()
        mobile_raw = get_value(row, ["MOBILE NO", "MOBILENO", "PHONE", "MOBILENUMBER"])
        mobile = str(int(mobile_raw)) if isinstance(mobile_raw, float) else str(mobile_raw).strip() if pd.notna(mobile_raw) else ""
        edi = float(get_value(row, ["EDI AMOUNT", "EDIAMOUNT", "EDI"]) or 0)
        overdue = float(get_value(row, ["OVER DUE", "OVERDUE"]) or 0)
        advance = float(get_value(row, ["ADVANCE", "ADV"]) or 0)
        payable = (edi + overdue) - advance
        bucket_aging = parse_bucket_value(get_value(row, ["BUCKET AGING", "BUCKETAGING", "DAYS PENDING", "DPDS"]))

        # Skip checks
        if not name or not mobile:
            add_event("Skipped", "Missing Name or Mobile", mobile=mobile, bucket=bucket_aging, progress=f"{sent_count}/{total}", wait="-")
            continue
        if loan_no in skip_loans:
            add_event("Skipped", f"Loan {loan_no} in skip list", mobile=mobile, bucket=bucket_aging, progress=f"{sent_count}/{total}", wait="-")
            continue
        if payable <= 0:
            add_event("Skipped", "No pending amount", mobile=mobile, bucket=bucket_aging, progress=f"{sent_count}/{total}", wait="-")
            continue
        if method == "method1" and bucket_aging == 0:
            add_event("Skipped", "Method1 requires bucket aging > 0", mobile=mobile, bucket=bucket_aging, progress=f"{sent_count}/{total}", wait="-")
            continue
        if method in ["method2", "method3"] and edi == 0:
            add_event("Skipped", f"{method} requires EDI != 0", mobile=mobile, bucket=bucket_aging, progress=f"{sent_count}/{total}", wait="-")
            continue

        message = build_msg_dynamic(row, name, loan_no, advance, edi, overdue, payable, method, emi_day)
        if not message:
            add_event("Skipped", "No message generated", mobile=mobile, bucket=bucket_aging, progress=f"{sent_count}/{total}", wait="-")
            continue

        wait_time = random.randint(sleep_min, sleep_max)
        success = send_whatsapp(mobile, message)
        sent_count += 1
        if success:
            add_event("Success", f"{name}", mobile=mobile, bucket=bucket_aging, progress=f"{sent_count}/{total}", wait=f"{wait_time}s")
        else:
            add_event("Failed", f"Failed {name}", mobile=mobile, bucket=bucket_aging, progress=f"{sent_count}/{total}", wait=f"{wait_time}s")

        # ✅ Milestone notifications (>= 25%, 50%, 75%, 100%)
        progress_percent = int((sent_count / total) * 100)
        for m in milestone_thresholds:
            if not notified[m] and progress_percent >= m:
                time.sleep(20)  # cooldown before notifying admin
                notify_admin(f"📊 Milestone Reached: {m}%\n✅ Sent {sent_count}/{total}")
                notified[m] = True

        time.sleep(wait_time)

    if not stop_sending:
        notify_admin(f"✅ Completed. Sent {sent_count}/{total} messages.")
        add_event("Completed", f"Completed. Sent {sent_count}/{total}", progress=f"{sent_count}/{total}", wait="-")

    task_running = False
    stop_sending = False
# ---------------- ROUTES ----------------
@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    global stop_sending, task_running, current_total, sse_logs, report_rows, success_count, skipped_count, failed_count
    if request.method == "POST":
        if task_running:
            return render_template("index.html", live=True, logs=logs, total_customers=current_total)
        file = request.files.get("file")
        skip_loans_input = request.form.get("skip_loans", "").strip()
        sleep_min = int(request.form.get("sleep_min", "61"))
        sleep_max = int(request.form.get("sleep_max", "120"))
        method = request.form.get("method", "method1")
        emi_day = request.form.get("emi_day", "").strip() or get_telugu_weekday()
        if skip_loans_input:
            save_skip_loans(skip_loans_input)
        if not file:
            return redirect(url_for("index"))
        sse_logs, report_rows = [], []
        success_count = skipped_count = failed_count = 0
        stop_sending = False
        task_running = True
        file_content = file.read()
        try:
            current_total = len(pd.read_excel(BytesIO(file_content)))
        except Exception:
            current_total = 0
        threading.Thread(
            target=process_messages,
            args=(BytesIO(file_content), skip_loans_input, sleep_min, sleep_max, method, emi_day)
        ).start()
        return render_template("index.html",
                               skip_loans=skip_loans_input,
                               sleep_min=sleep_min, sleep_max=sleep_max,
                               method=method, emi_day=emi_day,
                               live=True, logs=logs, total_customers=current_total)
    return render_template("index.html",
                           skip_loans=",".join(load_skip_loans()),
                           sleep_min=61, sleep_max=120,
                           method="method1",
                           emi_day=get_telugu_weekday(),
                           live=task_running, logs=logs, total_customers=current_total)

@app.route("/stop")
@login_required
def stop():
    global stop_sending
    stop_sending = True
    notify_admin("🛑 Script stopped by user.")
    return redirect(url_for("index"))

@app.route("/stream_logs")
@login_required
def stream_logs():
    def generate():
        last_index = 0
        while True:
            global sse_logs
            if last_index < len(sse_logs):
                for i in range(last_index, len(sse_logs)):
                    yield f"data: {sse_logs[i]}\n\n"
                last_index = len(sse_logs)
            time.sleep(1)
    return Response(generate(), mimetype="text/event-stream")

@app.route("/download_report")
@login_required
def download_report():
    global report_rows
    if not report_rows:
        df_empty = pd.DataFrame(columns=["time","status","message","mobile","bucket","progress","wait"])
        output = BytesIO()
        df_empty.to_excel(output, index=False)
        output.seek(0)
        return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                         as_attachment=True, download_name="report.xlsx")
    df = pd.DataFrame(report_rows)
    cols = ["time","status","message","mobile","bucket","progress","wait"]
    for c in cols:
        if c not in df.columns: df[c] = ""
    df = df[cols]
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)
    return send_file(output, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                     as_attachment=True, download_name="report.xlsx")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
