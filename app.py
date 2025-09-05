from flask import Flask, render_template, request, redirect, url_for, Response
import re, pandas as pd, requests, os, time, threading, random
from io import BytesIO

# ---------------- CONFIG ----------------
WASENDER_URL = os.getenv("WASENDER_URL", "https://wasenderapi.com/api/send-message")
API_KEY = os.getenv("WASENDER_API_KEY", "eb292d52c33035e6c9c31691a1828baed465764ffe43b60c466d8c5f3bf9e462")
PAYMENT_LINK = os.getenv("PAYMENT_LINK", "https://websitepayments.veritasfin.in")

app = Flask(__name__)
logs = []

# Global state
stop_sending = False
task_running = False


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

    # ----------------- Bucket Templates -----------------
    if days_pending == 0:  # Fresh reminder, no days line
        template = (
            "👋 ప్రియమైన {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "💰 EMI బకాయి: ₹{payable}\n\n"
            "⚠️ ఈరోజే చెల్లించండి, లేట్ ఫైన్ & CIBIL స్కోర్ ప్రభావం నివారించండి.\n\n"
            "💳 చెల్లించండి: {paylink}"
        )

    elif 1 <= days_pending <= 13:  # Normal Reminder
        template = (
            "👋 ప్రియమైన {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "💰 EMI OVERDUE: ₹{payable}\n"
            "⏳ {days} రోజులుగా పెండింగ్‌లో ఉంది.\n\n"
            "⚠️ దయచేసి వెంటనే చెల్లించండి, లేకపోతే లేట్ ఫైన్ & CIBIL స్కోర్‌పై ప్రభావం ఉంటుంది.\n\n"
            "💳 చెల్లించండి: {paylink}"
        )

    elif 14 <= days_pending <= 30:  # Warning
        template = (
            "⚠️ హెచ్చరిక - {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "⏳ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
            "💸 మొత్తం OVERDUE: ₹{payable}\n\n"
            "తక్షణం చెల్లించకపోతే పెనాల్టీలు మరియు CIBIL స్కోర్‌పై ప్రభావం ఉంటుంది.\n\n"
            "💳 చెల్లించండి: {paylink}"
        )

    elif 31 <= days_pending <= 60:  # Strong Warning
        template = (
            "🚨 ACTION REQUIR - {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "❌ {days} రోజులుగా EMI చెల్లించలేదు.\n"
            "💸 మొత్తం OVERDUE: ₹{payable}\n\n"
            "⚠️ వెంటనే చెల్లించకపోతే లీగల్ యాక్షన్ & రికవరీ ప్రాసెస్ ప్రారంభమవుతుంది.\n\n"
            "💳 తక్షణం చెల్లించండి: {paylink}"
        )

    elif 61 <= days_pending <= 90:  # Legal Warning
        template = (
            "🛑 LEGAL WARNING – {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "❌ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
            "💸 మొత్తం OVERDUE: ₹{payable}\n\n"
            "⚠️ తక్షణం చెల్లించకపోతే లీగల్ యాక్షన్ తీసుకోవాల్సి వస్తుంది.\n\n"
            "💳 వెంటనే చెల్లించండి: {paylink}"
        )

    else:  # days_pending >= 91 → Legal Action
        template = (
            "⚖️ LEGAL NOTICE – {name} గారు,\n\n"
            "📌 లోన్ నంబర్: {loan_no}\n"
            "❌ {days} రోజులుగా EMI OVERDUE ఉంది.\n"
            "💸 మొత్తం OVERDUE: ₹{payable}\n\n"
            "⚖️ కోర్టు లీగల్ ప్రాసెస్ & రికవరీ చర్యలు ప్రారంభమవుతాయి. ఇది చివరి హెచ్చరిక.\n\n"
            "💳 వెంటనే చెల్లించండి: {paylink}"
        )

    # ----------------- Fill Values -----------------
    return template.format(
        name=name,
        loan_no=loan_no,
        advance=advance,
        edi=edi,
        overdue=overdue,
        payable=payable,
        days=int(days_pending),
        paylink=PAYMENT_LINK
    )

def send_whatsapp(mobile, message):
    """Send text only via WaSender"""
    mobile_str = str(mobile).strip()
    if not mobile_str.startswith("+"):
        mobile_str = f"+91{mobile_str}"

    payload = {
        "to": mobile_str,
        "text": message
    }

    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    try:
        res = requests.post(WASENDER_URL, json=payload, headers=headers)
        print("Response:", res.status_code, res.text)
        return res.status_code == 200
    except Exception as e:
        print("Error:", e)
        return False


# ----------- Background sending function ------------
def process_messages(file, skip_loans_input, sleep_min, sleep_max):
    global logs, stop_sending, task_running
    df = pd.read_excel(file)
    df.columns = normalize_columns(df.columns)
    skip_loans = [ln.strip().upper() for ln in re.split(r'[,\s]+', skip_loans_input) if ln.strip()]

    total = len(df)
    sent_count = 0

    for idx, row in df.iterrows():
        if stop_sending:
            logs.append("⏹ Sending stopped by user.")
            break

        name = get_value(row, ["CUSTOMER NAME", "CUSTOMERNAME", "NAME"])
        loan_no = str(get_value(row, ["LOAN A/C NO", "LOANA/CNO", "LOAN AC NO", "LOAN NO"]) or "").upper()
        mobile_raw = get_value(row, ["MOBILE NO", "MOBILENO", "PHONE", "MOBILENUMBER"])

        if pd.notna(mobile_raw):
            if isinstance(mobile_raw, float):
                mobile = str(int(mobile_raw))
            else:
                mobile = str(mobile_raw).strip()
        else:
            mobile = ""

        edi = float(get_value(row, ["EDI AMOUNT", "EDIAMOUNT", "EDI"]) or 0)
        overdue = float(get_value(row, ["OVER DUE", "OVERDUE"]) or 0)
        advance = float(get_value(row, ["ADVANCE", "ADV"]) or 0)
        payable = (edi + overdue) - advance

        if not name or not mobile:
            logs.append(f"⚠️ Skipped row – Missing Name or Mobile")
            continue

        if loan_no in skip_loans:
            logs.append(f"⏩ Skipped {name} ({mobile}) – Loan {loan_no} in skip list")
            continue

        if payable <= 0:
            logs.append(f"⏩ Skipped {name} ({mobile}) – No pending amount")
            continue

        message = build_msg_dynamic(row, name, loan_no, advance, edi, overdue, payable)
        success = send_whatsapp(mobile, message)
        sent_count += 1

        logs.append(f"✅ Sent to {name} ({mobile})" if success else f"❌ Failed {name} ({mobile})")
        logs.append(f"📊 Progress: {sent_count} / {total}")

        wait_time = random.randint(sleep_min, sleep_max)
        logs.append(f"⏳ Waiting {wait_time} seconds before next message...")
        time.sleep(wait_time)

    if not stop_sending:
        logs.append("🎉 Completed sending all messages")

    task_running = False
    stop_sending = False  # reset flag


@app.route("/", methods=["GET", "POST"])
def index():
    global logs, stop_sending, task_running

    if request.method == "POST":
        if task_running:
            logs.append("⚠️ A sending task is already running. Please stop it first.")
            return render_template("index.html", live=True, logs=logs)

        file = request.files.get("file")
        skip_loans_input = request.form.get("skip_loans", "").strip()
        sleep_min = int(request.form.get("sleep_min", "61"))
        sleep_max = int(request.form.get("sleep_max", "180"))

        if not file:
            return redirect(url_for("index"))

        # clear logs only when new task starts
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

    # 🔴 do NOT reset logs here anymore
    return render_template("index.html", skip_loans="", sleep_min=61, sleep_max=180, live=task_running, logs=logs)


@app.route("/stop")
def stop():
    global stop_sending
    stop_sending = True
    logs.append("🛑 Stop request received. Finishing current message...")
    return redirect(url_for("index"))


@app.route("/stream_logs")
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
