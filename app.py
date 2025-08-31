from flask import Flask, render_template, request, redirect, url_for, Response
import re, pandas as pd, requests, os, time, threading
from io import BytesIO

# ---------------- CONFIG ----------------
WASENDER_URL = os.getenv("WASENDER_URL", "https://wasenderapi.com/api/send-message")
API_KEY = os.getenv("WASENDER_API_KEY", "")
PAYMENT_LINK = os.getenv("PAYMENT_LINK", "https://websitepayments.veritasfin.in")

app = Flask(__name__)
logs = []

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

def build_msg(template, name, loan_no, advance, edi, overdue, payable):
    return template.format(
        name=name,
        loan_no=loan_no,
        advance=advance,
        edi=edi,
        overdue=overdue,
        payable=payable,
        paylink=PAYMENT_LINK
    )

def send_whatsapp(mobile, message):
    """Send text message via WaSender only"""
    mobile_str = str(mobile).strip()
    if not mobile_str.startswith("+"):
        mobile_str = f"+91{mobile_str}"

    payload = {"to": mobile_str, "text": message}
    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    try:
        res = requests.post(WASENDER_URL, json=payload, headers=headers)
        print("Response:", res.status_code, res.text)
        return res.status_code == 200
    except Exception as e:
        print("Error:", e)
        return False

# ----------- Background sending function ------------
def process_messages(file, template, skip_loans_input):
    global logs
    df = pd.read_excel(file)
    df.columns = normalize_columns(df.columns)
    skip_loans = [ln.strip().upper() for ln in re.split(r'[,\s]+', skip_loans_input) if ln.strip()]

    for _, row in df.iterrows():
        name = get_value(row, ["CUSTOMER NAME", "CUSTOMERNAME", "NAME"])
        loan_no = str(get_value(row, ["LOAN A/C NO", "LOANA/CNO", "LOAN AC NO", "LOAN NO"]) or "").upper()
        mobile_raw = get_value(row, ["MOBILE NO", "MOBILENO", "PHONE", "MOBILENUMBER"])
        if pd.notna(mobile_raw):
            if isinstance(mobile_raw, float):
                mobile = str(int(mobile_raw))   # removes .0
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

        message = build_msg(template, name, loan_no, advance, edi, overdue, payable)
        success = send_whatsapp(mobile, message)
        logs.append(f"✅ Sent to {name} ({mobile})" if success else f"❌ Failed {name} ({mobile})")

        time.sleep(1)  # Respect WaSender free trial limit



@app.route("/", methods=["GET", "POST"])
def index():
    global logs, preview_message
    logs = []
    preview_message = ""

    default_template = (
        "👋 ప్రియమైన {name} గారు,\n\n"
        "మీ Veritas Finance లో ఉన్న పెండింగ్ వివరాలు:\n\n"
        "🆔 Loan ID: {loan_no}\n"
        "📌 EDI మొత్తం: ₹{edi}\n"
        "🔴 OVER DUE మొత్తం: ₹{overdue}\n"
        "✅ చెల్లించవలసిన మొత్తం: ₹{payable}\n\n"
        "⚠️ దయచేసి వెంటనే చెల్లించండి, లేకపోతే పెనాల్టీలు మరియు CIBIL స్కోర్‌పై ప్రభావం పడుతుంది.\n\n"
        "💳 చెల్లించడానికి లింక్: {paylink}"
    )

    if request.method == "POST":
        file = request.files.get("file")
        template = request.form.get("template") or default_template
        skip_loans_input = request.form.get("skip_loans", "").strip()

        if not file:
            return redirect(url_for("index"))

        # Read file into memory
        file_bytes = BytesIO(file.read())

        # Start background thread with file bytes
        thread = threading.Thread(target=process_messages, args=(file_bytes, template, skip_loans_input))
        thread.start()

        return render_template("index.html", template=template, skip_loans=skip_loans_input, live=True, preview=preview_message, logs=[])

    return render_template("index.html", template=default_template, skip_loans="", live=False, preview=preview_message, logs=[])


# ----------- SSE Route for live logs ------------
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
