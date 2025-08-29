from flask import Flask, render_template, request, redirect, url_for
import re, pandas as pd, requests, os
import time

# ---------------- CONFIG ----------------
WASENDER_URL = os.getenv("WASENDER_URL", "https://wasenderapi.com/api/send-message")
API_KEY = os.getenv("WASENDER_API_KEY", "")
PAYMENT_LINK = os.getenv("PAYMENT_LINK", "https://websitepayments.veritasfin.in")

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = "static"
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


def send_whatsapp(mobile, message, image_url=None):
    """Send text with optional image caption via WaSender"""
    mobile_str = str(mobile).strip()
    if not mobile_str.startswith("+"):
        mobile_str = f"+91{mobile_str}"

    if image_url:
        payload = {
            "to": mobile_str,
            "type": "image",
            "image": {
                "url": image_url,
                "caption": message
            }
        }
    else:
        payload = {"to": mobile_str, "text": message}

    headers = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    try:
        res = requests.post(WASENDER_URL, json=payload, headers=headers)
        print("Response:", res.status_code, res.text)
        return res.status_code == 200
    except Exception as e:
        print("Error:", e)
        return False


# ----------- Flask Routes ------------
@app.route("/", methods=["GET", "POST"])
def index():
    global logs
    preview_message, preview_image = None, None

    if request.method == "POST":
        file = request.files.get("file")
        template = request.form.get("template")
        skip_loans_input = request.form.get("skip_loans", "").strip()

        # convert to list of loan numbers
        skip_loans = [ln.strip().upper() for ln in re.split(r'[,\s]+', skip_loans_input) if ln.strip()]

        if not file or not template:
            return redirect(url_for("index"))

        # fixed image from project static folder
        image_file = "my_banner.jpeg"
        image_path = os.path.join(app.config["UPLOAD_FOLDER"], image_file)
        image_url = None
        if os.path.exists(image_path):
            image_url = url_for("static", filename=image_file, _external=True)

        df = pd.read_excel(file)
        df.columns = normalize_columns(df.columns)

        logs = []
        first_preview_done = False

        for _, row in df.iterrows():
            name = get_value(row, ["CUSTOMER NAME", "CUSTOMERNAME", "NAME"])
            loan_no = str(get_value(row, ["LOAN A/C NO", "LOANA/CNO", "LOAN AC NO", "LOAN NO"]) or "").upper()
            mobile = get_value(row, ["MOBILE NO", "MOBILENO", "PHONE", "MOBILENUMBER"])
            edi = float(get_value(row, ["EDI AMOUNT", "EDIAMOUNT", "EDI"]) or 0)
            overdue = float(get_value(row, ["OVER DUE", "OVERDUE"]) or 0)
            advance = float(get_value(row, ["ADVANCE", "ADV"]) or 0)
            payable = (edi + overdue) - advance

            if not name or not mobile:
                logs.append(f"⚠️ Skipped row – Missing Name or Mobile")
                continue

            # 🔹 Skip loan numbers entered in HTML
            if loan_no in skip_loans:
                logs.append(f"⏩ Skipped {name} ({mobile}) – Loan {loan_no} in skip list")
                continue

            if payable <= 0:
                logs.append(f"⏩ Skipped {name} ({mobile}) – No pending amount")
                continue

            message = build_msg(template, name, loan_no, advance, edi, overdue, payable)

            if not first_preview_done:
                preview_message = message
                preview_image = image_url
                first_preview_done = True

            success = send_whatsapp(mobile, message, image_url=image_url)
            logs.append(f"✅ Sent to {name} ({mobile})" if success else f"❌ Failed {name} ({mobile})")
            

        return render_template("index.html", logs=logs, template=template,
                               preview=preview_message, preview_image=preview_image,
                               skip_loans=skip_loans_input)

    default_template = (
        "👋 ప్రియమైన {name} గారు,\n\n"
        "మీ Veritas Finance లో ఉన్న {loan_no} లోన్ నంబరుకు పెండింగ్ అమౌంట్ వివరాలు:\n\n"
        "📌 EDI మొత్తం: ₹{edi}\n"
        "🔴 OVER DUE మొత్తం: ₹{overdue}\n"
        "✅ చెల్లించవలసిన మొత్తం: ₹{payable}\n\n"
        "⚠️ దయచేసి వెంటనే చెల్లించండి, లేకపోతే పెనాల్టీలు మరియు CIBIL స్కోర్‌పై ప్రభావం పడుతుంది.\n"
        "💳 చెల్లించడానికి లింక్: {paylink}"
    )
    return render_template("index.html", logs=logs, template=default_template,
                           preview=preview_message, preview_image=None, skip_loans="")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # 🔹 Render requires binding to PORT env var
    app.run(host="0.0.0.0", port=port)
