import os, json
from datetime import datetime, timedelta
import uuid
import streamlit as st
import snowflake.connector

# --- Snowflake from env vars (recommended for Iguazio) ---
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "vistra.us-east-1.privatelink")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "RTL_PRD_WHS")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "RETAIL_PRD")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "USERDB_MKT")

REQUEST_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.OPTIC_ADHOC_LLM_REQUESTS"

def get_conn():
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )

def insert_request(payload: dict):
    sql = f"""
    INSERT INTO {REQUEST_TABLE} (
      REQUEST_ID, CREATED_TS, CREATED_BY, STATUS, PRIORITY,
      START_TS, END_TS, JOBNAMES, MAX_ROWS, MODEL_TYPE,
      PROMPT_NAME, USER_PROMPT, NOTES
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    params = [
        payload["REQUEST_ID"],
        payload["CREATED_TS"],
        payload["CREATED_BY"],
        payload["STATUS"],
        payload["PRIORITY"],
        payload["START_TS"],
        payload["END_TS"],
        payload["JOBNAMES"],
        payload["MAX_ROWS"],
        payload["MODEL_TYPE"],
        payload["PROMPT_NAME"],
        payload["USER_PROMPT"],
        payload["NOTES"],
    ]
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()

def main():
    st.title("OPTIC Ad-Hoc LLM Request Intake")
    st.caption("This app submits requests to Snowflake. Processing happens on a separate worker.")

    today = datetime.today().date()

    st.sidebar.header("Request Parameters")

    created_by = st.sidebar.text_input("Your name / email (for tracking)", value="")
    priority = st.sidebar.number_input("Priority (higher = sooner)", min_value=0, max_value=100, value=10, step=1)

    date_range = st.sidebar.date_input("Interaction date range", (today, today))
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    start_ts = datetime.combine(start_date, datetime.min.time())
    end_ts = datetime.combine(end_date + timedelta(days=1), datetime.min.time())

    jobnames = st.sidebar.multiselect(
        "Jobnames",
        ["Retention", "Acquisition", "Service"],
        default=["Retention", "Acquisition", "Service"],
    )

    max_rows = st.sidebar.number_input("Max calls", min_value=1, max_value=200000, value=5000, step=500)

    model_type = st.sidebar.radio("Model type (metadata only)", ["reasoning", "mini"], index=0)

    st.subheader("Prompt")
    prompt_name = st.text_input("Prompt Name", placeholder="Example: Supervisor Blatant Refusal v2")
    user_prompt = st.text_area("Prompt Instructions", height=220)

    notes = st.text_area("Notes (optional)", height=100)

    submitted = st.button("Submit Request")

    if not submitted:
        return

    if not created_by.strip():
        st.error("Please enter your name/email for tracking.")
        return
    if not prompt_name.strip():
        st.error("Please enter a Prompt Name.")
        return
    if not user_prompt.strip():
        st.error("Please enter the Prompt Instructions.")
        return

    request_id = str(uuid.uuid4())

    payload = {
        "REQUEST_ID": request_id,
        "CREATED_TS": datetime.utcnow(),
        "CREATED_BY": created_by.strip(),
        "STATUS": "NEW",
        "PRIORITY": int(priority),
        "START_TS": start_ts,
        "END_TS": end_ts,
        "JOBNAMES": json.dumps(jobnames),
        "MAX_ROWS": int(max_rows),
        "MODEL_TYPE": model_type,
        "PROMPT_NAME": prompt_name.strip(),
        "USER_PROMPT": user_prompt.strip(),
        "NOTES": notes.strip(),
    }

    try:
        insert_request(payload)
    except Exception as e:
        st.error(f"Failed to submit request: {e}")
        return

    st.success("Submitted!")
    st.code(f"REQUEST_ID: {request_id}")

if __name__ == "__main__":
    main()
