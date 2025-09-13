import streamlit as st
import requests
import time
import os

API = os.environ.get("API_BASE", "http://localhost:8000")

st.title("Hudson's Bakery – Forecast Console")

# --- Upload section
uploaded = st.file_uploader("Upload raw CSV", type=["csv"])

if st.button("Upload & Enrich", disabled=not uploaded):
    try:
        files = {"file": (uploaded.name, uploaded.getvalue(), "text/csv")}
        r = requests.post(f"{API}/ingest_enrich", files=files, timeout=60)
        if r.status_code != 200:
            st.error(f"Failed to start ingest: {r.text}")
        else:
            job_id = r.json().get("job_id")
            if not job_id:
                st.error("Server did not return a job_id.")
            else:
                st.session_state["job_id"] = job_id
                st.info(f"Job started: {job_id}")
    except Exception as e:
        st.error(f"Request error: {e}")

job_id = st.session_state.get("job_id")

if job_id:
    st.subheader("Ingestion Status")

    # Optional: auto-refresh that re-runs the app once per interval
    auto = st.checkbox("Auto-refresh", value=True, help="Automatically refresh status every ~1.2s")

    # Fetch current status/logs once per run
    data = {}
    try:
        s = requests.get(f"{API}/jobs/{job_id}", timeout=30)
        if s.status_code != 200:
            st.error(f"Status fetch failed: HTTP {s.status_code}")
        else:
            data = s.json()
    except Exception as e:
        st.error(f"Status request error: {e}")

    status = data.get("status", "UNKNOWN")
    st.info(f"Status: **{status}**")

    # Single logs widget per run with a unique, stable key for this job_id
    st.text_area(
        "Logs",
        data.get("log", ""),
        height=240,
        key=f"logs_{job_id}"
    )

    # Final actions
    if status == "SUCCESS" and data.get("ready_for_prediction"):
        st.success("Upload Success! Data inserted & enriched.")
        st.button("Start Prediction", help="(This will call Manish’s endpoint)", disabled=False)
    elif status == "FAILED":
        st.error("Job failed. Check logs above.")
    else:
        # For PENDING/RUNNING/etc.
        cols = st.columns(2)
        with cols[0]:
            if st.button("Refresh now"):
                st.experimental_rerun()
        with cols[1]:
            st.caption("Waiting for job to finish...")

        # Light-weight auto-refresh without creating duplicate widgets
        if auto and status not in ("SUCCESS", "FAILED"):
            time.sleep(1.2)
            st.rerun()


# py -m streamlit run ui/app.py --server.port 8501
