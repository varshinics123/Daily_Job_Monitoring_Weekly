
import os
import csv
import requests

from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo
from azure.identity import ClientSecretCredential


# ============================================================
# CONFIGURATION
# ============================================================

TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]
SYNAPSE_ENDPOINT = os.environ["SYNAPSE_ENDPOINT"].rstrip("/")

API_VERSION = "2020-12-01"
SYNAPSE_SCOPE = "https://dev.azuresynapse.net/.default"
LOCAL_TZ = ZoneInfo("Asia/Kolkata")
CSV_FILE = "synapse_pipeline_results.csv"

MONITORED_PIPELINES = [
    "PL_Captura_Snapshots",
    "BronzeToBI_daily_530_am",
    "BronzeToBI_daily_530_am_1",
    "BronzeToBI_daily_5am",
]

PIPELINE_ORDER = {
    name: index
    for index, name in enumerate(MONITORED_PIPELINES)
}

REPORT_DATE = os.environ.get("REPORT_DATE", "").strip()
EVENT_NAME = os.environ.get("GITHUB_EVENT_NAME", "")

COLUMNS = [
    "Pipeline name",
    "Run start",
    "Run end",
    "Duration",
    "Triggered by",
    "Status",
    "Run ID",
    "Avg of duration",
    "Error / Message",
]


# ============================================================
# AUTHENTICATION
# ============================================================

def get_access_token():
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )

    return credential.get_token(SYNAPSE_SCOPE).token


# ============================================================
# DATETIME HELPERS
# ============================================================

def parse_datetime(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except (ValueError, TypeError):
        return None


def to_ist(value):
    dt = parse_datetime(value)

    if dt is None:
        return None

    return dt.astimezone(LOCAL_TZ)


def format_datetime(value):
    dt = to_ist(value)

    if dt is None:
        return "-"

    return dt.strftime("%d/%m/%Y %I:%M:%S %p")


def duration_seconds(run):
    start = parse_datetime(run.get("runStart"))
    end = parse_datetime(run.get("runEnd"))

    # In-progress runs have no completed duration.
    if start is None or end is None:
        return None

    return max(0, (end - start).total_seconds())


def format_duration(seconds):
    if seconds is None:
        return "-"

    total = round(seconds)
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)

    return f"{hours:02}:{minutes:02}:{secs:02}"


def iso_utc(dt):
    return dt.astimezone(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


# ============================================================
# DETERMINE REPORTING PERIOD
# ============================================================

def get_report_range():
    today = datetime.now(LOCAL_TZ).date()

    if EVENT_NAME == "workflow_dispatch" and REPORT_DATE:
        try:
            selected_date = datetime.strptime(
                REPORT_DATE, "%Y-%m-%d"
            ).date()
        except ValueError as exc:
            raise ValueError(
                "REPORT_DATE must use YYYY-MM-DD format."
            ) from exc

        if selected_date > today:
            raise ValueError("Future dates cannot be selected.")

        return selected_date, selected_date, "Manual date"

    # Scheduled run or manual run with date left blank:
    # previous Monday through Friday.
    current_monday = today - timedelta(days=today.weekday())
    previous_monday = current_monday - timedelta(days=7)
    previous_friday = previous_monday + timedelta(days=4)

    return previous_monday, previous_friday, "Previous week"


# ============================================================
# FETCH ALL SYNAPSE RUN PAGES
# ============================================================

def get_pipeline_runs(start_date, end_date):
    token = get_access_token()

    url = (
        f"{SYNAPSE_ENDPOINT}/queryPipelineRuns"
        f"?api-version={API_VERSION}"
    )

    start_ist = datetime.combine(
        start_date, time.min
    ).replace(tzinfo=LOCAL_TZ)

    end_ist = datetime.combine(
        end_date + timedelta(days=1), time.min
    ).replace(tzinfo=LOCAL_TZ)

    now_utc = datetime.now(timezone.utc)
    start_utc = start_ist.astimezone(timezone.utc)
    end_utc = min(
        end_ist.astimezone(timezone.utc), now_utc
    )

    if start_utc >= end_utc:
        return []

    payload = {
        "lastUpdatedAfter": iso_utc(start_utc),
        "lastUpdatedBefore": iso_utc(end_utc),
        "filters": [
            {
                "operand": "PipelineName",
                "operator": "In",
                "values": MONITORED_PIPELINES,
            }
        ],
        "orderBy": [
            {
                "orderBy": "RunStart",
                "order": "ASC",
            }
        ],
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    all_runs = []
    continuation_token = None

    with requests.Session() as session:
        while True:
            if continuation_token:
                payload["continuationToken"] = continuation_token
            else:
                payload.pop("continuationToken", None)

            response = session.post(
                url,
                headers=headers,
                json=payload,
                timeout=60,
            )
            response.raise_for_status()

            data = response.json()
            page_runs = data.get("value", [])
            all_runs.extend(page_runs)

            print(
                f"Fetched page: {len(page_runs)} runs; "
                f"total: {len(all_runs)}"
            )

            continuation_token = data.get("continuationToken")

            if not continuation_token:
                break

    return all_runs


# ============================================================
# FILTER BY PIPELINE START DATE IN IST
# ============================================================

def filter_runs(runs, start_date, end_date):
    filtered = []

    for run in runs:
        if run.get("pipelineName") not in MONITORED_PIPELINES:
            continue

        start = to_ist(run.get("runStart"))

        if start is None:
            continue

        if start_date <= start.date() <= end_date:
            filtered.append(run)

    return filtered


# ============================================================
# CAPTURA JOB GROUPS FOR AVERAGES
# ============================================================

def get_average_group(run):
    pipeline = run.get("pipelineName")
    start = to_ist(run.get("runStart"))

    if pipeline == "PL_Captura_Snapshots":
        if start is None:
            return None

        if start.hour == 13 and start.minute == 30:
            return "PL_Captura_Snapshots_1_30_PM"

        if start.hour == 23 and start.minute == 30:
            return "PL_Captura_Snapshots_11_30_PM"

        return None

    return pipeline


def calculate_averages(runs):
    grouped = {}

    for run in runs:
        seconds = duration_seconds(run)

        # Only completed runs contribute to averages.
        if seconds is None:
            continue

        group = get_average_group(run)

        if group is not None:
            grouped.setdefault(group, []).append(seconds)

    return {
        group: sum(values) / len(values)
        for group, values in grouped.items()
        if values
    }


# ============================================================
# TRIGGER AND ERROR DETAILS
# ============================================================

def get_triggered_by(run):
    invoked_by = run.get("invokedBy") or {}

    if isinstance(invoked_by, dict):
        return (
            invoked_by.get("name")
            or invoked_by.get("pipelineName")
            or invoked_by.get("id")
            or "-"
        )

    return str(invoked_by) if invoked_by else "-"


def get_error_message(run):
    message = run.get("message")

    if message:
        return str(message)

    error = run.get("error")

    if isinstance(error, dict):
        return str(
            error.get("message")
            or error.get("code")
            or "-"
        )

    if error:
        return str(error)

    return "-"


# ============================================================
# SORT AND BUILD TABLE
# ============================================================

def sort_runs(runs):
    def key(run):
        pipeline = run.get("pipelineName", "")
        start = to_ist(run.get("runStart"))

        job_order = 0

        if pipeline == "PL_Captura_Snapshots":
            group = get_average_group(run)

            if group == "PL_Captura_Snapshots_1_30_PM":
                job_order = 0
            elif group == "PL_Captura_Snapshots_11_30_PM":
                job_order = 1
            else:
                job_order = 2

        return (
            PIPELINE_ORDER.get(pipeline, 999),
            job_order,
            start or datetime.max.replace(tzinfo=LOCAL_TZ),
        )

    return sorted(runs, key=key)


def build_report(runs, averages):
    rows = []
    average_displayed = set()

    for run in sort_runs(runs):
        group = get_average_group(run)
        average_text = ""

        if group and group not in average_displayed:
            average = averages.get(group)

            if average is not None:
                average_text = format_duration(average)

            average_displayed.add(group)

        rows.append({
            "Pipeline name": run.get("pipelineName", "-"),
            "Run start": format_datetime(run.get("runStart")),
            "Run end": format_datetime(run.get("runEnd")),
            "Duration": format_duration(duration_seconds(run)),
            "Triggered by": get_triggered_by(run),
            "Status": run.get("status", "Unknown"),
            "Run ID": run.get("runId", "-"),
            "Avg of duration": average_text,
            "Error / Message": get_error_message(run),
        })

    return rows


# ============================================================
# PRINT TABLE
# ============================================================

def print_table(rows):
    print()
    print("SYNAPSE JOB MONITORING RESULTS")
    print("=" * 180)

    if not rows:
        print("No monitored pipeline runs found.")
        return

    widths = {
        col: max(
            len(col),
            max(len(str(row.get(col, ""))) for row in rows),
        )
        for col in COLUMNS
    }

    print(" | ".join(col.ljust(widths[col]) for col in COLUMNS))
    print("-+-".join("-" * widths[col] for col in COLUMNS))

    for row in rows:
        print(
            " | ".join(
                str(row.get(col, "")).ljust(widths[col])
                for col in COLUMNS
            )
        )

    print(f"\nTotal rows: {len(rows)}")


# ============================================================
# CREATE CSV
# ============================================================

def create_csv(rows):
    with open(
        CSV_FILE, "w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.DictWriter(
            file, fieldnames=COLUMNS
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"CSV generated: {CSV_FILE}")


# ============================================================
# MAIN
# ============================================================

def main():
    print("=" * 80)
    print("Starting Synapse Job Monitoring")
    print("=" * 80)

    start_date, end_date, period_type = get_report_range()

    print(
        f"Reporting period: {start_date:%d/%m/%Y} "
        f"to {end_date:%d/%m/%Y} (IST)"
    )
    print(f"Report mode: {period_type}")

    all_runs = get_pipeline_runs(start_date, end_date)
    print(f"Total API runs received: {len(all_runs)}")

    monitored_runs = filter_runs(
        all_runs, start_date, end_date
    )
    print(f"Matching monitored runs: {len(monitored_runs)}")

    averages = calculate_averages(monitored_runs)
    rows = build_report(monitored_runs, averages)

    print_table(rows)
    create_csv(rows)

    print("Monitoring completed.")


if __name__ == "__main__":
    main()
