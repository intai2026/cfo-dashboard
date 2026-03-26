import os
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from intuitlib.client import AuthClient
from streamlit.errors import StreamlitSecretNotFoundError

load_dotenv()

st.set_page_config(page_title="QBO CFO Dashboard", layout="wide")

DEFAULT_REDIRECT_URI = "http://localhost:8000/callback"


def get_secret(name: str, default: str = "") -> str:
    try:
        if name in st.secrets:
            return str(st.secrets[name]).strip()
    except StreamlitSecretNotFoundError:
        pass
    except Exception:
        pass

    return os.getenv(name, default).strip()


def require_secret(name: str) -> str:
    value = get_secret(name)
    if not value:
        raise ValueError(f"Missing required setting: {name}")
    return value


def get_base_url(environment: str) -> str:
    return (
        "https://sandbox-quickbooks.api.intuit.com"
        if environment == "sandbox"
        else "https://quickbooks.api.intuit.com"
    )


def refresh_tokens(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    redirect_uri: str,
    environment: str,
) -> Tuple[str, str]:
    auth_client = AuthClient(
        client_id=client_id,
        client_secret=client_secret,
        environment=environment,
        redirect_uri=redirect_uri,
    )
    auth_client.refresh(refresh_token)
    return auth_client.access_token, auth_client.refresh_token


def qbo_get(
    access_token: str,
    realm_id: str,
    environment: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    base_url = get_base_url(environment)
    url = f"{base_url}/v3/company/{realm_id}/{path}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    response = requests.get(url, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    return response.json()


def run_report(
    access_token: str,
    realm_id: str,
    environment: str,
    report_name: str,
    start_date: str,
    end_date: str,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params = {"start_date": start_date, "end_date": end_date}
    if extra_params:
        params.update(extra_params)
    return qbo_get(access_token, realm_id, environment, f"reports/{report_name}", params=params)


def query_entity(
    access_token: str,
    realm_id: str,
    environment: str,
    sql: str,
) -> Dict[str, Any]:
    return qbo_get(access_token, realm_id, environment, "query", {"query": sql})


def safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).replace(",", "").replace("$", "").strip()
    if text in ("", "-"):
        return None

    try:
        return float(text)
    except ValueError:
        return None


def flatten_rows(rows: List[Dict[str, Any]], parent_path: str = "") -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []

    for row in rows or []:
        row_type = row.get("type", "")
        header_cols = row.get("Header", {}).get("ColData", [])
        summary_cols = row.get("Summary", {}).get("ColData", [])
        leaf_cols = row.get("ColData", [])

        label = ""
        amount = None

        if header_cols:
            label = str(header_cols[0].get("value", "")).strip()
        elif leaf_cols:
            label = str(leaf_cols[0].get("value", "")).strip()

        if summary_cols:
            amount = summary_cols[-1].get("value")
        elif leaf_cols and len(leaf_cols) > 1:
            amount = leaf_cols[-1].get("value")

        current_path = f"{parent_path} > {label}" if parent_path and label else label or parent_path

        if label or amount is not None:
            output.append(
                {
                    "path": current_path,
                    "label": label,
                    "amount_raw": amount,
                    "amount": safe_float(amount),
                    "row_type": row_type,
                }
            )

        nested = row.get("Rows", {}).get("Row", [])
        if nested:
            output.extend(flatten_rows(nested, current_path))

    return output


def report_to_df(report_json: Dict[str, Any]) -> pd.DataFrame:
    rows = report_json.get("Rows", {}).get("Row", [])
    flat = flatten_rows(rows)

    if not flat:
        return pd.DataFrame(columns=["path", "label", "amount_raw", "amount", "row_type"])

    return pd.DataFrame(flat)


def find_metric(report_json: Dict[str, Any], labels: List[str]) -> Optional[float]:
    df = report_to_df(report_json)
    if df.empty:
        return None

    wanted = {x.lower() for x in labels}
    mask = df["label"].astype(str).str.strip().str.lower().isin(wanted)
    matches = df.loc[mask, "amount"].dropna()

    if matches.empty:
        return None

    return float(matches.iloc[0])


def aging_buckets_from_report(report_json: Dict[str, Any]) -> pd.DataFrame:
    rows = report_json.get("Rows", {}).get("Row", [])
    records: List[Dict[str, Any]] = []

    for row in rows:
        cols = row.get("ColData", [])
        if len(cols) < 6:
            continue

        name = cols[0].get("value")
        current = safe_float(cols[1].get("value")) or 0.0
        days_1_30 = safe_float(cols[2].get("value")) or 0.0
        days_31_60 = safe_float(cols[3].get("value")) or 0.0
        days_61_90 = safe_float(cols[4].get("value")) or 0.0
        days_91_plus = safe_float(cols[5].get("value")) or 0.0

        total = current + days_1_30 + days_31_60 + days_61_90 + days_91_plus

        if name and total:
            records.append(
                {
                    "Name": name,
                    "Current": current,
                    "1-30": days_1_30,
                    "31-60": days_31_60,
                    "61-90": days_61_90,
                    "91+": days_91_plus,
                    "Total": total,
                }
            )

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("Total", ascending=False)

    return df


def top_entities_from_report(
    report_json: Dict[str, Any],
    label_name: str = "Entity",
    top_n: int = 10,
) -> pd.DataFrame:
    rows = report_json.get("Rows", {}).get("Row", [])
    records: List[Dict[str, Any]] = []

    for row in rows:
        cols = row.get("ColData", [])
        if len(cols) < 2:
            continue

        name = cols[0].get("value")
        amount = safe_float(cols[-1].get("value"))

        if name and amount is not None:
            records.append({label_name: name, "Amount": amount})

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("Amount", ascending=False).head(top_n)

    return df


def pl_time_series(
    access_token: str,
    realm_id: str,
    environment: str,
    months_back: int = 6,
) -> pd.DataFrame:
    today = date.today().replace(day=1)
    records: List[Dict[str, Any]] = []

    for i in range(months_back - 1, -1, -1):
        y = today.year
        m = today.month - i

        while m <= 0:
            m += 12
            y -= 1

        month_first = date(y, m, 1)

        if m == 12:
            month_last = date(y + 1, 1, 1) - timedelta(days=1)
        else:
            month_last = date(y, m + 1, 1) - timedelta(days=1)

        report = run_report(
            access_token,
            realm_id,
            environment,
            "ProfitAndLoss",
            month_first.isoformat(),
            month_last.isoformat(),
        )

        records.append(
            {
                "Month": month_first.strftime("%Y-%m"),
                "Income": find_metric(report, ["Total Income", "Income"]),
                "Gross Profit": find_metric(report, ["Gross Profit"]),
                "Net Income": find_metric(report, ["Net Income"]),
            }
        )

    return pd.DataFrame(records)


@st.cache_data(ttl=3600, show_spinner=False)
def load_dashboard_data(
    client_id: str,
    client_secret: str,
    refresh_token: str,
    redirect_uri: str,
    realm_id: str,
    environment: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    access_token, new_refresh_token = refresh_tokens(
        client_id, client_secret, refresh_token, redirect_uri, environment
    )

    company_info = query_entity(access_token, realm_id, environment, "select * from CompanyInfo")
    profit_loss = run_report(access_token, realm_id, environment, "ProfitAndLoss", start_date, end_date)
    balance_sheet = run_report(access_token, realm_id, environment, "BalanceSheet", start_date, end_date)
    cash_flow = run_report(access_token, realm_id, environment, "CashFlow", start_date, end_date)
    ar_aging = run_report(access_token, realm_id, environment, "AgedReceivables", start_date, end_date)
    ap_aging = run_report(access_token, realm_id, environment, "AgedPayables", start_date, end_date)
    customer_sales = run_report(access_token, realm_id, environment, "CustomerSales", start_date, end_date)
    vendor_expenses = run_report(access_token, realm_id, environment, "VendorExpenses", start_date, end_date)
    monthly_trend = pl_time_series(access_token, realm_id, environment, months_back=6)

    company_name = (
        company_info.get("QueryResponse", {})
        .get("CompanyInfo", [{}])[0]
        .get("CompanyName", "Unknown Company")
    )

    kpis = {
        "Cash / Bank": find_metric(balance_sheet, ["Cash and cash equivalents", "Bank Accounts", "Cash"]),
        "Total Income": find_metric(profit_loss, ["Total Income", "Income"]),
        "Gross Profit": find_metric(profit_loss, ["Gross Profit"]),
        "Net Income": find_metric(profit_loss, ["Net Income"]),
        "Total Assets": find_metric(balance_sheet, ["Total Assets"]),
        "Total Liabilities": find_metric(balance_sheet, ["Total Liabilities"]),
        "Total Equity": find_metric(balance_sheet, ["Total Equity"]),
        "Operating Cash Flow": find_metric(
            cash_flow,
            ["Net cash provided by operating activities", "Net cash from operating activities"],
        ),
    }

    return {
        "company_name": company_name,
        "new_refresh_token": new_refresh_token,
        "kpis": kpis,
        "ar_df": aging_buckets_from_report(ar_aging),
        "ap_df": aging_buckets_from_report(ap_aging),
        "customer_sales_df": top_entities_from_report(customer_sales, "Customer", 10),
        "vendor_expenses_df": top_entities_from_report(vendor_expenses, "Vendor", 10),
        "monthly_trend_df": monthly_trend,
        "profit_loss_df": report_to_df(profit_loss),
        "balance_sheet_df": report_to_df(balance_sheet),
        "cash_flow_df": report_to_df(cash_flow),
    }


def metric_card(label: str, value: Optional[float]) -> None:
    if value is None:
        st.metric(label, "N/A")
    else:
        st.metric(label, f"${value:,.2f}")


st.title("CFO Dashboard")
st.caption("Uses local .env on Mac and st.secrets on Streamlit Cloud.")

with st.sidebar:
    st.header("Configuration")
    st.write("Credentials are loaded from .env locally or Streamlit secrets in the cloud.")

    environment = get_secret("ENVIRONMENT", "sandbox") or "sandbox"
    redirect_uri = get_secret("REDIRECT_URI", DEFAULT_REDIRECT_URI) or DEFAULT_REDIRECT_URI

    st.text_input("Environment", value=environment, disabled=True)
    st.text_input("Redirect URI", value=redirect_uri, disabled=True)

    st.header("Reporting Period")
    today = date.today()
    default_start = date(today.year, today.month, 1)

    start_date = st.date_input("Start date", value=default_start)
    end_date = st.date_input("End date", value=today)

    run_button = st.button("Load dashboard", type="primary")

st.info(
    "Expected keys: CLIENT_ID, CLIENT_SECRET, REALM_ID, REFRESH_TOKEN, REDIRECT_URI, and optionally ENVIRONMENT."
)

if run_button:
    try:
        client_id = require_secret("CLIENT_ID")
        client_secret = require_secret("CLIENT_SECRET")
        realm_id = require_secret("REALM_ID")
        refresh_token = require_secret("REFRESH_TOKEN")
        redirect_uri = require_secret("REDIRECT_URI")
        environment = get_secret("ENVIRONMENT", "sandbox") or "sandbox"

        data = load_dashboard_data(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            redirect_uri=redirect_uri,
            realm_id=realm_id,
            environment=environment,
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
        )

        st.success(f"Connected to {data['company_name']}")
        st.warning(
            "QuickBooks returned a new refresh token below. Update your .env or Streamlit secrets before the next run."
        )
        st.code(data["new_refresh_token"], language="text")

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            metric_card("Cash / Bank", data["kpis"]["Cash / Bank"])
        with c2:
            metric_card("Total Income", data["kpis"]["Total Income"])
        with c3:
            metric_card("Gross Profit", data["kpis"]["Gross Profit"])
        with c4:
            metric_card("Net Income", data["kpis"]["Net Income"])

        c5, c6, c7, c8 = st.columns(4)
        with c5:
            metric_card("Operating Cash Flow", data["kpis"]["Operating Cash Flow"])
        with c6:
            metric_card("Total Assets", data["kpis"]["Total Assets"])
        with c7:
            metric_card("Total Liabilities", data["kpis"]["Total Liabilities"])
        with c8:
            metric_card("Total Equity", data["kpis"]["Total Equity"])

        st.subheader("P&L Trend (Last 6 Months)")
        trend_df = data["monthly_trend_df"].set_index("Month")
        st.line_chart(trend_df[["Income", "Gross Profit", "Net Income"]])
        st.dataframe(data["monthly_trend_df"], use_container_width=True)

        left, right = st.columns(2)
        with left:
            st.subheader("Accounts Receivable Aging")
            ar_df = data["ar_df"]
            if ar_df.empty:
                st.write("No AR aging rows returned.")
            else:
                st.bar_chart(ar_df.set_index("Name")[["Current", "1-30", "31-60", "61-90", "91+"]])
                st.dataframe(ar_df, use_container_width=True)

        with right:
            st.subheader("Accounts Payable Aging")
            ap_df = data["ap_df"]
            if ap_df.empty:
                st.write("No AP aging rows returned.")
            else:
                st.bar_chart(ap_df.set_index("Name")[["Current", "1-30", "31-60", "61-90", "91+"]])
                st.dataframe(ap_df, use_container_width=True)

        left2, right2 = st.columns(2)
        with left2:
            st.subheader("Top Customers")
            customer_df = data["customer_sales_df"]
            if customer_df.empty:
                st.write("No customer sales rows returned.")
            else:
                st.bar_chart(customer_df.set_index("Customer"))
                st.dataframe(customer_df, use_container_width=True)

        with right2:
            st.subheader("Top Vendors")
            vendor_df = data["vendor_expenses_df"]
            if vendor_df.empty:
                st.write("No vendor expense rows returned.")
            else:
                st.bar_chart(vendor_df.set_index("Vendor"))
                st.dataframe(vendor_df, use_container_width=True)

        with st.expander("Detailed report rows"):
            tab1, tab2, tab3 = st.tabs(["Profit & Loss", "Balance Sheet", "Cash Flow"])
            with tab1:
                st.dataframe(data["profit_loss_df"], use_container_width=True)
            with tab2:
                st.dataframe(data["balance_sheet_df"], use_container_width=True)
            with tab3:
                st.dataframe(data["cash_flow_df"], use_container_width=True)

    except Exception as exc:
        st.error(f"Failed to load dashboard: {exc}")
        st.stop()
else:
    st.write("Click **Load dashboard** to use the credentials stored in your .env file.")
    st.markdown(
        """
Example `.env`:

```env
CLIENT_ID=your_client_id
CLIENT_SECRET=your_client_secret
REDIRECT_URI=http://localhost:8000/callback
ENVIRONMENT=sandbox
REFRESH_TOKEN=your_refresh_token
REALM_ID=your_realm_id
    """
)
