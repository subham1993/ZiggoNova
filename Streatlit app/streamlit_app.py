# streamlit_app.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import date, timedelta
import calendar
import matplotlib.pyplot as plt

DB_PATH = "earnings.db"

# ---------- Persistence ----------
@st.cache_resource
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS earnings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT NOT NULL,
            stock TEXT NOT NULL,
            amount REAL NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            y INTEGER NOT NULL,
            m INTEGER NOT NULL,
            target REAL NOT NULL,
            UNIQUE(y,m)
        )
        """
    )
    conn.commit()

    # Migration for old DBs
    cols = pd.read_sql_query("PRAGMA table_info(earnings)", conn)
    if "stock" not in cols["name"].tolist():
        conn.execute("ALTER TABLE earnings ADD COLUMN stock TEXT DEFAULT 'General'")
        conn.commit()
    return conn

conn = get_conn()

# ---------- Helpers ----------

def to_df(query, params=()):
    return pd.read_sql_query(query, conn, params=params)

def upsert_target(year: int, month: int, target: float):
    conn.execute(
        "INSERT INTO targets (y,m,target) VALUES (?,?,?)\n         ON CONFLICT(y,m) DO UPDATE SET target=excluded.target",
        (year, month, float(target)),
    )
    conn.commit()

def insert_earning(d: date, stock: str, amount: float):
    stock = (stock or "").strip() or "General"
    conn.execute(
        "INSERT INTO earnings (d, stock, amount) VALUES (?,?,?)",
        (d.isoformat(), stock, float(amount)),
    )
    conn.commit()

def delete_earning(entry_id: int):
    conn.execute("DELETE FROM earnings WHERE id=?", (entry_id,))
    conn.commit()

def month_bounds(year: int, month: int):
    first = date(year, month, 1)
    last = date(year, month, calendar.monthrange(year, month)[1])
    return first, last

def business_days_between(start: date, end: date) -> int:
    day_count = 0
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            day_count += 1
        cur += timedelta(days=1)
    return max(0, day_count)

# ---------- UI ----------
st.set_page_config(page_title="Monthly Stock Earnings Tracker", page_icon="💹", layout="wide")

# Small theme polish (metric value color)
st.markdown("""
<style>
div[data-testid='stMetricValue'] { font-weight: 700; }
</style>
""", unsafe_allow_html=True)

st.title("💹 Monthly Stock Earnings Tracker")

# ---- Sidebar: settings + add entry ----
with st.sidebar:
    st.header("Settings & Add Entry")
    today = date.today()
    col1, col2 = st.columns(2)
    with col1:
        year = st.number_input("Year", value=today.year, step=1, format="%d")
    with col2:
        month = st.number_input("Month", value=today.month, min_value=1, max_value=12, step=1)

    # Target controls
    tgt_df = to_df("SELECT target FROM targets WHERE y=? AND m=?", (int(year), int(month)))
    current_target = float(tgt_df["target"].iloc[0]) if not tgt_df.empty else 0.0
    new_target = st.number_input("Monthly target", value=float(current_target), step=100.0, format="%.2f")
    if st.button("Save target", key="save_target"):
        upsert_target(int(year), int(month), float(new_target))
        st.success("Target saved.")

    st.markdown("---")
    st.subheader("Add Earning Entry")
    with st.form("add_entry_form"):
        d = st.date_input("Date", value=today)
        stock = st.text_input("Stock name", placeholder="e.g., AAPL")
        amount = st.number_input("Earning for the day", value=0.0, step=10.0, format="%.2f")
        submitted = st.form_submit_button("Add")
    if submitted:
        insert_earning(d, stock, amount)
        st.success(f"Added {amount:+.2f} for {stock or 'General'} on {d.isoformat()}.")
        st.rerun()

# ---- Load month data once ----
first_day, last_day = month_bounds(int(year), int(month))
month_df = to_df(
    "SELECT id, d, stock, amount FROM earnings WHERE d BETWEEN ? AND ? ORDER BY d ASC, id ASC",
    (first_day.isoformat(), last_day.isoformat()),
)
if not month_df.empty:
    month_df["d"] = pd.to_datetime(month_df["d"]).dt.date

# ---- KPI section at top ----
st.subheader("Monthly Overview")
earned = float(month_df["amount"].sum()) if not month_df.empty else 0.0
remaining_amt = float(new_target - earned)
now = date.today()
clamped_today = min(max(now, first_day), last_day)
calendar_days_left = max(0, (last_day - clamped_today).days)
weekdays_left = business_days_between(clamped_today + timedelta(days=1), last_day)
use_weekdays = st.toggle("Use weekdays only for 'days left'", value=True, help="Toggle between weekdays or all days.")
base_days_left = weekdays_left if use_weekdays else calendar_days_left
required_per_day = remaining_amt / base_days_left if base_days_left > 0 else 0.0
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
with kpi1:
    st.metric("Target", f"{new_target:,.2f}")
with kpi2:
    st.metric("Earned so far", f"{earned:,.2f}")
with kpi3:
    st.metric("Remaining", f"{remaining_amt:,.2f}")
with kpi4:
    label_days = "weekdays" if use_weekdays else "days"
    st.metric(f"Days left ({label_days})", f"{base_days_left}")
with kpi5:
    st.metric("Needed per day", f"{required_per_day:,.2f}")

# ---- Progress feedback (simple) ----
if new_target > 0:
    if earned >= new_target:
        st.success("🎯 Goal achieved for this month! Great job.")
    elif earned >= 0.5 * new_target:
        st.info("Halfway there — you’ve reached at least 50% of your target.")

st.markdown("---")

# ---- Mini performance dashboard (daily bar + pie) ----
if not month_df.empty:
    perf1, perf2 = st.columns(2)
    with perf1:
        daily = month_df.groupby("d", as_index=False)["amount"].sum().rename(columns={"d": "Date", "amount": "Amount"})
        st.caption("Daily earnings (bar)")
        st.bar_chart(daily.set_index("Date")["Amount"], height=260)
    with perf2:
        by_stock = month_df.groupby("stock", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
        st.caption("Per-stock share (pie)")
        fig, ax = plt.subplots()
        # Avoid zero/negative total by taking absolute values for share display
        sizes = by_stock["amount"].abs()
        if sizes.sum() > 0:
            ax.pie(sizes, labels=by_stock["stock"], autopct="%1.0f%%")
        else:
            ax.text(0.5, 0.5, "No data", ha="center", va="center")
        ax.axis('equal')
        st.pyplot(fig, use_container_width=True)

st.markdown("---")

# ---- Entries table with checkbox delete ----
st.subheader("This month's entries")
if month_df.empty:
    st.info("No entries for this month yet.")
else:
    show_df = month_df.rename(columns={"d": "Date", "stock": "Stock", "amount": "Amount"})[["id", "Date", "Stock", "Amount"]]
    show_df = show_df.sort_values(["Date", "id"]).reset_index(drop=True)
    show_df["Delete"] = False

    edited = st.data_editor(
        show_df,
        use_container_width=True,
        column_config={
            "id": st.column_config.TextColumn("ID", disabled=True),
            "Date": st.column_config.DateColumn("Date", disabled=True),
            "Stock": st.column_config.TextColumn("Stock", disabled=True),
            "Amount": st.column_config.NumberColumn("Amount", disabled=True, format="%.2f"),
            "Delete": st.column_config.CheckboxColumn("Delete"),
        },
        key="delete_editor",
    )

    if st.button("Delete selected", key="delete_selected") and edited is not None:
        to_delete = edited[edited["Delete"] == True]["id"].tolist()
        if to_delete:
            for _id in to_delete:
                delete_earning(int(_id))
            st.success(f"Deleted {len(to_delete)} entr{'y' if len(to_delete)==1 else 'ies'}.")
            st.rerun()
        else:
            st.info("No rows were selected.")

# ---- Aggregations (single place) ----
if not month_df.empty:
    # Cumulative by day (line)
    agg = month_df.groupby("d", as_index=False)["amount"].sum()
    agg["cumulative"] = agg["amount"].cumsum()
    st.line_chart(agg.set_index("d")["cumulative"], height=250)

    # Per-stock totals table (only once)
    by_stock_tbl = month_df.groupby("stock", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
    st.subheader("Per-stock totals for the selected month")
    st.dataframe(by_stock_tbl.rename(columns={"stock": "Stock", "amount": "Total"}), use_container_width=True)

# ---- Monthly summary history (simple) ----
with st.expander("📅 Monthly summary history"):
    # List distinct months available in earnings
    months_df = to_df("SELECT DISTINCT substr(d,1,7) AS ym FROM earnings ORDER BY ym DESC")
    if months_df.empty:
        st.caption("No history yet.")
    else:
        ym_list = months_df["ym"].tolist()
        default_ym = f"{year:04d}-{month:02d}"
        sel_ym = st.selectbox("Pick a month", ym_list, index=ym_list.index(default_ym) if default_ym in ym_list else 0)
        y_sel, m_sel = map(int, sel_ym.split("-"))
        f, l = month_bounds(y_sel, m_sel)
        hist = to_df(
            "SELECT d, stock, amount FROM earnings WHERE d BETWEEN ? AND ?",
            (f.isoformat(), l.isoformat()),
        )
        if hist.empty:
            st.caption("No entries for that month.")
        else:
            total = float(hist["amount"].sum())
            num = int(len(hist))
            by_s = hist.groupby("stock", as_index=False)["amount"].sum()
            top = by_s.sort_values("amount", ascending=False).head(1)
            worst = by_s.sort_values("amount", ascending=True).head(1)
            c1, c2, c3, c4 = st.columns(4)
            with c1:
                st.metric("Total earned", f"{total:,.2f}")
            with c2:
                st.metric("# entries", f"{num}")
            with c3:
                st.metric("Top stock", top.iloc[0]["stock"] if not top.empty else "—")
            with c4:
                st.metric("Worst stock", worst.iloc[0]["stock"] if not worst.empty else "—")

# ---- Footer ----
st.caption("Built with Streamlit. Data is stored locally in earnings.db next to this script.")