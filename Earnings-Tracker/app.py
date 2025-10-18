# app.py
"""
Monthly Stock Earnings Tracker — Flask Edition (Render-ready)
- Simple authentication (single-user via env vars)
- Alternate pastel-glass UI
- No changes to data logic or routes' behavior

Quickstart (local)
------------------
1) pip install -r requirements.txt
2) (optional) set APP_USERNAME and either APP_PASSWORD or APP_PASSWORD_HASH
3) python app.py  # http://localhost:5000

Deploy to Render (free)
-----------------------
- Build: pip install -r requirements.txt
- Start: gunicorn app:app
- Add a free Render Postgres; set DATABASE_URL env var in the web service
"""

from __future__ import annotations

import os
import calendar
from datetime import date, datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Tuple

import pandas as pd
from flask import (
    Flask,
    flash,
    g,
    jsonify,
    redirect,
    render_template_string,
    request,
    session,
    url_for,
)
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from werkzeug.security import check_password_hash, generate_password_hash


# ---------- Config ----------
DB_PATH = os.environ.get("EARNINGS_DB", "earnings.db")
SECRET_KEY = os.environ.get("FLASK_SECRET_KEY", "dev-secret-change-me")
DATABASE_URL = os.environ.get("DATABASE_URL")  # Provided automatically by Render Postgres

# Auth config (single user). Default password is test123.
APP_USERNAME = os.environ.get("APP_USERNAME", "admin")
APP_PASSWORD_HASH = os.environ.get("APP_PASSWORD_HASH")  # preferred
APP_PASSWORD = os.environ.get("APP_PASSWORD")  # fallback (hashed at boot)

if not APP_PASSWORD_HASH:
    # If APP_PASSWORD provided, hash it; otherwise default to "test123"
    APP_PASSWORD_HASH = generate_password_hash(APP_PASSWORD if APP_PASSWORD else "test123")

app = Flask(__name__)
app.config.update(SECRET_KEY=SECRET_KEY)


# ---------- Auth Utilities ----------

def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user"):
            next_url = request.path
            if request.query_string:
                next_url += "?" + request.query_string.decode()
            return redirect(url_for("login", next=next_url))
        return fn(*args, **kwargs)

    return wrapper


def _do_login(username: str, password: str) -> bool:
    if username.strip().lower() != APP_USERNAME.strip().lower():
        return False
    return check_password_hash(APP_PASSWORD_HASH, password)


# ---------- Database Engine & Migration ----------

def get_engine() -> Engine:
    """Create (once per process) and return the SQLAlchemy engine.
    Uses Postgres when DATABASE_URL is set; otherwise SQLite file.
    """
    if "engine" not in g:
        if DATABASE_URL:
            g.engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        else:
            g.engine = create_engine(f"sqlite:///{DB_PATH}", pool_pre_ping=True)
        _run_migrations(g.engine)
    return g.engine


@app.teardown_appcontext
def close_engine(exception):
    g.pop("engine", None)


def _run_migrations(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS earnings (
                    id SERIAL PRIMARY KEY,
                    d TEXT NOT NULL,
                    stock TEXT NOT NULL,
                    amount REAL NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS targets (
                    id SERIAL PRIMARY KEY,
                    y INTEGER NOT NULL,
                    m INTEGER NOT NULL,
                    target REAL NOT NULL,
                    UNIQUE(y,m)
                )
                """
            )
        )
        try:
            conn.execute(
                text("ALTER TABLE earnings ADD COLUMN IF NOT EXISTS stock TEXT DEFAULT 'General'")
            )
        except Exception:
            pass


# ---------- Helpers ----------

def to_df(query: str, params: tuple | dict = ()):  # small util
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql_query(text(query), conn, params=params)


def upsert_target(year: int, month: int, target: float):
    eng = get_engine()
    stmt = text(
        """
        INSERT INTO targets (y,m,target) VALUES (:y,:m,:t)
        ON CONFLICT (y,m) DO UPDATE SET target = excluded.target
        """
    )
    with eng.begin() as conn:
        conn.execute(stmt, {"y": year, "m": month, "t": float(target)})


def insert_earning(d: date, stock: str, amount: float):
    stock = (stock or "").strip() or "General"
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(
            text("INSERT INTO earnings (d, stock, amount) VALUES (:d,:s,:a)"),
            {"d": d.isoformat(), "s": stock, "a": float(amount)},
        )


def delete_earning(entry_id: int):
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("DELETE FROM earnings WHERE id=:id"), {"id": entry_id})


def month_bounds(year: int, month: int) -> Tuple[date, date]:
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


# ---------- Templates (Alternate UI) ----------

BASE_HTML = r"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="Content-Security-Policy" content="default-src 'self' https: 'unsafe-inline' 'unsafe-eval'; img-src 'self' data: https:;">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Monthly Stock Earnings Tracker</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">
  <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;800&family=Space+Grotesk:wght@400;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {
      --bg: #f7f8fb;
      --glass: rgba(255,255,255,0.75);
      --border: rgba(0, 0, 0, 0.08);
      --shadow: 0 20px 40px rgba(31, 38, 135, 0.12);
      --accent: #5b7fff;
      --accent-2: #48d6b5;
      --text: #1e2a3a;
      --muted: #667085;
    }
    body {
      background:
        radial-gradient(1000px 600px at 0% 0%, #e8f1ff 0%, #f7f8fb 60%, transparent 60%),
        radial-gradient(1000px 800px at 100% 0%, #e6fff6 0%, #f7f8fb 55%, transparent 55%),
        linear-gradient(180deg, #ffffff, #f7f8fb);
      color: var(--text);
      font-family: 'Poppins', system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
      letter-spacing: .1px;
    }
    .navbar {
      background: linear-gradient(180deg, rgba(255,255,255,.85), rgba(255,255,255,.75)) !important;
      backdrop-filter: saturate(140%) blur(10px);
      border-bottom: 1px solid var(--border);
    }
    .brand {
      font-family: 'Space Grotesk', sans-serif;
      letter-spacing: .5px;
      color: var(--text);
    }
    .card-soft {
      border-radius: 18px;
      background: var(--glass);
      backdrop-filter: blur(10px);
      border: 1px solid var(--border);
      box-shadow: var(--shadow);
    }
    .metric { font-weight: 700; font-size: 1.02rem; text-transform: uppercase; }
    .metric span { font-weight: 800; font-size: 1.08rem; color: var(--accent); }
    .badge-chip {
      background: linear-gradient(90deg, rgba(91,127,255,.15), rgba(72,214,181,.15));
      border: 1px solid var(--border);
      color: #31456b;
    }
    .btn { border-radius: 14px; }
    .btn-primary {
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      border: none;
    }
    .btn-success {
      background: linear-gradient(90deg, #4bb543, #7ed957);
      border: none;
      color: #0b3212;
    }
    .btn-outline-danger { border-color: #f1416c; color: #f1416c; }
    .form-control, .form-select {
      background: rgba(255,255,255,0.85);
      border: 1px solid var(--border);
      color: var(--text);
      border-radius: 14px;
    }
    .form-control::placeholder { color: #98a0b3; }
    .table thead th { color: var(--muted); border-bottom-color: var(--border); }
    .table tbody tr { border-color: var(--border); }
    .alert { border-radius: 14px; }
    a, a:hover { color: #315efb; }
    .footnote { color: var(--muted); font-size: .9rem; }
  </style>
</head>
<body>
<nav class="navbar navbar-expand-lg mb-4">
  <div class="container">
    <span class="navbar-brand brand"><i class="bi bi-cash-coin"></i> Earnings Tracker</span>
    <div class="ms-auto d-flex align-items-center gap-3">
      <span class="badge badge-chip rounded-pill px-3"><i class="bi bi-shield-check"></i> Protected</span>
      {% if session.get('user') %}
        <span class="text-muted small">Signed in as <strong>{{ session.get('user') }}</strong></span>
        <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('logout') }}"><i class="bi bi-box-arrow-right"></i> Logout</a>
      {% endif %}
    </div>
  </div>
</nav>
<div class="container">
  {% with messages = get_flashed_messages() %}
    {% if messages %}
      <div class="alert alert-info card-soft">{{ messages[0] }}</div>
    {% endif %}
  {% endwith %}
  {{ body|safe }}
</div>
<script>
function post(path, params) {
  const form = document.createElement('form');
  form.method = 'POST';
  form.action = path;
  for (const key in params) {
    const input = document.createElement('input');
    input.type = 'hidden';
    input.name = key;
    input.value = params[key];
    form.appendChild(input);
  }
  document.body.appendChild(form);
  form.submit();
}
</script>
</body>
</html>
"""

LOGIN_HTML = r"""
<div class="row justify-content-center">
  <div class="col-md-6 col-lg-4">
    <div class="card-soft p-4">
      <h4 class="mb-3 brand text-center"><i class="bi bi-unlock"></i> Sign In</h4>
      <p class="text-muted text-center">Use your credentials to continue.</p>
      <form method="POST" action="{{ url_for('login', next=request.args.get('next','')) }}">
        <div class="mb-3">
          <label class="form-label">Username</label>
          <input class="form-control" name="username" placeholder="e.g., admin" autocomplete="username" required>
        </div>
        <div class="mb-2">
          <label class="form-label">Password</label>
          <input class="form-control" type="password" name="password" placeholder="••••••••" autocomplete="current-password" required>
        </div>
        <button class="btn btn-primary w-100 mt-2"><i class="bi bi-box-arrow-in-right"></i> Sign in</button>
      </form>
      <p class="footnote mt-3"><i class="bi bi-info-circle"></i> Default password is <code>test123</code>. Override via <code>APP_PASSWORD</code> or <code>APP_PASSWORD_HASH</code> in production.</p>
    </div>
  </div>
</div>
"""

INDEX_BODY = r"""
<div class="row g-4">
  <div class="col-lg-3">
    <div class="card card-soft p-3">
      <h5 class="mb-3"><i class="bi bi-sliders2-vertical"></i> Settings & Add Entry</h5>
      <form class="mb-3" method="GET">
        <div class="row g-2">
          <div class="col-6">
            <label class="form-label">Year</label>
            <input class="form-control" type="number" name="year" value="{{ year }}" required>
          </div>
          <div class="col-6">
            <label class="form-label">Month</label>
            <input class="form-control" type="number" name="month" min="1" max="12" value="{{ month }}" required>
          </div>
        </div>
        <div class="form-text">Press Enter to switch month</div>
      </form>

      <form class="mb-3" method="POST" action="{{ url_for('save_target') }}">
        <label class="form-label">Monthly target</label>
        <input class="form-control" type="number" step="0.01" name="target" value="{{ current_target }}">
        <input type="hidden" name="year" value="{{ year }}">
        <input type="hidden" name="month" value="{{ month }}">
        <button class="btn btn-primary w-100 mt-2"><i class="bi bi-save2"></i> Save target</button>
      </form>

      <hr>
      <h6><i class="bi bi-plus-circle"></i> Add Earning Entry</h6>
      <form method="POST" action="{{ url_for('add_entry') }}">
        <label class="form-label">Date</label>
        <input class="form-control" type="date" name="d" value="{{ today_iso }}" required>
        <label class="form-label mt-2">Stock name</label>
        <input class="form-control" type="text" name="stock" placeholder="e.g., AAPL">
        <label class="form-label mt-2">Earning for the day</label>
        <input class="form-control" type="number" step="0.01" name="amount" value="0.00">
        <button class="btn btn-success w-100 mt-3"><i class="bi bi-check2-circle"></i> Add</button>
      </form>
    </div>
  </div>

  <div class="col-lg-9">
    <div class="card card-soft p-3 mb-3">
      <div class="d-flex align-items-center justify-content-between">
        <h5 class="mb-3"><i class="bi bi-speedometer"></i> Monthly Overview</h5>
        <span class="badge badge-chip rounded-pill px-3"><i class="bi bi-calendar-event"></i> {{ year }}-{{ '%02d'|format(month) }}</span>
      </div>
      <div class="row text-center">
        <div class="col"><div class="metric">Target<br><span>{{ fmt(new_target) }}</span></div></div>
        <div class="col"><div class="metric">Earned so far<br><span>{{ fmt(earned) }}</span></div></div>
        <div class="col"><div class="metric">Remaining<br><span>{{ fmt(remaining_amt) }}</span></div></div>
        <div class="col"><div class="metric">Days left ({{ 'weekdays' if use_weekdays else 'days' }})<br><span>{{ base_days_left }}</span></div></div>
        <div class="col"><div class="metric">Needed per day<br><span>{{ fmt(required_per_day) }}</span></div></div>
      </div>
      {% if new_target > 0 %}
        {% if earned >= new_target %}
          <div class="alert alert-success mt-3"><i class="bi bi-trophy"></i> 🎯 Goal achieved for this month! Great job.</div>
        {% elif earned >= 0.5 * new_target %}
          <div class="alert alert-info mt-3"><i class="bi bi-rocket-takeoff"></i> Halfway there — you’ve reached at least 50% of your target.</div>
        {% endif %}
      {% endif %}
      <form class="form-check form-switch mt-2" method="GET">
        <input type="hidden" name="year" value="{{ year }}">
        <input type="hidden" name="month" value="{{ month }}">
        <input class="form-check-input" type="checkbox" role="switch" id="use_weekdays" name="use_weekdays" value="1" {% if use_weekdays %}checked{% endif %} onchange="this.form.submit()">
        <label class="form-check-label" for="use_weekdays">Use weekdays only for 'days left'</label>
      </form>
    </div>

    <div class="row g-3">
      <div class="col-md-6">
        <div class="card card-soft p-3">
          <div class="small text-muted">Daily earnings (bar)</div>
          <canvas id="dailyBar" height="220"></canvas>
        </div>
      </div>
      <div class="col-md-6">
        <div class="card card-soft p-3">
          <div class="small text-muted">Per-stock share (pie)</div>
          <canvas id="stockPie" height="220"></canvas>
        </div>
      </div>
    </div>

    <div class="card card-soft p-3 mt-3">
      <h5><i class="bi bi-table"></i> This month's entries</h5>
      {% if month_rows|length == 0 %}
        <div class="alert alert-light">No entries for this month yet.</div>
      {% else %}
        <form method="POST" action="{{ url_for('delete_selected') }}">
          <input type="hidden" name="year" value="{{ year }}">
          <input type="hidden" name="month" value="{{ month }}">
          <div class="table-responsive">
            <table class="table table-sm align-middle">
              <thead>
                <tr>
                  <th>Delete</th>
                  <th>ID</th>
                  <th>Date</th>
                  <th>Stock</th>
                  <th class="text-end">Amount</th>
                </tr>
              </thead>
              <tbody>
                {% for r in month_rows %}
                <tr>
                  <td><input class="form-check-input" type="checkbox" name="del" value="{{ r['id'] }}"></td>
                  <td>{{ r['id'] }}</td>
                  <td>{{ r['d'] }}</td>
                  <td>{{ r['stock'] }}</td>
                  <td class="text-end">{{ fmt(r['amount']) }}</td>
                </tr>
                {% endfor %}
              </tbody>
            </table>
          </div>
          <button class="btn btn-outline-danger"><i class="bi bi-trash3"></i> Delete selected</button>
        </form>

        <hr>
        <div class="small text-muted mb-2">Cumulative by day (line)</div>
        <canvas id="cumLine" height="220"></canvas>

        <h5 class="mt-4"><i class="bi bi-pie-chart"></i> Per-stock totals for the selected month</h5>
        <div class="table-responsive">
          <table class="table table-sm">
            <thead>
              <tr>
                <th>Stock</th>
                <th class="text-end">Total</th>
              </tr>
            </thead>
            <tbody>
              {% for s in per_stock %}
              <tr>
                <td>{{ s['stock'] }}</td>
                <td class="text-end">{{ fmt(s['total']) }}</td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      {% endif %}
    </div>

    <div class="card card-soft p-3 mt-3">
      <details>
        <summary>📅 Monthly summary history</summary>
        {% if ym_list|length == 0 %}
          <div class="text-muted small">No history yet.</div>
        {% else %}
          <form class="row g-2 mt-2" method="GET">
            <div class="col-6 col-md-4">
              <label class="form-label">Pick a month</label>
              <select class="form-select" name="sel_ym" onchange="const [y,m]=this.value.split('-'); this.form.year.value=y; this.form.month.value=m; this.form.submit();">
                {% for ym in ym_list %}
                  <option value="{{ ym }}" {% if ym==default_ym %}selected{% endif %}>{{ ym }}</option>
                {% endfor %}
              </select>
            </div>
            <input type="hidden" name="year" value="{{ year }}">
            <input type="hidden" name="month" value="{{ month }}">
          </form>
          {% if summary %}
          <div class="row text-center mt-3">
            <div class="col"><div class="metric">Total earned<br><span>{{ fmt(summary.total) }}</span></div></div>
            <div class="col"><div class="metric"># entries<br><span>{{ summary.num }}</span></div></div>
            <div class="col"><div class="metric">Top stock<br><span>{{ summary.top or '—' }}</span></div></div>
            <div class="col"><div class="metric">Worst stock<br><span>{{ summary.worst or '—' }}</span></div></div>
          </div>
          {% endif %}
        {% endif %}
      </details>
    </div>
  </div>
</div>

<script>
async function loadCharts() {
  const resp = await fetch('{{ url_for('month_data_api') }}?year={{ year }}&month={{ month }}');
  const data = await resp.json();

  const commonOpts = {
    responsive: true,
    plugins: { legend: { labels: { color: '#3b4960' } } },
    scales: {
      x: { ticks: { color: '#3b4960' }, grid: { color: 'rgba(0,0,0,0.06)' } },
      y: { ticks: { color: '#3b4960' }, grid: { color: 'rgba(0,0,0,0.06)' } }
    }
  };

  new Chart(document.getElementById('dailyBar'), {
    type: 'bar',
    data: { labels: data.daily.labels, datasets: [{ label: 'Amount', data: data.daily.values, borderWidth: 1 }] },
    options: commonOpts
  });

  new Chart(document.getElementById('stockPie'), {
    type: 'pie',
    data: { labels: data.by_stock.labels, datasets: [{ data: data.by_stock.values }] },
    options: { responsive: true, plugins: { legend: { position: 'bottom', labels: { color: '#3b4960' } } } }
  });

  new Chart(document.getElementById('cumLine'), {
    type: 'line',
    data: { labels: data.cumulative.labels, datasets: [{ label: 'Cumulative', data: data.cumulative.values, tension: .35, pointRadius: 2 }] },
    options: commonOpts
  });
}
loadCharts();
</script>
"""


# ---------- Views ----------

@app.get("/login")
def login():
    if session.get("user"):
        return redirect(request.args.get("next") or url_for("index"))
    body = render_template_string(LOGIN_HTML)
    return render_template_string(BASE_HTML, body=body)


@app.post("/login")
def login_post():
    username = request.form.get("username", "")
    password = request.form.get("password", "")
    if _do_login(username, password):
        session["user"] = APP_USERNAME
        flash("Signed in successfully.")
        return redirect(request.args.get("next") or url_for("index"))
    flash("Invalid credentials.")
    return redirect(url_for("login", next=request.args.get("next", "")))


@app.get("/logout")
def logout():
    session.pop("user", None)
    flash("You have been signed out.")
    return redirect(url_for("login"))


@app.route("/")
@login_required
def index():
    today = date.today()
    year = int(request.args.get("year", today.year))
    month = int(request.args.get("month", today.month))

    use_weekdays = request.args.get("use_weekdays", "1") == "1"

    tgt_df = to_df("SELECT target FROM targets WHERE y=:y AND m=:m", {"y": year, "m": month})
    current_target = float(tgt_df["target"].iloc[0]) if not tgt_df.empty else 0.0

    first_day, last_day = month_bounds(year, month)
    month_df = to_df(
        "SELECT id, d, stock, amount FROM earnings WHERE d BETWEEN :a AND :b ORDER BY d ASC, id ASC",
        {"a": first_day.isoformat(), "b": last_day.isoformat()},
    )
    if not month_df.empty:
        month_df["d"] = pd.to_datetime(month_df["d"]).dt.date

    earned = float(month_df["amount"].sum()) if not month_df.empty else 0.0
    new_target = current_target
    remaining_amt = float(new_target - earned)

    now = date.today()
    clamped_today = min(max(now, first_day), last_day)
    calendar_days_left = max(0, (last_day - clamped_today).days)
    weekdays_left = business_days_between(clamped_today + timedelta(days=1), last_day)
    base_days_left = weekdays_left if use_weekdays else calendar_days_left
    required_per_day = (remaining_amt / base_days_left) if base_days_left > 0 else 0.0

    month_rows: List[Dict[str, Any]] = []
    if not month_df.empty:
        for _, r in month_df.iterrows():
            month_rows.append(
                {
                    "id": int(r["id"]),
                    "d": r["d"].isoformat() if isinstance(r["d"], date) else str(r["d"]),
                    "stock": str(r["stock"]),
                    "amount": float(r["amount"]),
                }
            )

    per_stock: List[Dict[str, Any]] = []
    if not month_df.empty:
        by_stock = (
            month_df.groupby("stock", as_index=False)["amount"].sum().sort_values("amount", ascending=False)
        )
        for _, r in by_stock.iterrows():
            per_stock.append({"stock": r["stock"], "total": float(r["amount"])})

    months_df = to_df("SELECT DISTINCT substr(d,1,7) AS ym FROM earnings ORDER BY ym DESC")
    ym_list = months_df["ym"].tolist() if not months_df.empty else []
    default_ym = f"{year:04d}-{month:02d}"

    summary = None
    if default_ym in ym_list:
        y_sel, m_sel = map(int, default_ym.split("-"))
        f, l = month_bounds(y_sel, m_sel)
        hist = to_df(
            "SELECT d, stock, amount FROM earnings WHERE d BETWEEN :a AND :b",
            {"a": f.isoformat(), "b": l.isoformat()},
        )
        if not hist.empty:
            total = float(hist["amount"].sum())
            num = int(len(hist))
            by_s = hist.groupby("stock", as_index=False)["amount"].sum()
            top = by_s.sort_values("amount", ascending=False).head(1)
            worst = by_s.sort_values("amount", ascending=True).head(1)
            summary = type(
                "S",
                (),
                {
                    "total": total,
                    "num": num,
                    "top": top.iloc[0]["stock"] if not top.empty else None,
                    "worst": worst.iloc[0]["stock"] if not worst.empty else None,
                },
            )

    def fmt(x: float) -> str:
        return f"{x:,.2f}"

    body = render_template_string(
        INDEX_BODY,
        year=year,
        month=month,
        today_iso=today.isoformat(),
        current_target=current_target,
        new_target=new_target,
        earned=earned,
        remaining_amt=remaining_amt,
        base_days_left=base_days_left,
        required_per_day=required_per_day,
        use_weekdays=use_weekdays,
        month_rows=month_rows,
        per_stock=per_stock,
        ym_list=ym_list,
        default_ym=default_ym,
        summary=summary,
        fmt=fmt,
    )

    return render_template_string(BASE_HTML, body=body)


@app.post("/save_target")
@login_required
def save_target():
    year = int(request.form["year"])
    month = int(request.form["month"])
    target = float(request.form.get("target", 0.0))
    upsert_target(year, month, target)
    flash("Target saved.")
    return redirect(url_for("index", year=year, month=month))


@app.post("/add_entry")
@login_required
def add_entry():
    d = request.form.get("d") or date.today().isoformat()
    stock = request.form.get("stock", "").strip()
    amount = float(request.form.get("amount", 0.0))
    insert_earning(datetime.fromisoformat(d).date(), stock, amount)
    flash(f"Added {amount:+.2f} for {stock or 'General'} on {d}.")
    year = int(request.args.get("year", datetime.fromisoformat(d).year))
    month = int(request.args.get("month", datetime.fromisoformat(d).month))
    return redirect(url_for("index", year=year, month=month))


@app.post("/delete_selected")
@login_required
def delete_selected():
    year = int(request.form.get("year"))
    month = int(request.form.get("month"))
    ids = request.form.getlist("del")
    if ids:
        for _id in ids:
            delete_earning(int(_id))
        flash(f"Deleted {len(ids)} entr{'y' if len(ids)==1 else 'ies' }.")
    else:
        flash("No rows were selected.")
    return redirect(url_for("index", year=year, month=month))


@app.get("/api/month_data")
@login_required
def month_data_api():
    year = int(request.args.get("year"))
    month = int(request.args.get("month"))

    first_day, last_day = month_bounds(year, month)
    df = to_df(
        "SELECT d, stock, amount FROM earnings WHERE d BETWEEN :a AND :b ORDER BY d ASC",
        {"a": first_day.isoformat(), "b": last_day.isoformat()},
    )

    if df.empty:
        return jsonify(
            {
                "daily": {"labels": [], "values": []},
                "by_stock": {"labels": [], "values": []},
                "cumulative": {"labels": [], "values": []},
            }
        )

    df["d"] = pd.to_datetime(df["d"]).dt.date

    daily = df.groupby("d", as_index=False)["amount"].sum()
    daily_labels = [d.isoformat() for d in daily["d"].tolist()]
    daily_values = [float(x) for x in daily["amount"].tolist()]

    by_stock = df.groupby("stock", as_index=False)["amount"].sum()
    by_labels = by_stock["stock"].tolist()
    by_values = [abs(float(x)) for x in by_stock["amount"].tolist()]

    cum = daily.copy()
    cum["cumulative"] = cum["amount"].cumsum()
    cum_labels = daily_labels
    cum_values = [float(x) for x in cum["cumulative"].tolist()]

    return jsonify(
        {
            "daily": {"labels": daily_labels, "values": daily_values},
            "by_stock": {"labels": by_labels, "values": by_values},
            "cumulative": {"labels": cum_labels, "values": cum_values},
        }
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
