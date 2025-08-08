#!/usr/bin/env python3
"""
Multi-company accounting app (single-file)
Adds:
 - Company fiscal year start (month/day) and currency
 - Company-specific Profit & Loss and Balance Sheet
 - Export/backup per company as a ZIP of CSVs
Keeps:
 - Chart of Accounts, Vouchers, Ledger, Trial Balance, CSV exports
 - Session-persistent company selection
"""
from flask import Flask, render_template_string, request, redirect, url_for, flash, Response, session, send_file
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, and_
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import io, csv, zipfile
from dateutil.parser import parse as dateparse

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev-secret-key-multi-company-reports'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///accounts_multi_reports.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# ---------- Models ----------
class Company(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), unique=True, nullable=False)
    address = db.Column(db.String(255))
    # fiscal year start (month 1-12, day 1-31)
    fy_start_month = db.Column(db.Integer, nullable=False, default=4)  # default April 1
    fy_start_day = db.Column(db.Integer, nullable=False, default=1)
    currency = db.Column(db.String(10), nullable=False, default='INR')

    def __repr__(self):
        return f"<Company {self.name}>"

class Account(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('company.id'), nullable=False)
    company = db.relationship('Company')
    name = db.Column(db.String(120), nullable=False)
    group = db.Column(db.String(80), nullable=True)  # e.g., Assets, Liabilities, Income, Expense, Equity

    __table_args__ = (db.UniqueConstraint('company_id', 'name', name='uix_company_account_name'),)

    def __repr__(self):
        return f"<Account {self.name} ({self.company.name if self.company else 'NoCo'})>"

class Voucher(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('company.id'), nullable=False)
    company = db.relationship('Company')
    date = db.Column(db.Date, default=date.today, nullable=False)
    narration = db.Column(db.String(255), nullable=True)
    lines = db.relationship('VoucherLine', backref='voucher', cascade='all, delete-orphan')

class VoucherLine(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    voucher_id = db.Column(db.Integer, db.ForeignKey('voucher.id'), nullable=False)
    company_id = db.Column(db.Integer, db.ForeignKey('company.id'), nullable=False)
    company = db.relationship('Company')
    account_id = db.Column(db.Integer, db.ForeignKey('account.id'), nullable=False)
    account = db.relationship('Account')
    amount = db.Column(db.Numeric(14,2), nullable=False)  # positive amounts
    is_debit = db.Column(db.Boolean, nullable=False)  # True if debit, False if credit
    narration = db.Column(db.String(255), nullable=True)

# ---------- Helpers ----------
def to_decimal(value):
    return Decimal(value).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

def init_db():
    db.create_all()
    # create default company and accounts if none exist
    if Company.query.count() == 0:
        default_co = Company(name='Default Company', address='Your Address', fy_start_month=4, fy_start_day=1, currency='INR')
        db.session.add(default_co)
        db.session.flush()
        defaults = [
            ('Cash','Assets'),
            ('Bank','Assets'),
            ('Sundry Debtors','Assets'),
            ('Sundry Creditors','Liabilities'),
            ('Capital','Equity'),
            ('Sales','Income'),
            ('Purchase','Expense'),
            ('Rent','Expense'),
            ('GST Payable','Liabilities'),
        ]
        for name, group in defaults:
            db.session.add(Account(company_id=default_co.id, name=name, group=group))
        db.session.commit()

# ---------- Company selection helpers ----------
def get_current_company():
    cid = session.get('company_id')
    if cid:
        co = Company.query.get(cid)
        if co:
            return co
    # if only one company exists, auto-select it
    companies = Company.query.order_by(Company.name).all()
    if len(companies) == 1:
        session['company_id'] = companies[0].id
        return companies[0]
    return None

NO_COMPANY_REQUIRED = set([
    'index', 'companies', 'add_company', 'select_company', 'set_company'
])

@app.before_request
def ensure_company_selected():
    ep = (request.endpoint or '')
    if ep in NO_COMPANY_REQUIRED or ep.startswith('static'):
        return
    co = get_current_company()
    if not co:
        return redirect(url_for('select_company'))

# Fiscal year range for a company for a reference date
def get_fiscal_year_dates(company: Company, ref_date: date = None):
    if not ref_date:
        ref_date = date.today()
    ms = int(company.fy_start_month)
    ds = int(company.fy_start_day)
    # Build candidate start date in current calendar year
    try:
        start_this_year = date(ref_date.year, ms, ds)
    except ValueError:
        # handle invalid day (e.g., Feb 30) — use last day of month
        # compute by trying next month day 1 minus 1
        if ms == 12:
            next_month = date(ref_date.year+1,1,1)
        else:
            next_month = date(ref_date.year, ms+1, 1)
        start_this_year = next_month - timedelta(days=1)
    if ref_date >= start_this_year:
        start = start_this_year
        end = date(start.year+1, ms, ds) - timedelta(days=1)
    else:
        # fiscal start was last calendar year
        try:
            start = date(ref_date.year-1, ms, ds)
        except ValueError:
            if ms == 12:
                next_month = date(ref_date.year,1,1)
            else:
                next_month = date(ref_date.year-1, ms+1, 1)
            start = next_month - timedelta(days=1)
        end = date(start.year+1, ms, ds) - timedelta(days=1)
    return start, end

# Balance for an account up to an optional date
def get_account_balance(account_id, company_id, up_to_date=None):
    q = db.session.query(
        func.coalesce(func.sum(VoucherLine.amount).filter(VoucherLine.account_id==account_id, VoucherLine.is_debit==True, VoucherLine.company_id==company_id), 0).label('dr'),
        func.coalesce(func.sum(VoucherLine.amount).filter(VoucherLine.account_id==account_id, VoucherLine.is_debit==False, VoucherLine.company_id==company_id), 0).label('cr')
    )
    # The above doesn't support date filtering directly. We'll do explicit sums with joins for date.
    if up_to_date:
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==account_id, VoucherLine.is_debit==True, VoucherLine.company_id==company_id, Voucher.date <= up_to_date
        ).scalar()
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==account_id, VoucherLine.is_debit==False, VoucherLine.company_id==company_id, Voucher.date <= up_to_date
        ).scalar()
    else:
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).filter(
            VoucherLine.account_id==account_id, VoucherLine.is_debit==True, VoucherLine.company_id==company_id
        ).scalar()
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).filter(
            VoucherLine.account_id==account_id, VoucherLine.is_debit==False, VoucherLine.company_id==company_id
        ).scalar()
    dr = Decimal(dr or 0)
    cr = Decimal(cr or 0)
    return dr - cr  # positive => net debit

# ---------- Templates ----------
BASE_TMPL = """
<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Simple Accounts (Multi-Company)</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
  </head>
  <body class="bg-light">
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark mb-4">
      <div class="container-fluid">
        <a class="navbar-brand" href="{{ url_for('index') }}">Simple Accounts</a>
        <div class="collapse navbar-collapse">
          <ul class="navbar-nav me-auto">
            {% if current_company %}
              <li class="nav-item"><a class="nav-link" href="{{ url_for('accounts') }}">Chart of Accounts</a></li>
              <li class="nav-item"><a class="nav-link" href="{{ url_for('create_voucher') }}">New Voucher</a></li>
              <li class="nav-item"><a class="nav-link" href="{{ url_for('vouchers') }}">Vouchers</a></li>
              <li class="nav-item"><a class="nav-link" href="{{ url_for('trial_balance') }}">Trial Balance</a></li>
              <li class="nav-item"><a class="nav-link" href="{{ url_for('pnl') }}">Profit & Loss</a></li>
              <li class="nav-item"><a class="nav-link" href="{{ url_for('balance_sheet') }}">Balance Sheet</a></li>
              <li class="nav-item"><a class="nav-link" href="{{ url_for('backup_company') }}">Export / Backup</a></li>
            {% endif %}
            <li class="nav-item"><a class="nav-link" href="{{ url_for('companies') }}">Companies</a></li>
          </ul>

          <span class="navbar-text text-white me-3">
            {% if current_company %}
              Company: <strong>{{ current_company.name }}</strong> &nbsp; ({{ current_company.currency }})
            {% else %}
              <em>No company selected</em>
            {% endif %}
          </span>

          {% if current_company %}
            <a class="btn btn-sm btn-outline-light me-2" href="{{ url_for('select_company') }}">Switch Company</a>
          {% else %}
            <a class="btn btn-sm btn-outline-light me-2" href="{{ url_for('select_company') }}">Select Company</a>
          {% endif %}
        </div>
      </div>
    </nav>

    <div class="container">
      {% with messages = get_flashed_messages(with_categories=true) %}
        {% if messages %}
          {% for cat, msg in messages %}
            <div class="alert alert-{{cat}}">{{ msg }}</div>
          {% endfor %}
        {% endif %}
      {% endwith %}

      {{ content }}
    </div>
  </body>
</html>
"""

# ---------- Routes ----------
@app.route('/')
def index():
    current_company = get_current_company()
    content = """
    <div class="p-4 bg-white rounded shadow-sm">
      <h3>Welcome</h3>
      {% if current_company %}
        <p>Active Company: <strong>{{ current_company.name }}</strong> (Currency: {{ current_company.currency }})</p>
        <p>FY Start: {{ current_company.fy_start_month }}/{{ current_company.fy_start_day }}</p>
        <p>Use the menu to manage accounts, vouchers and reports for the selected company.</p>
        <p>
          <a class="btn btn-primary" href="{{ url_for('accounts') }}">Chart of Accounts</a>
          <a class="btn btn-primary" href="{{ url_for('create_voucher') }}">Create Voucher</a>
          <a class="btn btn-secondary" href="{{ url_for('trial_balance') }}">Trial Balance</a>
        </p>
      {% else %}
        <p>No company selected. Create or open a company to begin.</p>
        <p><a class="btn btn-primary" href="{{ url_for('companies') }}">Companies</a></p>
      {% endif %}
    </div>
    """
    return render_template_string(BASE_TMPL, content=content, current_company=current_company)

# --- Company management ---
@app.route('/companies')
def companies():
    companies = Company.query.order_by(Company.name).all()
    current_company = get_current_company()
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Companies <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('add_company') }}">+ Add</a></h4>
        <table class="table table-sm mt-2">
          <thead><tr><th>Name</th><th>Address</th><th>FY Start</th><th>Currency</th><th></th></tr></thead>
          <tbody>
            {% for c in companies %}
              <tr>
                <td>{{ c.name }}</td>
                <td>{{ c.address or '' }}</td>
                <td>{{ c.fy_start_month }}/{{ c.fy_start_day }}</td>
                <td>{{ c.currency }}</td>
                <td>
                  {% if current_company and current_company.id == c.id %}
                    <span class="badge bg-success">Active</span>
                  {% else %}
                    <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('set_company', company_id=c.id) }}">Open</a>
                  {% endif %}
                </td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    """, companies=companies, current_company=current_company)
    return render_template_string(BASE_TMPL, content=content, current_company=current_company)

@app.route('/companies/add', methods=['GET','POST'])
def add_company():
    if request.method == 'POST':
        name = request.form.get('name','').strip()
        addr = request.form.get('address','').strip()
        fy_month = int(request.form.get('fy_month', 4))
        fy_day = int(request.form.get('fy_day', 1))
        currency = request.form.get('currency','INR').strip()
        if not name:
            flash('Company name required','danger')
        else:
            if Company.query.filter(func.lower(Company.name)==name.lower()).first():
                flash('Company name already exists','danger')
            else:
                co = Company(name=name, address=addr, fy_start_month=fy_month, fy_start_day=fy_day, currency=currency)
                db.session.add(co)
                db.session.commit()
                # create some default accounts for this company
                defaults = [
                    ('Cash','Assets'),
                    ('Bank','Assets'),
                    ('Sundry Debtors','Assets'),
                    ('Sundry Creditors','Liabilities'),
                    ('Capital','Equity'),
                    ('Sales','Income'),
                    ('Purchase','Expense'),
                    ('Rent','Expense'),
                    ('GST Payable','Liabilities'),
                ]
                for n,g in defaults:
                    db.session.add(Account(company_id=co.id, name=n, group=g))
                db.session.commit()
                flash('Company created and opened', 'success')
                session['company_id'] = co.id
                return redirect(url_for('index'))
    current_company = get_current_company()
    content = """
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Add Company</h4>
        <form method="post">
          <div class="mb-3">
            <label class="form-label">Company Name</label>
            <input name="name" class="form-control">
          </div>
          <div class="mb-3">
            <label class="form-label">Address</label>
            <input name="address" class="form-control">
          </div>
          <div class="row">
            <div class="col-md-3 mb-3">
              <label>FY Start Month (1-12)</label>
              <input class="form-control" name="fy_month" value="4">
            </div>
            <div class="col-md-3 mb-3">
              <label>FY Start Day (1-31)</label>
              <input class="form-control" name="fy_day" value="1">
            </div>
            <div class="col-md-3 mb-3">
              <label>Currency</label>
              <input class="form-control" name="currency" value="INR">
            </div>
          </div>
          <button class="btn btn-primary">Create & Open</button>
          <a class="btn btn-secondary" href="{{ url_for('companies') }}">Cancel</a>
        </form>
      </div>
    """
    return render_template_string(BASE_TMPL, content=content, current_company=current_company)

@app.route('/companies/select')
def select_company():
    companies = Company.query.order_by(Company.name).all()
    current_company = get_current_company()
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Select Company</h4>
        <p>Choose the company you want to open for this browser session.</p>
        <table class="table table-sm mt-2">
          <thead><tr><th>Name</th><th>Address</th><th>FY Start</th><th>Currency</th><th></th></tr></thead>
          <tbody>
            {% for c in companies %}
              <tr>
                <td>{{ c.name }}</td>
                <td>{{ c.address or '' }}</td>
                <td>{{ c.fy_start_month }}/{{ c.fy_start_day }}</td>
                <td>{{ c.currency }}</td>
                <td>
                  <a class="btn btn-sm btn-primary" href="{{ url_for('set_company', company_id=c.id) }}">Open</a>
                </td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
        <p><a class="btn btn-outline-secondary" href="{{ url_for('add_company') }}">+ Create new company</a></p>
      </div>
    """, companies=companies, current_company=current_company)
    return render_template_string(BASE_TMPL, content=content, current_company=current_company)

@app.route('/companies/set/<int:company_id>')
def set_company(company_id):
    co = Company.query.get_or_404(company_id)
    session['company_id'] = co.id
    flash(f'Opened company: {co.name}', 'success')
    return redirect(url_for('index'))

# --- Accounts (same as before) ---
@app.route('/accounts')
def accounts():
    co = get_current_company()
    accts = Account.query.filter_by(company_id=co.id).order_by(Account.name).all()
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Chart of Accounts ({{ co.name }}) <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('add_account') }}">+ Add</a></h4>
        <table class="table table-sm mt-2">
          <thead><tr><th>Name</th><th>Group</th><th>Balance</th><th></th></tr></thead>
          <tbody>
            {% for a in accts %}
              <tr>
                <td><a href="{{ url_for('ledger', account_id=a.id) }}">{{ a.name }}</a></td>
                <td>{{ a.group or '' }}</td>
                <td class="text-end">{{ '{:.2f}'.format(get_account_balance(a.id, co.id) ) }}</td>
                <td><a href="{{ url_for('edit_account', account_id=a.id) }}" class="btn btn-sm btn-outline-secondary">Edit</a></td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    """, accts=accts, co=co, get_account_balance=get_account_balance)
    return render_template_string(BASE_TMPL, content=content, current_company=co)

@app.route('/accounts/add', methods=['GET','POST'])
def add_account():
    co = get_current_company()
    if request.method == 'POST':
        name = request.form.get('name').strip()
        group = request.form.get('group').strip()
        if not name:
            flash('Account name required','danger')
        else:
            if Account.query.filter_by(company_id=co.id).filter(func.lower(Account.name)==name.lower()).first():
                flash('Account name already exists for this company','danger')
            else:
                db.session.add(Account(company_id=co.id, name=name, group=group))
                db.session.commit()
                flash('Account created','success')
                return redirect(url_for('accounts'))
    content = """
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Add Account</h4>
        <form method="post">
          <div class="mb-3">
            <label class="form-label">Account Name</label>
            <input name="name" class="form-control">
          </div>
          <div class="mb-3">
            <label class="form-label">Group (Assets, Liabilities, Income, Expense, Equity)</label>
            <input name="group" class="form-control">
          </div>
          <button class="btn btn-primary">Save</button>
          <a class="btn btn-secondary" href="{{ url_for('accounts') }}">Cancel</a>
        </form>
      </div>
    """
    return render_template_string(BASE_TMPL, content=content, current_company=co)

@app.route('/accounts/<int:account_id>/edit', methods=['GET','POST'])
def edit_account(account_id):
    co = get_current_company()
    a = Account.query.filter_by(company_id=co.id).filter_by(id=account_id).first_or_404()
    if request.method == 'POST':
        a.name = request.form.get('name').strip()
        a.group = request.form.get('group').strip()
        db.session.commit()
        flash('Updated','success')
        return redirect(url_for('accounts'))
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Edit Account</h4>
        <form method="post">
          <div class="mb-3">
            <label class="form-label">Account Name</label>
            <input name="name" class="form-control" value="{{ a.name }}">
          </div>
          <div class="mb-3">
            <label class="form-label">Group</label>
            <input name="group" class="form-control" value="{{ a.group or '' }}">
          </div>
          <button class="btn btn-primary">Save</button>
          <a class="btn btn-secondary" href="{{ url_for('accounts') }}">Cancel</a>
        </form>
      </div>
    """, a=a, current_company=co)
    return render_template_string(BASE_TMPL, content=content, current_company=co)

# --- Vouchers ---
def parse_lines_from_form(form, company_id):
    lines = []
    idx = 1
    while True:
        key = f'account_id_{idx}'
        if key not in form:
            break
        aid = form.get(key)
        if not aid:
            idx += 1
            continue
        try:
            acct = Account.query.filter_by(company_id=company_id).get(int(aid))
        except:
            acct = None
        if not acct:
            idx += 1
            continue
        amt_raw = form.get(f'amount_{idx}', '0').strip()
        if not amt_raw:
            idx += 1
            continue
        try:
            amt = to_decimal(amt_raw)
        except:
            idx += 1
            continue
        t = form.get(f'type_{idx}', 'D')  # D or C
        is_debit = (t.upper() == 'D')
        narration = form.get(f'narration_%d' % idx, '')
        lines.append({'account': acct, 'amount': amt, 'is_debit': is_debit, 'narration': narration})
        idx += 1
    return lines

@app.route('/vouchers')
def vouchers():
    co = get_current_company()
    vouchers = Voucher.query.filter_by(company_id=co.id).order_by(Voucher.date.desc(), Voucher.id.desc()).all()
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Vouchers ({{ co.name }}) <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('create_voucher') }}">+ New</a></h4>
        <table class="table table-sm mt-2">
          <thead><tr><th>Date</th><th>Narration</th><th>Total Debit</th><th>Total Credit</th><th></th></tr></thead>
          <tbody>
            {% for v in vouchers %}
              <tr>
                <td>{{ v.date }}</td>
                <td>{{ v.narration or '' }}</td>
                <td>{{ '{:.2f}'.format(sum([float(l.amount) for l in v.lines if l.is_debit])) }}</td>
                <td>{{ '{:.2f}'.format(sum([float(l.amount) for l in v.lines if not l.is_debit])) }}</td>
                <td><a class="btn btn-sm btn-outline-secondary" href="{{ url_for('view_voucher', voucher_id=v.id) }}">View</a></td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      </div>
    """, vouchers=vouchers, co=co)
    return render_template_string(BASE_TMPL, content=content, current_company=co)

@app.route('/vouchers/new', methods=['GET','POST'])
def create_voucher():
    co = get_current_company()
    accounts = Account.query.filter_by(company_id=co.id).order_by(Account.name).all()
    if request.method == 'POST':
        date_str = request.form.get('date')
        try:
            dt = dateparse(date_str).date()
        except:
            dt = date.today()
        narration = request.form.get('narration')
        lines = parse_lines_from_form(request.form, company_id=co.id)
        total_debit = sum([l['amount'] for l in lines if l['is_debit']])
        total_credit = sum([l['amount'] for l in lines if not l['is_debit']])
        if total_debit != total_credit:
            flash(f"Debit ({total_debit}) and Credit ({total_credit}) must be equal.", 'danger')
        elif len(lines) < 2:
            flash("Voucher must have at least two lines (double-entry).", 'danger')
        else:
            v = Voucher(company_id=co.id, date=dt, narration=narration)
            db.session.add(v)
            db.session.flush()
            for l in lines:
                vl = VoucherLine(voucher_id=v.id, company_id=co.id, account_id=l['account'].id,
                                 amount=l['amount'], is_debit=l['is_debit'], narration=l['narration'])
                db.session.add(vl)
            db.session.commit()
            flash('Voucher saved', 'success')
            return redirect(url_for('vouchers'))
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Create Voucher ({{ co.name }})</h4>
        <form method="post">
          <div class="mb-3">
            <label>Date</label>
            <input class="form-control" name="date" value="{{ request.form.get('date', '') or (now) }}">
          </div>
          <div class="mb-3">
            <label>Narration</label>
            <input class="form-control" name="narration" value="{{ request.form.get('narration','') }}">
          </div>
          <h6>Lines</h6>
          <p class="text-muted">At least two lines; total debits must equal total credits.</p>
          <table class="table table-sm">
            <thead><tr><th>Account</th><th>DR/CR</th><th>Amount</th><th>Narration</th></tr></thead>
            <tbody>
              {% for i in range(1,9) %}
              <tr>
                <td>
                  <select class="form-select" name="account_id_{{i}}">
                    <option value="">-- choose --</option>
                    {% for a in accounts %}
                      <option value="{{a.id}}" {% if request.form.get('account_id_'+i|string)==(a.id|string) %}selected{% endif %}>{{a.name}}</option>
                    {% endfor %}
                  </select>
                </td>
                <td>
                  <select class="form-select" name="type_{{i}}">
                    <option value="D" {% if request.form.get('type_'+i|string)=='D' %}selected{% endif %}>Debit</option>
                    <option value="C" {% if request.form.get('type_'+i|string)=='C' %}selected{% endif %}>Credit</option>
                  </select>
                </td>
                <td><input class="form-control" name="amount_{{i}}" value="{{ request.form.get('amount_'+i|string,'') }}"></td>
                <td><input class="form-control" name="narration_{{i}}" value="{{ request.form.get('narration_'+i|string,'') }}"></td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
          <button class="btn btn-primary">Save Voucher</button>
          <a class="btn btn-secondary" href="{{ url_for('vouchers') }}">Cancel</a>
        </form>
      </div>
    """, accounts=accounts, co=co, now=date.today().isoformat())
    return render_template_string(BASE_TMPL, content=content, current_company=co)

@app.route('/vouchers/<int:voucher_id>')
def view_voucher(voucher_id):
    co = get_current_company()
    v = Voucher.query.filter_by(company_id=co.id).get_or_404(voucher_id)
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Voucher #{{ v.id }} <small class="text-muted">{{ v.date }}</small></h4>
        <p>{{ v.narration }}</p>
        <table class="table table-sm">
          <thead><tr><th>Account</th><th>DR</th><th>CR</th><th>Narration</th></tr></thead>
          <tbody>
            {% for l in v.lines %}
              <tr>
                <td><a href="{{ url_for('ledger', account_id=l.account.id) }}">{{ l.account.name }}</a></td>
                <td>{{ ('{:.2f}'.format(l.amount) if l.is_debit else '') }}</td>
                <td>{{ ('{:.2f}'.format(l.amount) if (not l.is_debit) else '') }}</td>
                <td>{{ l.narration or '' }}</td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
        <a class="btn btn-secondary" href="{{ url_for('vouchers') }}">Back</a>
      </div>
    """, v=v, co=co)
    return render_template_string(BASE_TMPL, content=content, current_company=co)

# --- Ledger & exports ---
@app.route('/ledger/<int:account_id>')
def ledger(account_id):
    co = get_current_company()
    acct = Account.query.filter_by(company_id=co.id).filter_by(id=account_id).first_or_404()
    q = VoucherLine.query.join(Voucher).filter(VoucherLine.account_id==acct.id, VoucherLine.company_id==co.id).order_by(Voucher.date, Voucher.id)
    lines = q.all()
    running = Decimal('0.00')
    ledger_rows = []
    for l in lines:
        amt = Decimal(l.amount)
        if l.is_debit:
            running += amt
        else:
            running -= amt
        ledger_rows.append({'date': l.voucher.date, 'voucher_id': l.voucher.id, 'narration': l.voucher.narration or l.narration, 'debit': l.amount if l.is_debit else None, 'credit': l.amount if (not l.is_debit) else None, 'running': running})
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Ledger: {{ acct.name }} ({{ co.name }})</h4>
        <p>Group: {{ acct.group or '-' }}</p>
        <table class="table table-sm">
          <thead><tr><th>Date</th><th>Voucher</th><th>Narration</th><th>Debit</th><th>Credit</th><th>Running</th></tr></thead>
          <tbody>
            {% for r in rows %}
              <tr>
                <td>{{ r.date }}</td>
                <td><a href="{{ url_for('view_voucher', voucher_id=r.voucher_id) }}">#{{ r.voucher_id }}</a></td>
                <td>{{ r.narration or '' }}</td>
                <td class="text-end">{{ ('{:.2f}'.format(r.debit) if r.debit else '') }}</td>
                <td class="text-end">{{ ('{:.2f}'.format(r.credit) if r.credit else '') }}</td>
                <td class="text-end">{{ '{:.2f}'.format(r.running) }}</td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
        <p><a class="btn btn-sm btn-outline-secondary" href="{{ url_for('export_ledger', account_id=acct.id) }}">Export CSV</a>
           <a class="btn btn-secondary" href="{{ url_for('accounts') }}">Back</a></p>
      </div>
    """, acct=acct, rows=ledger_rows, co=co)
    return render_template_string(BASE_TMPL, content=content, current_company=co)

@app.route('/export/ledger/<int:account_id>')
def export_ledger(account_id):
    co = get_current_company()
    acct = Account.query.filter_by(company_id=co.id).filter_by(id=account_id).first_or_404()
    q = VoucherLine.query.join(Voucher).filter(VoucherLine.account_id==acct.id, VoucherLine.company_id==co.id).order_by(Voucher.date, Voucher.id)
    lines = q.all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Date','VoucherID','Narration','Debit','Credit'])
    for l in lines:
        writer.writerow([l.voucher.date.isoformat(), l.voucher.id, l.voucher.narration or l.narration or '', (str(l.amount) if l.is_debit else ''), (str(l.amount) if not l.is_debit else '')])
    output.seek(0)
    fname = f"ledger_{co.name}_{acct.name}.csv".replace(' ','_')
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-disposition":f"attachment; filename={fname}"})

# --- Trial Balance ---
@app.route('/trial_balance')
def trial_balance():
    co = get_current_company()
    accts = Account.query.filter_by(company_id=co.id).order_by(Account.group, Account.name).all()
    rows = []
    total_debit = Decimal('0.00')
    total_credit = Decimal('0.00')
    for a in accts:
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount).filter(VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id), 0)).scalar()
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount).filter(VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id), 0)).scalar()
        dr = Decimal(dr or 0)
        cr = Decimal(cr or 0)
        net = dr - cr
        if net >= 0:
            total_debit += net
            debit_val = net
            credit_val = None
        else:
            total_credit += -net
            debit_val = None
            credit_val = -net
        rows.append({'account': a, 'debit': debit_val, 'credit': credit_val})
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Trial Balance ({{ co.name }})</h4>
        <p><a class="btn btn-sm btn-outline-secondary" href="{{ url_for('export_trial_balance') }}">Export CSV</a></p>
        <table class="table table-sm">
          <thead><tr><th>Account</th><th>Debit</th><th>Credit</th></tr></thead>
          <tbody>
            {% for r in rows %}
              <tr>
                <td><a href="{{ url_for('ledger', account_id=r.account.id) }}">{{ r.account.name }}</a></td>
                <td class="text-end">{{ ('{:.2f}'.format(r.debit) if r.debit else '') }}</td>
                <td class="text-end">{{ ('{:.2f}'.format(r.credit) if r.credit else '') }}</td>
              </tr>
            {% endfor %}
          </tbody>
          <tfoot>
            <tr><th>Total</th><th class="text-end">{{ '{:.2f}'.format(total_debit) }}</th><th class="text-end">{{ '{:.2f}'.format(total_credit) }}</th></tr>
          </tfoot>
        </table>
        {% if total_debit == total_credit %}
          <div class="alert alert-success">Trial Balance balanced (Debits = Credits)</div>
        {% else %}
          <div class="alert alert-danger">Trial Balance NOT balanced</div>
        {% endif %}
      </div>
    """, rows=rows, total_debit=total_debit, total_credit=total_credit, co=co)
    return render_template_string(BASE_TMPL, content=content, current_company=co)

@app.route('/export/trial_balance')
def export_trial_balance():
    co = get_current_company()
    accts = Account.query.filter_by(company_id=co.id).order_by(Account.group, Account.name).all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Account','Debit','Credit'])
    total_debit = Decimal('0.00')
    total_credit = Decimal('0.00')
    for a in accts:
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount).filter(VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id), 0)).scalar() or 0
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount).filter(VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id), 0)).scalar() or 0
        dr = Decimal(dr)
        cr = Decimal(cr)
        net = dr - cr
        if net >= 0:
            total_debit += net
            writer.writerow([a.name, str(net), ''])
        else:
            total_credit += -net
            writer.writerow([a.name, '', str(-net)])
    writer.writerow(['Total', str(total_debit), str(total_credit)])
    output.seek(0)
    fname = f"trial_balance_{co.name}.csv".replace(' ','_')
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-disposition":f"attachment; filename={fname}"})

# --- Profit & Loss ---
@app.route('/reports/pnl')
def pnl():
    co = get_current_company()
    # optional start/end override via query params
    s = request.args.get('start')
    e = request.args.get('end')
    if s and e:
        try:
            start = dateparse(s).date()
            end = dateparse(e).date()
        except:
            start, end = get_fiscal_year_dates(co)
    else:
        start, end = get_fiscal_year_dates(co)
    # Income accounts: group == Income (case-insensitive). Sum (credits - debits) within date range.
    income_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'income').order_by(Account.name).all()
    expense_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'expense').order_by(Account.name).all()
    income_rows = []
    expense_rows = []
    total_income = Decimal('0.00')
    total_expense = Decimal('0.00')
    for a in income_accounts:
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
            Voucher.date >= start, Voucher.date <= end
        ).scalar() or 0
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
            Voucher.date >= start, Voucher.date <= end
        ).scalar() or 0
        val = Decimal(cr) - Decimal(dr)  # net credit increases income
        income_rows.append({'account': a, 'amount': val})
        total_income += max(val, Decimal('0.00'))
    for a in expense_accounts:
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
            Voucher.date >= start, Voucher.date <= end
        ).scalar() or 0
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
            Voucher.date >= start, Voucher.date <= end
        ).scalar() or 0
        val = Decimal(dr) - Decimal(cr)  # net debit increases expense
        expense_rows.append({'account': a, 'amount': val})
        total_expense += max(val, Decimal('0.00'))
    net_profit = total_income - total_expense
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Profit & Loss ({{ co.name }})</h4>
        <p>Period: {{ start }} to {{ end }}</p>
        <div class="row">
          <div class="col-md-6">
            <h5>Income</h5>
            <table class="table table-sm"><thead><tr><th>Account</th><th class="text-end">Amount ({{ co.currency }})</th></tr></thead>
            <tbody>
            {% for r in income_rows %}
              <tr><td>{{ r.account.name }}</td><td class="text-end">{{ '{:.2f}'.format(r.amount) }}</td></tr>
            {% endfor %}
            <tr><th>Total Income</th><th class="text-end">{{ '{:.2f}'.format(total_income) }}</th></tr>
            </tbody></table>
          </div>
          <div class="col-md-6">
            <h5>Expenses</h5>
            <table class="table table-sm"><thead><tr><th>Account</th><th class="text-end">Amount ({{ co.currency }})</th></tr></thead>
            <tbody>
            {% for r in expense_rows %}
              <tr><td>{{ r.account.name }}</td><td class="text-end">{{ '{:.2f}'.format(r.amount) }}</td></tr>
            {% endfor %}
            <tr><th>Total Expense</th><th class="text-end">{{ '{:.2f}'.format(total_expense) }}</th></tr>
            </tbody></table>
          </div>
        </div>
        <div class="alert alert-info">Net Profit (Income - Expense): <strong>{{ '{:.2f}'.format(net_profit) }} {{ co.currency }}</strong></div>
        <p>
          <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('export_pnl', start=start, end=end) }}">Export CSV</a>
          <a class="btn btn-secondary" href="{{ url_for('index') }}">Back</a>
        </p>
      </div>
    """, co=co, income_rows=income_rows, expense_rows=expense_rows, total_income=total_income, total_expense=total_expense, net_profit=net_profit, start=start, end=end)
    return render_template_string(BASE_TMPL, content=content, current_company=co)

@app.route('/export/pnl')
def export_pnl():
    co = get_current_company()
    s = request.args.get('start')
    e = request.args.get('end')
    if s and e:
        try:
            start = dateparse(s).date()
            end = dateparse(e).date()
        except:
            start, end = get_fiscal_year_dates(co)
    else:
        start, end = get_fiscal_year_dates(co)
    income_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'income').order_by(Account.name).all()
    expense_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'expense').order_by(Account.name).all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Type','Account','Amount'])
    total_income = Decimal('0.00')
    total_expense = Decimal('0.00')
    for a in income_accounts:
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
            Voucher.date >= start, Voucher.date <= end
        ).scalar() or 0
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
            Voucher.date >= start, Voucher.date <= end
        ).scalar() or 0
        val = Decimal(cr) - Decimal(dr)
        writer.writerow(['Income', a.name, str(val)])
        total_income += max(val, Decimal('0.00'))
    for a in expense_accounts:
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
            Voucher.date >= start, Voucher.date <= end
        ).scalar() or 0
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
            Voucher.date >= start, Voucher.date <= end
        ).scalar() or 0
        val = Decimal(dr) - Decimal(cr)
        writer.writerow(['Expense', a.name, str(val)])
        total_expense += max(val, Decimal('0.00'))
    writer.writerow(['Total Income', '', str(total_income)])
    writer.writerow(['Total Expense', '', str(total_expense)])
    writer.writerow(['Net Profit', '', str(total_income - total_expense)])
    output.seek(0)
    fname = f"pnl_{co.name}_{start}_{end}.csv".replace(' ','_')
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-disposition":f"attachment; filename={fname}"})

# --- Balance Sheet ---
@app.route('/reports/balance_sheet')
def balance_sheet():
    co = get_current_company()
    # default date: fiscal year end
    d = request.args.get('date')
    if d:
        try:
            upto = dateparse(d).date()
        except:
            _, upto = get_fiscal_year_dates(co)
    else:
        _, upto = get_fiscal_year_dates(co)
    # Classify by groups
    assets = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'assets').order_by(Account.name).all()
    liabilities = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'liabilities').order_by(Account.name).all()
    equity = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'equity').order_by(Account.name).all()
    assets_rows = []
    liabilities_rows = []
    equity_rows = []
    total_assets = Decimal('0.00')
    total_liabilities = Decimal('0.00')
    total_equity = Decimal('0.00')
    for a in assets:
        bal = get_account_balance(a.id, co.id, up_to_date=upto)
        assets_rows.append({'account': a, 'amount': bal})
        total_assets += bal if bal > 0 else Decimal('0.00')
    for a in liabilities:
        bal = get_account_balance(a.id, co.id, up_to_date=upto)
        # liabilities normally credit; our get_account_balance returns dr-cr (positive = debit). For liability credit balances are negative.
        liabilities_rows.append({'account': a, 'amount': -bal})  # flip sign for presentation
        total_liabilities += (-bal) if (-bal) > 0 else Decimal('0.00')
    for a in equity:
        bal = get_account_balance(a.id, co.id, up_to_date=upto)
        equity_rows.append({'account': a, 'amount': -bal})
        total_equity += (-bal) if (-bal) > 0 else Decimal('0.00')
    # Add retained earnings from P&L (net profit) into equity: compute current FY profit
    fy_start, fy_end = get_fiscal_year_dates(co, ref_date=upto)
    # compute net profit for FY
    income_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'income').all()
    expense_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'expense').all()
    total_income = Decimal('0.00')
    total_expense = Decimal('0.00')
    for a in income_accounts:
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
            Voucher.date >= fy_start, Voucher.date <= fy_end
        ).scalar() or 0
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
            Voucher.date >= fy_start, Voucher.date <= fy_end
        ).scalar() or 0
        total_income += Decimal(cr) - Decimal(dr)
    for a in expense_accounts:
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
            Voucher.date >= fy_start, Voucher.date <= fy_end
        ).scalar() or 0
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
            Voucher.date >= fy_start, Voucher.date <= fy_end
        ).scalar() or 0
        total_expense += Decimal(dr) - Decimal(cr)
    retained = total_income - total_expense
    if retained != 0:
        equity_rows.append({'account': None, 'amount': retained, 'label': 'Retained Earnings (FY Net)'})
        total_equity += retained
    content = render_template_string("""
      <div class="p-3 bg-white rounded shadow-sm">
        <h4>Balance Sheet ({{ co.name }})</h4>
        <p>As of: {{ upto }}</p>
        <div class="row">
          <div class="col-md-6">
            <h5>Assets</h5>
            <table class="table table-sm"><thead><tr><th>Account</th><th class="text-end">Amount ({{ co.currency }})</th></tr></thead>
            <tbody>
            {% for r in assets_rows %}
              <tr><td>{{ r.account.name }}</td><td class="text-end">{{ '{:.2f}'.format(r.amount) }}</td></tr>
            {% endfor %}
            <tr><th>Total Assets</th><th class="text-end">{{ '{:.2f}'.format(total_assets) }}</th></tr>
            </tbody></table>
          </div>
          <div class="col-md-6">
            <h5>Liabilities & Equity</h5>
            <table class="table table-sm"><thead><tr><th>Account</th><th class="text-end">Amount ({{ co.currency }})</th></tr></thead>
            <tbody>
            <tr><th colspan="2">Liabilities</th></tr>
            {% for r in liabilities_rows %}
              <tr><td>{{ r.account.name }}</td><td class="text-end">{{ '{:.2f}'.format(r.amount) }}</td></tr>
            {% endfor %}
            <tr><th>Liabilities Total</th><th class="text-end">{{ '{:.2f}'.format(total_liabilities) }}</th></tr>
            <tr><th colspan="2">Equity</th></tr>
            {% for r in equity_rows %}
              <tr>
                <td>{{ r.account.name if r.account else r.label }}</td>
                <td class="text-end">{{ '{:.2f}'.format(r.amount) }}</td>
              </tr>
            {% endfor %}
            <tr><th>Total Equity</th><th class="text-end">{{ '{:.2f}'.format(total_equity) }}</th></tr>
            </tbody></table>
          </div>
        </div>
        <p>
          <a class="btn btn-sm btn-outline-secondary" href="{{ url_for('export_balance_sheet', date=upto) }}">Export CSV</a>
          <a class="btn btn-secondary" href="{{ url_for('index') }}">Back</a>
        </p>
      </div>
    """, co=co, assets_rows=assets_rows, liabilities_rows=liabilities_rows, equity_rows=equity_rows, total_assets=total_assets, total_liabilities=total_liabilities, total_equity=total_equity, upto=upto)
    return render_template_string(BASE_TMPL, content=content, current_company=co)

@app.route('/export/balance_sheet')
def export_balance_sheet():
    co = get_current_company()
    d = request.args.get('date')
    if d:
        try:
            upto = dateparse(d).date()
        except:
            _, upto = get_fiscal_year_dates(co)
    else:
        _, upto = get_fiscal_year_dates(co)
    assets = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'assets').order_by(Account.name).all()
    liabilities = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'liabilities').order_by(Account.name).all()
    equity = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'equity').order_by(Account.name).all()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Section','Account','Amount'])
    total_assets = Decimal('0.00')
    for a in assets:
        bal = get_account_balance(a.id, co.id, up_to_date=upto)
        writer.writerow(['Assets', a.name, str(bal)])
        total_assets += bal if bal > 0 else Decimal('0.00')
    total_liabilities = Decimal('0.00')
    for a in liabilities:
        bal = get_account_balance(a.id, co.id, up_to_date=upto)
        writer.writerow(['Liabilities', a.name, str(-bal)])
        total_liabilities += (-bal) if (-bal) > 0 else Decimal('0.00')
    total_equity = Decimal('0.00')
    for a in equity:
        bal = get_account_balance(a.id, co.id, up_to_date=upto)
        writer.writerow(['Equity', a.name, str(-bal)])
        total_equity += (-bal) if (-bal) > 0 else Decimal('0.00')
    # retained earnings
    fy_start, fy_end = get_fiscal_year_dates(co, ref_date=upto)
    income_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'income').all()
    expense_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'expense').all()
    total_income = Decimal('0.00')
    total_expense = Decimal('0.00')
    for a in income_accounts:
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
            Voucher.date >= fy_start, Voucher.date <= fy_end
        ).scalar() or 0
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
            Voucher.date >= fy_start, Voucher.date <= fy_end
        ).scalar() or 0
        total_income += Decimal(cr) - Decimal(dr)
    for a in expense_accounts:
        dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
            Voucher.date >= fy_start, Voucher.date <= fy_end
        ).scalar() or 0
        cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
            VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
            Voucher.date >= fy_start, Voucher.date <= fy_end
        ).scalar() or 0
        total_expense += Decimal(dr) - Decimal(cr)
    retained = total_income - total_expense
    if retained != 0:
        writer.writerow(['Equity', 'Retained Earnings (FY Net)', str(retained)])
        total_equity += retained
    writer.writerow(['Totals', 'Assets', str(total_assets)])
    writer.writerow(['Totals', 'Liabilities', str(total_liabilities)])
    writer.writerow(['Totals', 'Equity', str(total_equity)])
    output.seek(0)
    fname = f"balancesheet_{co.name}_{upto}.csv".replace(' ','_')
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-disposition":f"attachment; filename={fname}"})

# --- Export / Backup company as ZIP ---
@app.route('/companies/backup')
def backup_company():
    co = get_current_company()
    # Create CSVs in memory and write them into a ZIP buffer
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
        # Accounts
        acc_buf = io.StringIO()
        w = csv.writer(acc_buf)
        w.writerow(['ID','Name','Group'])
        accts = Account.query.filter_by(company_id=co.id).order_by(Account.id).all()
        for a in accts:
            w.writerow([a.id, a.name, a.group or ''])
        zf.writestr('accounts.csv', acc_buf.getvalue())

        # Vouchers
        v_buf = io.StringIO()
        w = csv.writer(v_buf)
        w.writerow(['ID','Date','Narration'])
        vs = Voucher.query.filter_by(company_id=co.id).order_by(Voucher.id).all()
        for v in vs:
            w.writerow([v.id, v.date.isoformat(), v.narration or ''])
        zf.writestr('vouchers.csv', v_buf.getvalue())

        # Voucher lines
        vl_buf = io.StringIO()
        w = csv.writer(vl_buf)
        w.writerow(['ID','VoucherID','AccountID','IsDebit','Amount','Narration'])
        vls = VoucherLine.query.filter_by(company_id=co.id).order_by(VoucherLine.id).all()
        for l in vls:
            w.writerow([l.id, l.voucher_id, l.account_id, l.is_debit, str(l.amount), l.narration or ''])
        zf.writestr('voucher_lines.csv', vl_buf.getvalue())

        # Trial balance
        tb_buf = io.StringIO()
        w = csv.writer(tb_buf)
        w.writerow(['Account','Debit','Credit'])
        accts = Account.query.filter_by(company_id=co.id).order_by(Account.group, Account.name).all()
        total_debit = Decimal('0.00')
        total_credit = Decimal('0.00')
        for a in accts:
            dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount).filter(VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id), 0)).scalar() or 0
            cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount).filter(VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id), 0)).scalar() or 0
            dr = Decimal(dr)
            cr = Decimal(cr)
            net = dr - cr
            if net >= 0:
                total_debit += net
                w.writerow([a.name, str(net), ''])
            else:
                total_credit += -net
                w.writerow([a.name, '', str(-net)])
        w.writerow(['Total', str(total_debit), str(total_credit)])
        zf.writestr('trial_balance.csv', tb_buf.getvalue())

        # P&L (current FY)
        start, end = get_fiscal_year_dates(co)
        pnl_buf = io.StringIO()
        w = csv.writer(pnl_buf)
        w.writerow(['Type','Account','Amount'])
        income_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'income').order_by(Account.name).all()
        expense_accounts = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'expense').order_by(Account.name).all()
        for a in income_accounts:
            cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
                VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
                Voucher.date >= start, Voucher.date <= end
            ).scalar() or 0
            dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
                VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
                Voucher.date >= start, Voucher.date <= end
            ).scalar() or 0
            val = Decimal(cr) - Decimal(dr)
            w.writerow(['Income', a.name, str(val)])
        for a in expense_accounts:
            dr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
                VoucherLine.account_id==a.id, VoucherLine.is_debit==True, VoucherLine.company_id==co.id,
                Voucher.date >= start, Voucher.date <= end
            ).scalar() or 0
            cr = db.session.query(func.coalesce(func.sum(VoucherLine.amount),0)).join(Voucher).filter(
                VoucherLine.account_id==a.id, VoucherLine.is_debit==False, VoucherLine.company_id==co.id,
                Voucher.date >= start, Voucher.date <= end
            ).scalar() or 0
            val = Decimal(dr) - Decimal(cr)
            w.writerow(['Expense', a.name, str(val)])
        zf.writestr('profit_and_loss_current_fy.csv', pnl_buf.getvalue())

        # Balance sheet (as of FY end)
        bs_buf = io.StringIO()
        w = csv.writer(bs_buf)
        w.writerow(['Section','Account','Amount'])
        _, upto = get_fiscal_year_dates(co)
        assets = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'assets').order_by(Account.name).all()
        liabilities = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'liabilities').order_by(Account.name).all()
        equity = Account.query.filter_by(company_id=co.id).filter(func.lower(Account.group) == 'equity').order_by(Account.name).all()
        for a in assets:
            bal = get_account_balance(a.id, co.id, up_to_date=upto)
            w.writerow(['Assets', a.name, str(bal)])
        for a in liabilities:
            bal = get_account_balance(a.id, co.id, up_to_date=upto)
            w.writerow(['Liabilities', a.name, str(-bal)])
        for a in equity:
            bal = get_account_balance(a.id, co.id, up_to_date=upto)
            w.writerow(['Equity', a.name, str(-bal)])
        # retained earnings appended
        w.writerow(['Meta','FY Start', str(start)])
        w.writerow(['Meta','FY End', str(end)])
        zf.writestr('balance_sheet_as_of_fy_end.csv', bs_buf.getvalue())

    zip_buffer.seek(0)
    fname = f'backup_{co.name}_{date.today().isoformat()}.zip'.replace(' ','_')
    return send_file(zip_buffer, as_attachment=True, download_name=fname, mimetype='application/zip')

# ---------- Run ----------
if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5000)
