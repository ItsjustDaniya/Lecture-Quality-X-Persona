#!/usr/bin/env python3
"""
Lecture Quality Views Pipeline
Auto-generated cron pipeline — lecture quality dashboards

Ported from the DS_batches_data notebook, wrapped for unattended GitHub
Actions execution:
  - Auth via a Metabase API key (X-Api-Key header) instead of the old
    username/password session-login flow — no more ASHRITHA_SECRET_KEY,
    no login POST, no token refresh step.
  - requests.post is patched to use a retry-hardened Session (connection
    resets / 5xx / 429 are retried automatically), matching the fix applied
    to the main Assignment Automation Pipeline for card 9913-style failures.
  - gc.open()/gc.open_by_key() calls are wrapped so a missing share grant
    fails with the exact service-account email to add, instead of a bare
    SpreadsheetNotFound traceback.
  - Any uncaught exception exits non-zero so the GitHub Actions run goes red.
"""

import os
import sys
import json
import time
import traceback

import requests
import pandas as pd
import gspread
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials

start_time = time.time()

# -------------------- ENV & AUTH --------------------
METABASE_API_KEY = os.getenv("METABASE_API_KEY")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")

missing = [n for n, v in [
    ("METABASE_API_KEY", METABASE_API_KEY),
    ("SERVICE_ACCOUNT_JSON", service_account_json),
] if not v]
if missing:
    raise ValueError(f"❌ Missing environment variables: {', '.join(missing)}")

service_info = json.loads(service_account_json)
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ],
)
gc = gspread.authorize(creds)

METABASE_BASE = "https://metabase-lierhfgoeiwhr.newtonschool.co"

# -------------------- RETRY-HARDENED SESSION --------------------
# Same fix as the main Assignment Automation Pipeline: ConnectionError /
# ECONNRESET, 429, and 5xx are retried at the transport level instead of
# failing the whole job on the first hiccup.
SESSION = requests.Session()
_adapter = HTTPAdapter(
    max_retries=Retry(
        total=4,
        connect=4,
        read=2,
        backoff_factor=5,             # 5s, 10s, 20s, 40s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["POST", "GET"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    ),
    pool_connections=10,
    pool_maxsize=10,
)
SESSION.mount("https://", _adapter)
SESSION.mount("http://", _adapter)

# Every requests.post(...) call in the ported notebook code below now goes
# through the retry-hardened session automatically — no need to edit each
# call site individually.
requests.post = SESSION.post

# Static header used for every Metabase API call — no login step, no
# token expiry/refresh to worry about.
METABASE_HEADERS = {
    "Content-Type": "application/json",
    "X-Api-Key": METABASE_API_KEY,
}


def safe_open_sheet(title):
    """gc.open() wrapped to fail with an actionable message (the exact
    service-account email to share the sheet with) instead of a bare
    SpreadsheetNotFound traceback."""
    try:
        return gc.open(title)
    except gspread.exceptions.SpreadsheetNotFound:
        raise RuntimeError(
            f"❌ Could not open Google Sheet '{title}'. Either the title "
            f"doesn't match exactly, or it hasn't been shared with this "
            f"service account: {service_info.get('client_email')}. "
            "Share it as Editor, then re-run."
        )


def safe_open_by_key(key):
    """Same as safe_open_sheet, but for gc.open_by_key()."""
    try:
        return gc.open_by_key(key)
    except gspread.exceptions.SpreadsheetNotFound:
        raise RuntimeError(
            f"❌ Could not open Google Sheet with key '{key}'. Share it with "
            f"this service account as Editor: {service_info.get('client_email')}"
        )


print("🔎 ENV CHECK")
print(f"   Metabase API key   : {'[SET]' if METABASE_API_KEY else '[MISSING]'}")
print(f"   SA client_email    : {service_info.get('client_email')}")

# ═══════════════════════════════════════════════════════════════════════════
# PIPELINE BODY (ported from notebook cells: 30-44)
# ═══════════════════════════════════════════════════════════════════════════
try:
    # ──────────────────────────────────────────────────────────────────────
    # Cell 30
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ============================ CONFIG ============================
    INPUT_SHEET_NAME = 'Calendar'
    INPUT_WORKSHEET  = 'Lecture_Quality'
    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'

    MODULES = {                       # module label -> regex matching the 'Batch' column
        'SQL':          r'DS SQL',
        'Spreadsheets': r'DS Spreadsheet',
        'Power BI':     r'DS Power ?BI',
        'Python':       r'DS Python',
        'EDA 1':        r'DS EDA 1',
        'EDA 2':        r'DS EDA 2',
    }

    CUTOFF_MONTH = pd.Timestamp('2025-12-01')   # keep cohorts in this month or later
    MIN_CLASSES_PER_ROW = 1                     # drop instructor rows below this many classes (1 = keep all)
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str):
            return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m:
            return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4:
            year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2:
                return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)


    # ---------- 1. READ INPUT ----------
    ws_in = safe_open_sheet(INPUT_SHEET_NAME).worksheet(INPUT_WORKSHEET)
    raw = ws_in.get_all_values()
    df = pd.DataFrame(raw[1:], columns=raw[0])
    print(f"Read input: {df.shape[0]} rows")

    # ---------- 2. CLEAN + METRIC + BATCH MONTH ----------
    df['date']           = pd.to_datetime(df['date'], errors='coerce')
    df['live_attendees'] = pd.to_numeric(df['live_attendees'], errors='coerce')
    df['batch_strength'] = pd.to_numeric(df['batch_strength'], errors='coerce')
    df['attendance_rate'] = np.where(
        df['batch_strength'] > 0,
        (df['live_attendees'] / df['batch_strength'] * 100).round(1),
        np.nan
    )
    df['batch_month'] = df['Batch'].apply(parse_batch_month)
    df['instructor_name'] = df['instructor_name'].fillna('Unknown').str.strip()

    # ---------- 3. ASSEMBLE ALL MODULES INTO ONE FRAME ----------
    frames = []
    for module_label, pattern in MODULES.items():
        s = df[df['Batch'].str.contains(pattern, case=False, na=False) &
               (df['batch_month'] >= CUTOFF_MONTH)].copy()
        if not s.empty:
            s['module_label'] = module_label
            frames.append(s)

    allsub = pd.concat(frames, ignore_index=True)
    allsub = allsub.drop_duplicates(['Batch', 'lecture_id', 'instructor_name'])

    # class number = chronological index of each unique lecture within its batch
    lo = (allsub[['Batch', 'lecture_id', 'date']]
          .drop_duplicates()
          .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    allsub = allsub.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')

    # week number within the batch
    allsub['week_no'] = ((allsub['date'] - allsub.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    # row identity for the INSTRUCTOR views only (Section 2) — Section 1 groups by Batch alone
    allsub['row_label'] = allsub['Batch'] + '  —  ' + allsub['instructor_name']

    if MIN_CLASSES_PER_ROW > 1:
        counts = allsub.groupby('row_label')['lecture_id'].nunique()
        allsub = allsub[allsub['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    # row ordering: module order (SQL, Spreadsheets, Power BI), then cohort month
    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 4. SECTION 1 — BATCH VIEWS, grouped by Batch only (instructors merged) ----------
    # Multiple instructors teaching the same batch are now ONE row. Where more than
    # one instructor has a value for the same class/week, the cell is the mean of
    # their values (skipna) rather than picking one arbitrarily.
    batch_meta = allsub[['Batch', 'module_label', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module_label'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    def flat_table_by_batch(period_col, prefix, agg):
        vals = allsub.pivot_table(index='Batch', columns=period_col,
                                  values='attendance_rate', aggfunc=agg, dropna=False).round(1)
        vals.columns = [f'{prefix} {c}' for c in vals.columns]
        vals = vals.reindex(batch_order)   # apply module -> cohort-month row order, keep every batch even if all-NaN
        out = batch_meta[['Batch', 'module_label']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module_label': 'Module'})
        period_cols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[period_cols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + period_cols + ['Avg']]

    coc = flat_table_by_batch('class_no', 'Class', 'mean')
    wow = flat_table_by_batch('week_no',  'Week',  'mean')
    print(f"COC table: {coc.shape} | WOW table: {wow.shape}")
    print(f"Modules present in COC: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in allsub before pivot: {allsub[allsub['module_label']=='Power BI']['Batch'].nunique()}")
    print(f"Power BI rows in final COC table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 4b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = allsub[['row_label', 'Batch', 'instructor_name', 'module_label', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module_label'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    def flat_table_by_instructor(period_col, prefix, agg):
        vals = allsub.pivot_table(index='row_label', columns=period_col,
                                  values='attendance_rate', aggfunc=agg).round(1)
        vals.columns = [f'{prefix} {c}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module_label']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module_label': 'Module'})
        period_cols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[period_cols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + period_cols + ['Avg']]

    coc_full = flat_table_by_instructor('class_no', 'Class', 'first')
    wow_full = flat_table_by_instructor('week_no',  'Week',  'mean')

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()

        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))   # all modules this instructor teaches

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)

            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)

        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor COC table: {coc_instructor.shape} | Instructor WOW table: {wow_instructor.shape}")

    # ---------- 5. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)

    def get_or_create_ws(book, title, rows=600, cols=60):
        try:
            ws = book.worksheet(title); ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    # Section 1 — Batch Views (grouped by Batch, no Instructor column)
    ws_coc = get_or_create_ws(out_book, 'Class-on-Class (DOD)')
    set_with_dataframe(ws_coc, coc, include_index=False, include_column_header=True)

    ws_wow = get_or_create_ws(out_book, 'Week-on-Week (WOW)')
    set_with_dataframe(ws_wow, wow, include_index=False, include_column_header=True)

    # Section 2 — Instructor Views (Batch+Instructor granularity)
    ws_coc_instr = get_or_create_ws(out_book, 'Instructor View (DOD)')
    set_with_dataframe(ws_coc_instr, coc_instructor, include_index=False, include_column_header=True)

    ws_wow_instr = get_or_create_ws(out_book, 'Instructor View (WOW)')
    set_with_dataframe(ws_wow_instr, wow_instructor, include_index=False, include_column_header=True)

    print("=== DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 31
    # ──────────────────────────────────────────────────────────────────────
    import requests
    import pandas as pd
    from gspread_dataframe import set_with_dataframe

    # ---------------------------------------------------------
    # 1. Fetch per-student-per-week data from Metabase
    #    (query now includes week_start_date and week_no_wrt_module)
    # ---------------------------------------------------------
    res = requests.post(
        f'{METABASE_BASE}/api/card/11408/query/json',
        headers=METABASE_HEADERS
    )
    response_json = res.json()
    df_on_time = pd.DataFrame(response_json)
    sheet = safe_open_by_key('1vmSaipWrCYXI6eVxf10H3XYJhNycVQxxAA6x_s2Rdvc')
    worksheet = sheet.worksheet("On_time_join_rate")
    # clearing worksheet
    worksheet.clear()
    #Convert to a DataFrame
    # export df to a sheet
    set_with_dataframe(worksheet, df_on_time, include_index=False, include_column_header=True)

    # ──────────────────────────────────────────────────────────────────────
    # Cell 32
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ============================ CONFIG ============================
    SKELETON_SHEET_NAME = 'Calendar'
    SKELETON_WORKSHEET  = 'Lecture_Quality'

    METRIC_SHEET_NAME   = 'Lecture Quality - M02'
    METRIC_WORKSHEET    = 'On_time_join_rate'
    METRIC_VALUE_COL    = 'on_time_join_rate'
    METRIC_LECTURE_COL  = 'lecture_id'

    OUTPUT_SHEET_KEY    = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB             = 'M02 On-Time Join (DOD)'
    WOW_TAB             = 'M02 On-Time Join (WOW)'
    DOD_INSTRUCTOR_TAB  = 'M02 On-Time Join Instructor (DOD)'
    WOW_INSTRUCTOR_TAB  = 'M02 On-Time Join Instructor (WOW)'
    WOW_AGG             = 'mean'

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str): return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m: return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4: year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2: return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def read_sheet(name, tab):
        raw = safe_open_sheet(name).worksheet(tab).get_all_values()
        return pd.DataFrame(raw[1:], columns=raw[0])

    # ---------- 1. BUILD THE LECTURE SKELETON ----------
    skel = read_sheet(SKELETON_SHEET_NAME, SKELETON_WORKSHEET)
    skel['date'] = pd.to_datetime(skel['date'], errors='coerce')
    skel['lecture_id'] = pd.to_numeric(skel['lecture_id'], errors='coerce').astype('Int64')

    def assign_module(b):
        if not isinstance(b, str): return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I): return label
        return None
    skel['module']      = skel['Batch'].apply(assign_module)
    skel['batch_month'] = skel['Batch'].apply(parse_batch_month)

    skel = skel[skel['module'].notna() & (skel['batch_month'] >= CUTOFF_MONTH)].copy()
    skel['instructor_name'] = skel['instructor_name'].fillna('Unknown').str.strip()
    skel['row_label']       = skel['Batch'] + '  —  ' + skel['instructor_name']   # internal pivot key only

    lo = (skel[['Batch', 'lecture_id', 'date']].drop_duplicates()
            .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    skel = skel.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    skel['week_no'] = ((skel['date'] - skel.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    # ---------- 2. JOIN THE METRIC ----------
    metric = read_sheet(METRIC_SHEET_NAME, METRIC_WORKSHEET)
    metric[METRIC_LECTURE_COL] = pd.to_numeric(metric[METRIC_LECTURE_COL], errors='coerce').astype('Int64')
    metric[METRIC_VALUE_COL]   = pd.to_numeric(metric[METRIC_VALUE_COL], errors='coerce')

    data = skel.merge(metric[[METRIC_LECTURE_COL, METRIC_VALUE_COL]],
                      left_on='lecture_id', right_on=METRIC_LECTURE_COL, how='left')
    data = data.rename(columns={METRIC_VALUE_COL: 'metric'})
    print(f"Skeleton lectures: {skel['lecture_id'].nunique()} | matched a metric value: {data['metric'].notna().sum()}")

    if MIN_CLASSES_PER_ROW > 1:
        counts = data.groupby('row_label')['lecture_id'].nunique()
        data = data[data['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 3. SECTION 1 — BATCH VIEWS, grouped by Batch only (instructors merged) ----------
    # Multiple instructors teaching the same batch are now ONE row. Where more than
    # one instructor has a value for the same class/week, the cell is the mean of
    # their values (skipna). dropna=False on the pivot ensures a batch with zero
    # matched metric values still shows up as a blank row instead of disappearing.
    batch_meta = data[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    def flat_table_by_batch(period_col, prefix, agg):
        vals = data.pivot_table(index='Batch', columns=period_col,
                                values='metric', aggfunc=agg, dropna=False).round(1)
        vals.columns = [f'{prefix} {c}' for c in vals.columns]
        vals = vals.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch('class_no', 'Class', 'mean')
    wow = flat_table_by_batch('week_no',  'Week',  WOW_AGG)
    print(f"DOD: {coc.shape} | WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in data before pivot: {data[data['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 3b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = data[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    def flat_table_by_instructor(period_col, prefix, agg):
        vals = data.pivot_table(index='row_label', columns=period_col, values='metric', aggfunc=agg).round(1)
        vals.columns = [f'{prefix} {c}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor('class_no', 'Class', 'first')
    wow_full = flat_table_by_instructor('week_no',  'Week',  WOW_AGG)

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()

        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))   # all modules this instructor teaches

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)

            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)

        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 4. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)
    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title); ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M02 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 33
    # ──────────────────────────────────────────────────────────────────────
    import requests
    import pandas as pd
    from gspread_dataframe import set_with_dataframe

    # ---------------------------------------------------------
    # 1. Fetch per-student-per-week data from Metabase
    #    (query now includes week_start_date and week_no_wrt_module)
    # ---------------------------------------------------------
    res = requests.post(
        f'{METABASE_BASE}/api/card/11409/query/json',
        headers=METABASE_HEADERS
    )
    response_json = res.json()
    df_scr = pd.DataFrame(response_json)
    sheet = safe_open_by_key('1vmSaipWrCYXI6eVxf10H3XYJhNycVQxxAA6x_s2Rdvc')
    worksheet = sheet.worksheet("session_completion_rate")
    # clearing worksheet
    worksheet.clear()
    #Convert to a DataFrame
    # export df to a sheet
    set_with_dataframe(worksheet, df_scr, include_index=False, include_column_header=True)

    # ──────────────────────────────────────────────────────────────────────
    # Cell 34
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ============================ CONFIG ============================
    SKELETON_SHEET_NAME = 'Calendar'
    SKELETON_WORKSHEET  = 'Lecture_Quality'

    METRIC_SHEET_NAME   = 'Lecture Quality - M02'        # <-- M03 output (change if it's elsewhere)
    METRIC_WORKSHEET    = 'session_completion_rate'      # <-- its tab (exact name)
    METRIC_VALUE_COL    = 'session_completion_rate'
    METRIC_LECTURE_COL  = 'lecture_id'

    OUTPUT_SHEET_KEY    = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB             = 'M03 Completion (DOD)'
    WOW_TAB             = 'M03 Completion (WOW)'
    DOD_INSTRUCTOR_TAB  = 'M03 Completion Instructor (DOD)'
    WOW_INSTRUCTOR_TAB  = 'M03 Completion Instructor (WOW)'
    WOW_AGG             = 'mean'
    # ================================================================

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str): return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m: return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4: year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2: return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def read_sheet(name, tab):
        raw = safe_open_sheet(name).worksheet(tab).get_all_values()
        return pd.DataFrame(raw[1:], columns=raw[0])

    # ---------- 1. BUILD THE LECTURE SKELETON ----------
    skel = read_sheet(SKELETON_SHEET_NAME, SKELETON_WORKSHEET)
    skel['date'] = pd.to_datetime(skel['date'], errors='coerce')
    skel['lecture_id'] = pd.to_numeric(skel['lecture_id'], errors='coerce').astype('Int64')

    def assign_module(b):
        if not isinstance(b, str): return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I): return label
        return None
    skel['module']      = skel['Batch'].apply(assign_module)
    skel['batch_month'] = skel['Batch'].apply(parse_batch_month)

    skel = skel[skel['module'].notna() & (skel['batch_month'] >= CUTOFF_MONTH)].copy()
    skel['instructor_name'] = skel['instructor_name'].fillna('Unknown').str.strip()
    skel['row_label']       = skel['Batch'] + '  —  ' + skel['instructor_name']   # internal pivot key only

    lo = (skel[['Batch', 'lecture_id', 'date']].drop_duplicates()
            .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    skel = skel.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    skel['week_no'] = ((skel['date'] - skel.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    # ---------- 2. JOIN THE METRIC ----------
    metric = read_sheet(METRIC_SHEET_NAME, METRIC_WORKSHEET)
    metric[METRIC_LECTURE_COL] = pd.to_numeric(metric[METRIC_LECTURE_COL], errors='coerce').astype('Int64')
    metric[METRIC_VALUE_COL]   = pd.to_numeric(metric[METRIC_VALUE_COL], errors='coerce')

    data = skel.merge(metric[[METRIC_LECTURE_COL, METRIC_VALUE_COL]],
                      left_on='lecture_id', right_on=METRIC_LECTURE_COL, how='left')
    data = data.rename(columns={METRIC_VALUE_COL: 'metric'})
    print(f"Skeleton lectures: {skel['lecture_id'].nunique()} | matched a metric value: {data['metric'].notna().sum()}")

    if MIN_CLASSES_PER_ROW > 1:
        counts = data.groupby('row_label')['lecture_id'].nunique()
        data = data[data['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 3. SECTION 1 — BATCH VIEWS, grouped by Batch only (instructors merged) ----------
    # Multiple instructors teaching the same batch are now ONE row. Where more than
    # one instructor has a value for the same class/week, the cell is the mean of
    # their values (skipna). dropna=False on the pivot ensures a batch with zero
    # matched metric values still shows up as a blank row instead of disappearing.
    batch_meta = data[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    def flat_table_by_batch(period_col, prefix, agg):
        vals = data.pivot_table(index='Batch', columns=period_col,
                                values='metric', aggfunc=agg, dropna=False).round(1)
        vals.columns = [f'{prefix} {c}' for c in vals.columns]
        vals = vals.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch('class_no', 'Class', 'mean')
    wow = flat_table_by_batch('week_no',  'Week',  WOW_AGG)
    print(f"DOD: {coc.shape} | WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in data before pivot: {data[data['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 3b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = data[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    def flat_table_by_instructor(period_col, prefix, agg):
        vals = data.pivot_table(index='row_label', columns=period_col, values='metric', aggfunc=agg).round(1)
        vals.columns = [f'{prefix} {c}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor('class_no', 'Class', 'first')
    wow_full = flat_table_by_instructor('week_no',  'Week',  WOW_AGG)

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()

        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))   # all modules this instructor teaches

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)

            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)

        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 4. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)
    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title); ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M03 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 35
    # ──────────────────────────────────────────────────────────────────────
    import requests
    import pandas as pd
    from gspread_dataframe import set_with_dataframe

    # ---------------------------------------------------------
    # 1. Fetch per-student-per-week data from Metabase
    #    (query now includes week_start_date and week_no_wrt_module)
    # ---------------------------------------------------------
    res = requests.post(
        f'{METABASE_BASE}/api/card/11410/query/json',
        headers=METABASE_HEADERS
    )
    response_json = res.json()
    df_mdr = pd.DataFrame(response_json)
    sheet = safe_open_by_key('1vmSaipWrCYXI6eVxf10H3XYJhNycVQxxAA6x_s2Rdvc')
    worksheet = sheet.worksheet("drop_off_rate")
    # clearing worksheet
    worksheet.clear()
    #Convert to a DataFrame
    # export df to a sheet
    set_with_dataframe(worksheet, df_mdr, include_index=False, include_column_header=True)

    # ──────────────────────────────────────────────────────────────────────
    # Cell 36
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ============================ CONFIG ============================
    SKELETON_SHEET_NAME = 'Calendar'
    SKELETON_WORKSHEET  = 'Lecture_Quality'

    # ===== M04 run =====
    METRIC_SHEET_NAME   = 'Lecture Quality - M02'        # workbook holding the M04 output
    METRIC_WORKSHEET    = 'drop_off_rate'                # its tab (exact name)
    METRIC_VALUE_COL    = 'drop_off_rate'                # the metric column in that output
    METRIC_LECTURE_COL  = 'lecture_id'

    OUTPUT_SHEET_KEY    = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB             = 'M04 Drop-Off (DOD)'
    WOW_TAB             = 'M04 Drop-Off (WOW)'
    DOD_INSTRUCTOR_TAB  = 'M04 Drop-Off Instructor (DOD)'
    WOW_INSTRUCTOR_TAB  = 'M04 Drop-Off Instructor (WOW)'
    WOW_AGG             = 'mean'
    # ================================================================

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str): return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m: return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4: year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2: return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def read_sheet(name, tab):
        raw = safe_open_sheet(name).worksheet(tab).get_all_values()
        return pd.DataFrame(raw[1:], columns=raw[0])

    # ---------- 1. BUILD THE LECTURE SKELETON ----------
    skel = read_sheet(SKELETON_SHEET_NAME, SKELETON_WORKSHEET)
    skel['date'] = pd.to_datetime(skel['date'], errors='coerce')
    skel['lecture_id'] = pd.to_numeric(skel['lecture_id'], errors='coerce').astype('Int64')

    def assign_module(b):
        if not isinstance(b, str): return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I): return label
        return None
    skel['module']      = skel['Batch'].apply(assign_module)
    skel['batch_month'] = skel['Batch'].apply(parse_batch_month)

    skel = skel[skel['module'].notna() & (skel['batch_month'] >= CUTOFF_MONTH)].copy()
    skel['instructor_name'] = skel['instructor_name'].fillna('Unknown').str.strip()
    skel['row_label']       = skel['Batch'] + '  —  ' + skel['instructor_name']   # internal pivot key only

    lo = (skel[['Batch', 'lecture_id', 'date']].drop_duplicates()
            .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    skel = skel.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    skel['week_no'] = ((skel['date'] - skel.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    # ---------- 2. JOIN THE METRIC ----------
    metric = read_sheet(METRIC_SHEET_NAME, METRIC_WORKSHEET)
    metric[METRIC_LECTURE_COL] = pd.to_numeric(metric[METRIC_LECTURE_COL], errors='coerce').astype('Int64')
    metric[METRIC_VALUE_COL]   = pd.to_numeric(metric[METRIC_VALUE_COL], errors='coerce')

    data = skel.merge(metric[[METRIC_LECTURE_COL, METRIC_VALUE_COL]],
                      left_on='lecture_id', right_on=METRIC_LECTURE_COL, how='left')
    data = data.rename(columns={METRIC_VALUE_COL: 'metric'})
    print(f"Skeleton lectures: {skel['lecture_id'].nunique()} | matched a metric value: {data['metric'].notna().sum()}")

    if MIN_CLASSES_PER_ROW > 1:
        counts = data.groupby('row_label')['lecture_id'].nunique()
        data = data[data['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 3. SECTION 1 — BATCH VIEWS, grouped by Batch only (instructors merged) ----------
    # Multiple instructors teaching the same batch are now ONE row. Where more than
    # one instructor has a value for the same class/week, the cell is the mean of
    # their values (skipna). dropna=False on the pivot ensures a batch with zero
    # matched metric values still shows up as a blank row instead of disappearing.
    batch_meta = data[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    def flat_table_by_batch(period_col, prefix, agg):
        vals = data.pivot_table(index='Batch', columns=period_col,
                                values='metric', aggfunc=agg, dropna=False).round(1)
        vals.columns = [f'{prefix} {c}' for c in vals.columns]
        vals = vals.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch('class_no', 'Class', 'mean')
    wow = flat_table_by_batch('week_no',  'Week',  WOW_AGG)
    print(f"DOD: {coc.shape} | WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in data before pivot: {data[data['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 3b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = data[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    def flat_table_by_instructor(period_col, prefix, agg):
        vals = data.pivot_table(index='row_label', columns=period_col, values='metric', aggfunc=agg).round(1)
        vals.columns = [f'{prefix} {c}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor('class_no', 'Class', 'first')
    wow_full = flat_table_by_instructor('week_no',  'Week',  WOW_AGG)

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()

        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))   # all modules this instructor teaches

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)

            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)

        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 4. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)
    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title); ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M04 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 37
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import requests
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ========================================================================
    # STEP 0 — FETCH RAW PER-STUDENT LEAVE TIMES FROM METABASE (M05 query)
    # ========================================================================
    CARD_ID = 11425  # <-- set this to your saved M05 card id

    res = requests.post(
        f'{METABASE_BASE}/api/card/{CARD_ID}/query/json',
        headers=METABASE_HEADERS
    )
    raw_leave = pd.DataFrame(res.json())  # columns: lecture_id, course_id, batch_name, user_id, lecture_duration_mins, leave_min

    # ============================ CONFIG ============================
    SKELETON_SHEET_NAME = 'Calendar'
    SKELETON_WORKSHEET  = 'Lecture_Quality'

    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB             = 'M05 Drop-Off Bucket (DOD)'
    WOW_TAB             = 'M05 Drop-Off Bucket (WOW)'
    DOD_INSTRUCTOR_TAB  = 'M05 Drop-Off Bucket Instructor (DOD)'
    WOW_INSTRUCTOR_TAB  = 'M05 Drop-Off Bucket Instructor (WOW)'

    BUCKET_MINUTES = 15   # 15-minute intervals → 8 buckets for a 2-hour session
    MAX_BUCKETS    = 8    # 2hrs / 15min = 8 parts

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ================================================================

    # BUCKET LABELS: short form for cell readability
    BUCKET_LABELS = {
        i: f'B{i} ({(i-1)*BUCKET_MINUTES}–{i*BUCKET_MINUTES}m)'
        for i in range(1, MAX_BUCKETS + 1)
    }

    # NOTE ON VIEWS:
    # DOD: Rows = Batch, Columns = Class 1, Class 2...
    #   Each cell = "B3 (30–45m), 42%" — which 15-min bucket had peak drop-off
    #   in that class AND what % of exits fell in that bucket.
    #
    # WOW: Rows = Batch, Columns = Week 1, Week 2...
    #   Each cell = the bucket label that was the modal exit point most often
    #   across all lectures in that week (mode, ties broken by latest bucket).

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str): return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m: return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4: year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2: return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def assign_module(b):
        if not isinstance(b, str): return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I): return label
        return None

    def read_sheet(name, tab):
        raw = safe_open_sheet(name).worksheet(tab).get_all_values()
        return pd.DataFrame(raw[1:], columns=raw[0])

    def mode_max(series):
        """Mode of a series; on ties returns the latest (max) bucket."""
        s = series.dropna()
        if s.empty: return np.nan
        counts = s.value_counts()
        return counts[counts == counts.max()].index.max()

    # ---------- 1. BUILD THE LECTURE SKELETON ----------
    skel = read_sheet(SKELETON_SHEET_NAME, SKELETON_WORKSHEET)
    skel['date'] = pd.to_datetime(skel['date'], errors='coerce')
    skel['lecture_id'] = pd.to_numeric(skel['lecture_id'], errors='coerce').astype('Int64')

    skel['module']      = skel['Batch'].apply(assign_module)
    skel['batch_month'] = skel['Batch'].apply(parse_batch_month)
    skel = skel[skel['module'].notna() & (skel['batch_month'] >= CUTOFF_MONTH)].copy()
    skel['instructor_name'] = skel['instructor_name'].fillna('Unknown').str.strip()
    skel['row_label']       = skel['Batch'] + '  —  ' + skel['instructor_name']

    lo = (skel[['Batch', 'lecture_id', 'date']].drop_duplicates()
            .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    skel = skel.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    skel['week_no'] = ((skel['date'] - skel.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    lec_lookup = skel[['lecture_id', 'Batch', 'class_no', 'week_no']].drop_duplicates('lecture_id')

    # ---------- 2. BUCKET EACH EXIT INTO A 15-MIN INTERVAL ----------
    raw_leave['lecture_id'] = pd.to_numeric(raw_leave['lecture_id'], errors='coerce').astype('Int64')
    raw_leave['leave_min'] = pd.to_numeric(raw_leave['leave_min'], errors='coerce')
    raw_leave['lecture_duration_mins'] = pd.to_numeric(raw_leave['lecture_duration_mins'], errors='coerce')

    att = raw_leave.merge(lec_lookup, on='lecture_id', how='inner')
    att = att[att['lecture_duration_mins'] > 0].copy()
    att['exit_bucket'] = np.ceil(att['leave_min'] / BUCKET_MINUTES).clip(lower=1, upper=MAX_BUCKETS).astype(int)

    # ---------- 3. PER-LECTURE STATS ----------
    lec_counts = att.groupby(['lecture_id', 'Batch', 'class_no', 'week_no', 'exit_bucket']).size().rename('exits').reset_index()
    lec_total  = lec_counts.groupby('lecture_id')['exits'].transform('sum')
    lec_counts['pct_of_exits'] = (lec_counts['exits'] / lec_total * 100).round(1)

    # Modal exit bucket per lecture (bucket with most exits + its %)
    modal = (lec_counts.sort_values(['lecture_id', 'exits'], ascending=[True, False])
                       .drop_duplicates('lecture_id')
                       [['lecture_id', 'Batch', 'class_no', 'week_no', 'exit_bucket', 'pct_of_exits']]
                       .rename(columns={'exit_bucket': 'modal_bucket', 'pct_of_exits': 'modal_pct'}))

    # Combined label: "B3 (30–45m), 42%"
    modal['cell_label'] = modal.apply(
        lambda r: f"{BUCKET_LABELS[int(r['modal_bucket'])]}, {r['modal_pct']}%"
        if pd.notna(r['modal_bucket']) else np.nan, axis=1
    )

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 4. BATCH META (row ordering) ----------
    batch_meta = skel[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    # ---------- 5. DOD: Batch × Class — "bucket, %" ----------
    # Aggregate to batch+class: most common modal bucket across instructors + avg pct
    class_agg = (modal.groupby(['Batch', 'class_no'])
                      .agg(modal_bucket=('modal_bucket', mode_max),
                           modal_pct=('modal_pct', 'mean'))
                      .round({'modal_pct': 1})
                      .reset_index())

    class_agg['cell_label'] = class_agg.apply(
        lambda r: f"{BUCKET_LABELS[int(r['modal_bucket'])]}, {r['modal_pct']}%"
        if pd.notna(r['modal_bucket']) else np.nan, axis=1
    )

    coc_pv = class_agg.pivot_table(index='Batch', columns='class_no',
                                    values='cell_label', aggfunc='first')
    coc_pv.columns = [f'Class {int(c)}' for c in coc_pv.columns]
    coc_pv = coc_pv.reindex(batch_order)

    coc = batch_meta[['Batch', 'module']].merge(coc_pv.reset_index(), on='Batch', how='right')
    coc = coc.rename(columns={'module': 'Module'})
    ccols = [c for c in coc.columns if c.startswith('Class ')]
    coc = coc[['Batch', 'Module'] + ccols]
    print(f"M05 DOD (class x bucket,pct): {coc.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")

    # ---------- 6. WOW: Batch × Week — most frequent modal bucket that week ----------
    wow_data = modal.merge(batch_meta[['Batch', 'module']], on='Batch', how='inner')
    wow_pv = wow_data.groupby(['Batch', 'week_no'])['modal_bucket'].agg(mode_max).reset_index()
    wow_pv = wow_pv.pivot(index='Batch', columns='week_no', values='modal_bucket')
    wow_pv.columns = [f'Week {int(c)}' for c in wow_pv.columns]
    wow_pv = wow_pv.reindex(batch_order)

    wow = batch_meta[['Batch', 'module']].merge(wow_pv.reset_index(), on='Batch', how='right')
    wow = wow.rename(columns={'module': 'Module'})
    wcols = [c for c in wow.columns if c.startswith('Week ')]
    for wc in wcols:
        wow[wc] = wow[wc].apply(lambda x: BUCKET_LABELS.get(int(x), x) if pd.notna(x) else np.nan)
    wow = wow[['Batch', 'Module'] + wcols]
    print(f"M05 WOW (most freq modal bucket per week): {wow.shape}")

    # ---------- 7. INSTRUCTOR-GROUPED VIEWS ----------
    meta = skel[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    # Instructor DOD: same "bucket, %" per class per instructor
    instr_modal = modal.merge(skel[['lecture_id', 'row_label']].drop_duplicates('lecture_id'), on='lecture_id', how='left')
    instr_class_agg = (instr_modal.groupby(['row_label', 'class_no'])
                                  .agg(modal_bucket=('modal_bucket', mode_max),
                                       modal_pct=('modal_pct', 'mean'))
                                  .round({'modal_pct': 1})
                                  .reset_index())
    instr_class_agg['cell_label'] = instr_class_agg.apply(
        lambda r: f"{BUCKET_LABELS[int(r['modal_bucket'])]}, {r['modal_pct']}%"
        if pd.notna(r['modal_bucket']) else np.nan, axis=1
    )

    coc_instr_pv = instr_class_agg.pivot_table(index='row_label', columns='class_no',
                                                values='cell_label', aggfunc='first')
    coc_instr_pv.columns = [f'Class {int(c)}' for c in coc_instr_pv.columns]

    coc_full = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
        coc_instr_pv.reset_index(), on='row_label', how='left')
    coc_full = coc_full.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
    iccols = [c for c in coc_full.columns if c.startswith('Class ')]
    coc_full = coc_full[['Batch', 'Instructor', 'Module'] + iccols]

    # Instructor WOW
    wow_instr = instr_modal.groupby(['row_label', 'week_no'])['modal_bucket'].agg(mode_max).reset_index()
    wow_instr_pv = wow_instr.pivot(index='row_label', columns='week_no', values='modal_bucket')
    wow_instr_pv.columns = [f'Week {int(c)}' for c in wow_instr_pv.columns]

    wow_full = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
        wow_instr_pv.reset_index(), on='row_label', how='left')
    wow_full = wow_full.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
    iwcols = [c for c in wow_full.columns if c.startswith('Week ')]
    for wc in iwcols:
        wow_full[wc] = wow_full[wc].apply(lambda x: BUCKET_LABELS.get(int(x), x) if pd.notna(x) else np.nan)
    wow_full = wow_full[['Batch', 'Instructor', 'Module'] + iwcols]

    def instructor_view(flat_df, period_cols):
        rows = []
        for instr in flat_df['Instructor'].drop_duplicates().tolist():
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))
            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols: header[c] = ''
            rows.append(header)
            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols: sub[c] = r[c]
                rows.append(sub)
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, iccols)
    wow_instructor = instructor_view(wow_full, iwcols)
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 8. WRITE — 4 tabs only ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)
    def get_or_create_ws(book, title, rows=400, cols=80):
        try:
            ws = book.worksheet(title); ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M05 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 38
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import requests
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ========================================================================
    # STEP 0 — FETCH RAW PER-LECTURE AVG TIME FROM METABASE (M06 query)
    #   Replace CARD_ID with the Metabase card id you save m06_avg_time_query.sql as.
    # ========================================================================
    CARD_ID = 11426  # <-- set this to your saved M06 card id

    res = requests.post(
        f'{METABASE_BASE}/api/card/{CARD_ID}/query/json',
        headers=METABASE_HEADERS
    )
    metric = pd.DataFrame(res.json())   # columns: lecture_id, course_id, batch_name, attendees, avg_time_in_session_mins

    # ============================ CONFIG ============================
    SKELETON_SHEET_NAME = 'Calendar'
    SKELETON_WORKSHEET  = 'Lecture_Quality'

    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB = 'M06 Avg Time in Session (DOD)'
    WOW_TAB = 'M06 Avg Time in Session (WOW)'
    DOD_INSTRUCTOR_TAB = 'M06 Avg Time in Session Instructor (DOD)'
    WOW_INSTRUCTOR_TAB = 'M06 Avg Time in Session Instructor (WOW)'

    METRIC_VALUE_COL   = 'avg_time_in_session_mins'
    METRIC_LECTURE_COL = 'lecture_id'

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    WOW_AGG = 'mean'
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str): return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m: return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4: year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2: return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def assign_module(b):
        if not isinstance(b, str): return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I): return label
        return None

    def read_sheet(name, tab):
        raw = safe_open_sheet(name).worksheet(tab).get_all_values()
        return pd.DataFrame(raw[1:], columns=raw[0])

    # ---------- 1. BUILD THE LECTURE SKELETON ----------
    skel = read_sheet(SKELETON_SHEET_NAME, SKELETON_WORKSHEET)
    skel['date'] = pd.to_datetime(skel['date'], errors='coerce')
    skel['lecture_id'] = pd.to_numeric(skel['lecture_id'], errors='coerce').astype('Int64')

    skel['module']      = skel['Batch'].apply(assign_module)
    skel['batch_month'] = skel['Batch'].apply(parse_batch_month)
    skel = skel[skel['module'].notna() & (skel['batch_month'] >= CUTOFF_MONTH)].copy()
    skel['instructor_name'] = skel['instructor_name'].fillna('Unknown').str.strip()
    skel['row_label']       = skel['Batch'] + '  —  ' + skel['instructor_name']

    lo = (skel[['Batch', 'lecture_id', 'date']].drop_duplicates()
            .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    skel = skel.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    skel['week_no'] = ((skel['date'] - skel.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    # ---------- 2. JOIN THE METRIC ----------
    metric[METRIC_LECTURE_COL] = pd.to_numeric(metric[METRIC_LECTURE_COL], errors='coerce').astype('Int64')
    metric[METRIC_VALUE_COL]   = pd.to_numeric(metric[METRIC_VALUE_COL], errors='coerce')

    data = skel.merge(metric[[METRIC_LECTURE_COL, METRIC_VALUE_COL]],
                      left_on='lecture_id', right_on=METRIC_LECTURE_COL, how='left')
    data = data.rename(columns={METRIC_VALUE_COL: 'metric'})
    print(f"Skeleton lectures: {skel['lecture_id'].nunique()} | matched a metric value: {data['metric'].notna().sum()}")

    if MIN_CLASSES_PER_ROW > 1:
        counts = data.groupby('row_label')['lecture_id'].nunique()
        data = data[data['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 3. SECTION 1 — BATCH VIEWS, grouped by Batch only (instructors merged) ----------
    # Multiple instructors teaching the same batch are now ONE row. Where more than
    # one instructor has a value for the same class/week, the cell is the mean of
    # their values (skipna). dropna=False on the pivot ensures a batch with zero
    # matched metric values still shows up as a blank row instead of disappearing.
    batch_meta = data[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    def flat_table_by_batch(period_col, prefix, agg):
        vals = data.pivot_table(index='Batch', columns=period_col, values='metric', aggfunc=agg, dropna=False).round(1)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        vals = vals.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch('class_no', 'Class', 'mean')
    wow = flat_table_by_batch('week_no',  'Week',  WOW_AGG)
    print(f"M06 DOD: {coc.shape} | M06 WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in data before pivot: {data[data['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 3b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = data[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    def flat_table_by_instructor(period_col, prefix, agg):
        vals = data.pivot_table(index='row_label', columns=period_col, values='metric', aggfunc=agg).round(1)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor('class_no', 'Class', 'first')
    wow_full = flat_table_by_instructor('week_no',  'Week',  WOW_AGG)

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()
        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)
            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 4. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)
    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title); ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M06 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 39
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import requests
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ========================================================================
    # STEP 0 — FETCH RAW PER-STUDENT ATTENDANCE FROM METABASE (M07 query)
    #   Replace CARD_ID with the Metabase card id you save m07_retention_query.sql as.
    # ========================================================================
    CARD_ID = 11414  # <-- set this to your saved M07 card id

    res = requests.post(
        f'{METABASE_BASE}/api/card/{CARD_ID}/query/json',
        headers=METABASE_HEADERS
    )
    raw_attendance = pd.DataFrame(res.json())   # columns: lecture_id, course_id, batch_name, user_id

    # ========================================================================
    # CONFIG
    # ========================================================================
    SKELETON_SHEET_NAME = 'Calendar'
    SKELETON_WORKSHEET  = 'Lecture_Quality'

    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB = 'M07 Retention (DOD)'
    WOW_TAB = 'M07 Retention (WOW)'
    DOD_INSTRUCTOR_TAB = 'M07 Retention Instructor (DOD)'
    WOW_INSTRUCTOR_TAB = 'M07 Retention Instructor (WOW)'

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ========================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str):
            return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m:
            return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4:
            year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2:
                return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def assign_module(b):
        if not isinstance(b, str):
            return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I):
                return label
        return None

    def read_sheet(name, tab):
        raw = safe_open_sheet(name).worksheet(tab).get_all_values()
        return pd.DataFrame(raw[1:], columns=raw[0])

    # ---------- 1. BUILD THE LECTURE SKELETON (class_no / week_no per batch) ----------
    skel = read_sheet(SKELETON_SHEET_NAME, SKELETON_WORKSHEET)
    skel['date'] = pd.to_datetime(skel['date'], errors='coerce')
    skel['lecture_id'] = pd.to_numeric(skel['lecture_id'], errors='coerce').astype('Int64')

    skel['module'] = skel['Batch'].apply(assign_module)
    skel['batch_month'] = skel['Batch'].apply(parse_batch_month)
    skel = skel[skel['module'].notna() & (skel['batch_month'] >= CUTOFF_MONTH)].copy()
    skel['instructor_name'] = skel['instructor_name'].fillna('Unknown').str.strip()
    skel['row_label'] = skel['Batch'] + '  —  ' + skel['instructor_name']

    lo = (skel[['Batch', 'lecture_id', 'date']].drop_duplicates()
            .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    skel = skel.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    skel['week_no'] = ((skel['date'] - skel.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    # lecture_id -> (Batch, class_no, week_no) lookup, restricted to in-scope lectures
    lec_lookup = skel[['lecture_id', 'Batch', 'class_no', 'week_no']].drop_duplicates('lecture_id')

    # ---------- 2. ATTACH class_no / week_no TO RAW ATTENDANCE ----------
    raw_attendance['lecture_id'] = pd.to_numeric(raw_attendance['lecture_id'], errors='coerce').astype('Int64')
    att = raw_attendance.merge(lec_lookup, on='lecture_id', how='inner')   # inner = keep only in-scope lectures

    # ---------- 3. CLASS-ON-CLASS RETENTION: class N -> class N+1, per batch ----------
    # NOTE: retention is computed per BATCH (not per instructor) — it's a cohort-wide
    # stat, so Section 1 below pivots directly off this without any instructor merge.
    def retention_pairs(att, period_col):
        """Returns DataFrame: Batch, period (the *starting* period of the pair), retention_pct."""
        rows = []
        for batch, g in att.groupby('Batch'):
            periods = sorted(g[period_col].dropna().unique())
            users_by_period = {p: set(g.loc[g[period_col] == p, 'user_id']) for p in periods}
            for i in range(len(periods) - 1):
                p_now, p_next = periods[i], periods[i + 1]
                now_users, next_users = users_by_period[p_now], users_by_period[p_next]
                if len(now_users) == 0:
                    continue
                retained = len(now_users & next_users)
                pct = round(100.0 * retained / len(now_users), 1)
                rows.append({'Batch': batch, period_col: p_now, 'retention_pct': pct})
        return pd.DataFrame(rows)

    coc_pairs = retention_pairs(att, 'class_no')
    wow_pairs = retention_pairs(att, 'week_no')

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 4. SECTION 1 — BATCH VIEWS (already batch-level; dropna=False keeps every batch) ----------
    batch_meta = skel[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    if MIN_CLASSES_PER_ROW > 1:
        counts = skel.groupby('Batch')['lecture_id'].nunique()
        batch_meta = batch_meta[batch_meta['Batch'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    def flat_table_by_batch(pairs, period_col, prefix):
        vals = pairs.pivot_table(index='Batch', columns=period_col,
                                  values='retention_pct', aggfunc='mean', dropna=False).round(1)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        vals = vals.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch(coc_pairs, 'class_no', 'Class')
    wow = flat_table_by_batch(wow_pairs, 'week_no', 'Week')
    print(f"M07 DOD: {coc.shape} | M07 WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in skeleton before pivot: {skel[skel['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 4b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = skel[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    if MIN_CLASSES_PER_ROW > 1:
        counts = skel.groupby('row_label')['lecture_id'].nunique()
        meta = meta[meta['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    def flat_table_by_instructor(pairs, period_col, prefix):
        pairs = pairs.merge(meta[['Batch', 'row_label']].drop_duplicates(), on='Batch', how='inner')
        vals = pairs.pivot_table(index='row_label', columns=period_col,
                                  values='retention_pct', aggfunc='first')
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor(coc_pairs, 'class_no', 'Class')
    wow_full = flat_table_by_instructor(wow_pairs, 'week_no', 'Week')

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()
        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)
            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 5. WRITE TO OUTPUT SHEET ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)

    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M07 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 40
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import requests
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ========================================================================
    # STEP 0 — FETCH RAW PER-STUDENT ATTENDANCE FROM METABASE
    #   Reuses the SAME card as M07 (m07_retention_query.sql) — one row per
    #   (lecture_id, course_id, batch_name, user_id) who was live-present.
    # ========================================================================
    CARD_ID = 11427  # <-- same M07 card id

    res = requests.post(
        f'{METABASE_BASE}/api/card/{CARD_ID}/query/json',
        headers=METABASE_HEADERS
    )
    raw_attendance = pd.DataFrame(res.json())

    # ============================ CONFIG ============================
    SKELETON_SHEET_NAME = 'Calendar'
    SKELETON_WORKSHEET  = 'Lecture_Quality'

    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB = 'M08 Cohort Retention (DOD)'
    WOW_TAB = 'M08 Cohort Retention (WOW)'
    DOD_INSTRUCTOR_TAB = 'M08 Cohort Retention Instructor (DOD)'
    WOW_INSTRUCTOR_TAB = 'M08 Cohort Retention Instructor (WOW)'

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str): return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m: return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4: year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2: return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def assign_module(b):
        if not isinstance(b, str): return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I): return label
        return None

    def read_sheet(name, tab):
        raw = safe_open_sheet(name).worksheet(tab).get_all_values()
        return pd.DataFrame(raw[1:], columns=raw[0])

    # ---------- 1. BUILD THE LECTURE SKELETON (class_no / week_no per batch) ----------
    skel = read_sheet(SKELETON_SHEET_NAME, SKELETON_WORKSHEET)
    skel['date'] = pd.to_datetime(skel['date'], errors='coerce')
    skel['lecture_id'] = pd.to_numeric(skel['lecture_id'], errors='coerce').astype('Int64')

    skel['module']      = skel['Batch'].apply(assign_module)
    skel['batch_month'] = skel['Batch'].apply(parse_batch_month)
    skel = skel[skel['module'].notna() & (skel['batch_month'] >= CUTOFF_MONTH)].copy()
    skel['instructor_name'] = skel['instructor_name'].fillna('Unknown').str.strip()
    skel['row_label']       = skel['Batch'] + '  —  ' + skel['instructor_name']

    lo = (skel[['Batch', 'lecture_id', 'date']].drop_duplicates()
            .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    skel = skel.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    skel['week_no'] = ((skel['date'] - skel.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    lec_lookup = skel[['lecture_id', 'Batch', 'class_no', 'week_no']].drop_duplicates('lecture_id')

    # ---------- 2. ATTACH class_no / week_no TO RAW ATTENDANCE ----------
    raw_attendance['lecture_id'] = pd.to_numeric(raw_attendance['lecture_id'], errors='coerce').astype('Int64')
    att = raw_attendance.merge(lec_lookup, on='lecture_id', how='inner')

    # ---------- 3. CUMULATIVE COHORT RETENTION: baseline = Session 1 / Week 1 ----------
    # NOTE: computed per BATCH (not per instructor) — Section 1 pivots directly off
    # this without needing an instructor merge.
    def cumulative_retention(att, period_col):
        """Returns DataFrame: Batch, period, retention_pct (% of period-1 baseline still present)."""
        rows = []
        for batch, g in att.groupby('Batch'):
            periods = sorted(g[period_col].dropna().unique())
            if not periods:
                continue
            baseline_period = periods[0]
            baseline_users = set(g.loc[g[period_col] == baseline_period, 'user_id'])
            if len(baseline_users) == 0:
                continue
            for p in periods:
                present_users = set(g.loc[g[period_col] == p, 'user_id'])
                still_here = len(baseline_users & present_users)
                pct = round(100.0 * still_here / len(baseline_users), 1)
                rows.append({'Batch': batch, period_col: p, 'retention_pct': pct})
        return pd.DataFrame(rows)

    coc_vals = cumulative_retention(att, 'class_no')
    wow_vals = cumulative_retention(att, 'week_no')

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 4. SECTION 1 — BATCH VIEWS (already batch-level; dropna=False keeps every batch) ----------
    batch_meta = skel[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    if MIN_CLASSES_PER_ROW > 1:
        counts = skel.groupby('Batch')['lecture_id'].nunique()
        batch_meta = batch_meta[batch_meta['Batch'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    def flat_table_by_batch(vals, period_col, prefix):
        pv = vals.pivot_table(index='Batch', columns=period_col,
                               values='retention_pct', aggfunc='mean', dropna=False).round(1)
        pv.columns = [f'{prefix} {int(c)}' for c in pv.columns]
        pv = pv.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(pv.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch(coc_vals, 'class_no', 'Class')   # Class 1 column should read 100 for every row (baseline)
    wow = flat_table_by_batch(wow_vals, 'week_no', 'Week')     # Week 1 column should read 100 for every row (baseline)
    print(f"M08 DOD: {coc.shape} | M08 WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in skeleton before pivot: {skel[skel['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 4b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = skel[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    if MIN_CLASSES_PER_ROW > 1:
        counts = skel.groupby('row_label')['lecture_id'].nunique()
        meta = meta[meta['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    def flat_table_by_instructor(vals, period_col, prefix):
        vals = vals.merge(meta[['Batch', 'row_label']].drop_duplicates(), on='Batch', how='inner')
        pv = vals.pivot_table(index='row_label', columns=period_col, values='retention_pct', aggfunc='first')
        pv.columns = [f'{prefix} {int(c)}' for c in pv.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            pv.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor(coc_vals, 'class_no', 'Class')
    wow_full = flat_table_by_instructor(wow_vals, 'week_no', 'Week')

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()
        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)
            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 5. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)
    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title); ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M08 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 41
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import requests
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ========================================================================
    # STEP 0 — FETCH RAW PER-STUDENT ATTENDANCE FROM METABASE
    #   Reuses the SAME card as M07/M08 — one row per
    #   (lecture_id, course_id, batch_name, user_id) who was live-present.
    # ========================================================================
    CARD_ID = 11427  # <-- same M07/M08 card id

    res = requests.post(
        f'{METABASE_BASE}/api/card/{CARD_ID}/query/json',
        headers=METABASE_HEADERS
    )
    raw_attendance = pd.DataFrame(res.json())

    # ============================ CONFIG ============================
    SKELETON_SHEET_NAME = 'Calendar'
    SKELETON_WORKSHEET  = 'Lecture_Quality'

    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB = 'M09 Early Drop-Out (DOD)'
    WOW_TAB = 'M09 Early Drop-Out (WOW)'
    DOD_INSTRUCTOR_TAB = 'M09 Early Drop-Out Instructor (DOD)'
    WOW_INSTRUCTOR_TAB = 'M09 Early Drop-Out Instructor (WOW)'

    # Per definitions: DOD is a SINGLE value per batch (not a Class1..N grid) —
    # % of Session-1 attendees who were absent from BOTH Session 2 and Session 3.
    # Rows are ordered chronologically by cohort month so the trend reads top to
    # bottom. WOW is a *different* metric shape: % of ENROLLED students absent
    # in that week (denominator = enrolled, not the S1 baseline).
    # Both DOD and WOW are computed at the BATCH level already (no per-instructor
    # math involved), so Section 1 needs no instructor merge/averaging — just drop
    # the Instructor column from the final output.
    ENROLLED_COL = 'batch_strength'   # used as the enrolled denominator for the WOW absence rate

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str): return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m: return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4: year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2: return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def assign_module(b):
        if not isinstance(b, str): return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I): return label
        return None

    def read_sheet(name, tab):
        raw = safe_open_sheet(name).worksheet(tab).get_all_values()
        return pd.DataFrame(raw[1:], columns=raw[0])

    # ---------- 1. BUILD THE LECTURE SKELETON (class_no / week_no per batch) ----------
    skel = read_sheet(SKELETON_SHEET_NAME, SKELETON_WORKSHEET)
    skel['date'] = pd.to_datetime(skel['date'], errors='coerce')
    skel['lecture_id'] = pd.to_numeric(skel['lecture_id'], errors='coerce').astype('Int64')
    skel[ENROLLED_COL] = pd.to_numeric(skel[ENROLLED_COL], errors='coerce')

    skel['module']      = skel['Batch'].apply(assign_module)
    skel['batch_month'] = skel['Batch'].apply(parse_batch_month)
    skel = skel[skel['module'].notna() & (skel['batch_month'] >= CUTOFF_MONTH)].copy()
    skel['instructor_name'] = skel['instructor_name'].fillna('Unknown').str.strip()
    skel['row_label']       = skel['Batch'] + '  —  ' + skel['instructor_name']

    lo = (skel[['Batch', 'lecture_id', 'date']].drop_duplicates()
            .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    skel = skel.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    skel['week_no'] = ((skel['date'] - skel.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    lec_lookup = skel[['lecture_id', 'Batch', 'class_no', 'week_no']].drop_duplicates('lecture_id')

    # enrolled count per batch: take the max batch_strength seen (most stable proxy for "enrolled")
    enrolled_per_batch = skel.groupby('Batch')[ENROLLED_COL].max().rename('enrolled')

    # ---------- 2. ATTACH class_no / week_no TO RAW ATTENDANCE ----------
    raw_attendance['lecture_id'] = pd.to_numeric(raw_attendance['lecture_id'], errors='coerce').astype('Int64')
    att = raw_attendance.merge(lec_lookup, on='lecture_id', how='inner')

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 3. SECTION 1 — BATCH VIEWS (already batch-level; no Instructor column) ----------
    batch_meta = skel[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    # DOD — single early drop-out % per batch
    dropout_rows = []
    for batch, g in att.groupby('Batch'):
        s1_users = set(g.loc[g['class_no'] == 1, 'user_id'])
        if not s1_users:
            continue
        s2_users = set(g.loc[g['class_no'] == 2, 'user_id'])
        s3_users = set(g.loc[g['class_no'] == 3, 'user_id'])
        absent_both = s1_users - (s2_users | s3_users)
        pct = round(100.0 * len(absent_both) / len(s1_users), 1)
        dropout_rows.append({'Batch': batch, 'Early Drop-Out %': pct})
    dropout_df = pd.DataFrame(dropout_rows)

    coc = batch_meta[['Batch', 'module']].merge(dropout_df, on='Batch', how='left')
    coc = coc.rename(columns={'module': 'Module'})
    coc['Batch'] = pd.Categorical(coc['Batch'], categories=batch_order, ordered=True)
    coc = coc.sort_values('Batch').reset_index(drop=True)
    coc = coc[['Batch', 'Module', 'Early Drop-Out %']]
    print(f"M09 DOD (single value per batch): {coc.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in skeleton before pivot: {skel[skel['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # WOW — weekly absence rate vs ENROLLED (not vs S1 baseline)
    present_by_week = att.groupby(['Batch', 'week_no'])['user_id'].nunique().rename('present').reset_index()
    present_by_week = present_by_week.merge(enrolled_per_batch, on='Batch', how='left')
    present_by_week['absence_pct'] = np.where(
        present_by_week['enrolled'] > 0,
        ((1 - present_by_week['present'] / present_by_week['enrolled']) * 100).round(1),
        np.nan
    )

    pv = present_by_week.pivot_table(index='Batch', columns='week_no', values='absence_pct', aggfunc='mean', dropna=False)
    pv.columns = [f'Week {int(c)}' for c in pv.columns]
    pv = pv.reindex(batch_order)
    wow = batch_meta[['Batch', 'module']].merge(pv.reset_index(), on='Batch', how='right')
    wow = wow.rename(columns={'module': 'Module'})
    wcols = [c for c in wow.columns if c.startswith('Week ')]
    wow['Avg'] = wow[wcols].mean(axis=1, skipna=True).round(1)
    wow = wow[['Batch', 'Module'] + wcols + ['Avg']]
    print(f"M09 WOW (weekly absence %): {wow.shape}")

    # ---------- 3b. SECTION 2 — INSTRUCTOR-GROUPED VIEWS ----------
    # NOTE: M09's DOD table has NO Class N columns (it's a single value per batch),
    # so its instructor view just groups the single 'Early Drop-Out %' column instead
    # of a Class/Week grid. The WOW instructor view follows the normal Week N pattern.
    meta = skel[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    coc_full = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(dropout_df, on='Batch', how='left')
    coc_full = coc_full.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})

    wow_full_pv = present_by_week.pivot_table(index='Batch', columns='week_no', values='absence_pct', aggfunc='mean')
    wow_full_pv.columns = [f'Week {int(c)}' for c in wow_full_pv.columns]
    wow_full = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
        wow_full_pv.reset_index(), on='Batch', how='left')
    wow_full = wow_full.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
    wfcols = [c for c in wow_full.columns if c.startswith('Week ')]
    wow_full['Avg'] = wow_full[wfcols].mean(axis=1, skipna=True).round(1)

    def instructor_view_single_value(flat_df, value_col):
        """For DOD: one metric column instead of a Class/Week grid."""
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()
        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))
            rows.append({'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules, value_col: ''})
            for _, r in g.iterrows():
                rows.append({'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module'], value_col: r[value_col]})
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module', value_col]]

    def instructor_view_grid(flat_df, prefix):
        """For WOW: normal Week N / Avg grid, same pattern as other metrics."""
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()
        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)
            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view_single_value(coc_full, 'Early Drop-Out %')
    wow_instructor = instructor_view_grid(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 4. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)
    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title); ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M09 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 42
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ============================ CONFIG ============================
    INPUT_SHEET_NAME = 'Calendar'
    INPUT_WORKSHEET  = 'Lecture_Quality'
    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB = 'M10 Question Attempt (DOD)'
    WOW_TAB = 'M10 Question Attempt (WOW)'
    DOD_INSTRUCTOR_TAB = 'M10 Question Attempt Instructor (DOD)'
    WOW_INSTRUCTOR_TAB = 'M10 Question Attempt Instructor (WOW)'

    # M10 = % of enrolled students who attempted the in-class question within 24hrs.
    # The skeleton sheet doesn't have a strict "attempted within 24hr" column — only:
    #   batch_attempt_rate            -> % who attempted (NOT time-bound)
    #   batch_completion_rate_on_time -> % who completed within the on-time/24hr window
    # 'on_time' completion is the closer match to the M10 definition, so that's the
    # default metric column below. Swap to 'batch_attempt_rate' in one line if you'd
    # rather track raw attempts regardless of timing.
    METRIC_COL = 'batch_completion_rate_on_time'

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str):
            return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m:
            return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4:
            year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2:
                return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def assign_module(b):
        if not isinstance(b, str):
            return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I):
                return label
        return None

    # ---------- 1. READ INPUT ----------
    ws_in = safe_open_sheet(INPUT_SHEET_NAME).worksheet(INPUT_WORKSHEET)
    raw = ws_in.get_all_values()
    df = pd.DataFrame(raw[1:], columns=raw[0])
    print(f"Read input: {df.shape[0]} rows")

    # ---------- 2. CLEAN + MODULE/MONTH TAG ----------
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['lecture_id'] = pd.to_numeric(df['lecture_id'], errors='coerce').astype('Int64')
    df[METRIC_COL] = pd.to_numeric(df[METRIC_COL], errors='coerce')

    df['module'] = df['Batch'].apply(assign_module)
    df['batch_month'] = df['Batch'].apply(parse_batch_month)
    df['instructor_name'] = df['instructor_name'].fillna('Unknown').str.strip()

    allsub = df[df['module'].notna() & (df['batch_month'] >= CUTOFF_MONTH)].copy()
    allsub = allsub.drop_duplicates(['Batch', 'lecture_id', 'instructor_name'])

    # ---------- 3. class_no / week_no PER BATCH ----------
    lo = (allsub[['Batch', 'lecture_id', 'date']]
          .drop_duplicates()
          .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    allsub = allsub.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    allsub['week_no'] = ((allsub['date'] - allsub.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    allsub['row_label'] = allsub['Batch'] + '  —  ' + allsub['instructor_name']

    if MIN_CLASSES_PER_ROW > 1:
        counts = allsub.groupby('row_label')['lecture_id'].nunique()
        allsub = allsub[allsub['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 4. SECTION 1 — BATCH VIEWS, grouped by Batch only (instructors merged) ----------
    # Multiple instructors teaching the same batch are now ONE row. Where more than
    # one instructor has a value for the same class/week, the cell is the mean of
    # their values (skipna). dropna=False on the pivot ensures a batch with zero
    # matched metric values still shows up as a blank row instead of disappearing.
    batch_meta = allsub[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    def flat_table_by_batch(period_col, prefix, agg):
        vals = allsub.pivot_table(index='Batch', columns=period_col,
                                   values=METRIC_COL, aggfunc=agg, dropna=False).round(1)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        vals = vals.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch('class_no', 'Class', 'mean')
    wow = flat_table_by_batch('week_no',  'Week',  'mean')
    print(f"M10 DOD: {coc.shape} | M10 WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in data before pivot: {allsub[allsub['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 4b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = allsub[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    def flat_table_by_instructor(period_col, prefix, agg):
        vals = allsub.pivot_table(index='row_label', columns=period_col,
                                   values=METRIC_COL, aggfunc=agg).round(1)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor('class_no', 'Class', 'first')
    wow_full = flat_table_by_instructor('week_no',  'Week',  'mean')

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()
        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)
            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 5. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)

    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M10 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 43
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ============================ CONFIG ============================
    INPUT_SHEET_NAME = 'Calendar'
    INPUT_WORKSHEET  = 'Lecture_Quality'
    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB = 'M11 CSAT (DOD)'
    WOW_TAB = 'M11 CSAT (WOW)'
    DOD_INSTRUCTOR_TAB = 'M11 CSAT Instructor (DOD)'
    WOW_INSTRUCTOR_TAB = 'M11 CSAT Instructor (WOW)'

    # M11 = Avg post-session rating (1-5). Valid only if >= 30% of live attendees
    # submitted a rating (total_users_filled / live_attendees >= 0.30). Lectures
    # below that response threshold are blanked out rather than included, since
    # the definitions sheet explicitly calls them unreliable below 30%.
    RATING_COL       = 'lecture_rating_out_of_five'
    RESPONDERS_COL   = 'total_users_filled'
    LIVE_ATTEND_COL  = 'live_attendees'
    MIN_RESPONSE_PCT = 0.30

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str):
            return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m:
            return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4:
            year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2:
                return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def assign_module(b):
        if not isinstance(b, str):
            return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I):
                return label
        return None

    # ---------- 1. READ INPUT ----------
    ws_in = safe_open_sheet(INPUT_SHEET_NAME).worksheet(INPUT_WORKSHEET)
    raw = ws_in.get_all_values()
    df = pd.DataFrame(raw[1:], columns=raw[0])
    print(f"Read input: {df.shape[0]} rows")

    # ---------- 2. CLEAN + VALIDITY FILTER + MODULE/MONTH TAG ----------
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['lecture_id'] = pd.to_numeric(df['lecture_id'], errors='coerce').astype('Int64')
    df[RATING_COL]      = pd.to_numeric(df[RATING_COL], errors='coerce')
    df[RESPONDERS_COL]  = pd.to_numeric(df[RESPONDERS_COL], errors='coerce')
    df[LIVE_ATTEND_COL] = pd.to_numeric(df[LIVE_ATTEND_COL], errors='coerce')

    df['response_rate'] = np.where(
        df[LIVE_ATTEND_COL] > 0,
        df[RESPONDERS_COL] / df[LIVE_ATTEND_COL],
        np.nan
    )
    # Blank out the rating itself wherever response rate is below threshold
    df['csat_valid'] = np.where(df['response_rate'] >= MIN_RESPONSE_PCT, df[RATING_COL], np.nan)

    df['module'] = df['Batch'].apply(assign_module)
    df['batch_month'] = df['Batch'].apply(parse_batch_month)
    df['instructor_name'] = df['instructor_name'].fillna('Unknown').str.strip()

    allsub = df[df['module'].notna() & (df['batch_month'] >= CUTOFF_MONTH)].copy()
    allsub = allsub.drop_duplicates(['Batch', 'lecture_id', 'instructor_name'])

    n_invalid = ((allsub['response_rate'] < MIN_RESPONSE_PCT) & allsub[RATING_COL].notna()).sum()
    print(f"Lectures excluded for <{int(MIN_RESPONSE_PCT*100)}% feedback response rate: {n_invalid}")

    # ---------- 3. class_no / week_no PER BATCH ----------
    lo = (allsub[['Batch', 'lecture_id', 'date']]
          .drop_duplicates()
          .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    allsub = allsub.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    allsub['week_no'] = ((allsub['date'] - allsub.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    allsub['row_label'] = allsub['Batch'] + '  —  ' + allsub['instructor_name']

    if MIN_CLASSES_PER_ROW > 1:
        counts = allsub.groupby('row_label')['lecture_id'].nunique()
        allsub = allsub[allsub['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 4. SECTION 1 — BATCH VIEWS, grouped by Batch only (instructors merged) ----------
    # Multiple instructors teaching the same batch are now ONE row. Where more than
    # one instructor has a value for the same class/week, the cell is the mean of
    # their values (skipna). dropna=False on the pivot ensures a batch with zero
    # matched metric values still shows up as a blank row instead of disappearing.
    batch_meta = allsub[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    def flat_table_by_batch(period_col, prefix, agg):
        vals = allsub.pivot_table(index='Batch', columns=period_col,
                                   values='csat_valid', aggfunc=agg, dropna=False).round(2)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        vals = vals.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(2)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch('class_no', 'Class', 'mean')   # DOD: avg rating for that class across instructors
    wow = flat_table_by_batch('week_no',  'Week',  'mean')   # WOW: avg rating across the week's sessions
    print(f"M11 DOD: {coc.shape} | M11 WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in data before pivot: {allsub[allsub['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 4b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = allsub[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    def flat_table_by_instructor(period_col, prefix, agg):
        vals = allsub.pivot_table(index='row_label', columns=period_col,
                                   values='csat_valid', aggfunc=agg).round(2)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(2)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor('class_no', 'Class', 'first')
    wow_full = flat_table_by_instructor('week_no',  'Week',  'mean')

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()
        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)
            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 5. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)

    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M11 DONE ✓ ===")

    # ──────────────────────────────────────────────────────────────────────
    # Cell 44
    # ──────────────────────────────────────────────────────────────────────
    import pandas as pd
    import numpy as np
    import re
    import gspread
    from gspread_dataframe import set_with_dataframe

    # ============================ CONFIG ============================
    INPUT_SHEET_NAME = 'Calendar'
    INPUT_WORKSHEET  = 'Lecture_Quality'
    OUTPUT_SHEET_KEY = '1oz_ZNFnMa5FPs1Hd5jr2s9RJAgDFLjUVjxuLH9P-kk0'
    DOD_TAB = 'M12 Feedback Response (DOD)'
    WOW_TAB = 'M12 Feedback Response (WOW)'
    DOD_INSTRUCTOR_TAB = 'M12 Feedback Response Instructor (DOD)'
    WOW_INSTRUCTOR_TAB = 'M12 Feedback Response Instructor (WOW)'

    # M12 = % of live attendees who submitted a post-session rating.
    # Same numerator/denominator as the response_rate check used to validate M11.
    RESPONDERS_COL   = 'total_users_filled'
    LIVE_ATTEND_COL  = 'overall_viewers'

    MODULES = {'SQL': r'DS SQL', 'Spreadsheets': r'DS Spreadsheet', 'Power BI': r'DS Power ?BI', 'Python': r'DS Python', 'EDA 1': r'DS EDA 1', 'EDA 2': r'DS EDA 2'}
    CUTOFF_MONTH = pd.Timestamp('2025-12-01')
    MIN_CLASSES_PER_ROW = 1
    # ================================================================

    MONTHS = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
              'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}

    def parse_batch_month(b):
        if not isinstance(b, str):
            return pd.NaT
        m = re.search(r'(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*', b.lower())
        if not m:
            return pd.NaT
        month = MONTHS[m.group(1)]
        y4 = re.search(r'20\d{2}', b)
        if y4:
            year = int(y4.group(0))
        else:
            y2 = re.search(r'\b(\d{2})\b', b[m.end():])
            if not y2:
                return pd.NaT
            year = 2000 + int(y2.group(1))
        return pd.Timestamp(year=year, month=month, day=1)

    def assign_module(b):
        if not isinstance(b, str):
            return None
        for label, pat in MODULES.items():
            if re.search(pat, b, flags=re.I):
                return label
        return None

    # ---------- 1. READ INPUT ----------
    ws_in = safe_open_sheet(INPUT_SHEET_NAME).worksheet(INPUT_WORKSHEET)
    raw = ws_in.get_all_values()
    df = pd.DataFrame(raw[1:], columns=raw[0])
    print(f"Read input: {df.shape[0]} rows")

    # ---------- 2. CLEAN + METRIC + MODULE/MONTH TAG ----------
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df['lecture_id'] = pd.to_numeric(df['lecture_id'], errors='coerce').astype('Int64')
    df[RESPONDERS_COL]  = pd.to_numeric(df[RESPONDERS_COL], errors='coerce')
    df[LIVE_ATTEND_COL] = pd.to_numeric(df[LIVE_ATTEND_COL], errors='coerce')

    df['response_rate'] = np.where(
        df[LIVE_ATTEND_COL] > 0,
        (df[RESPONDERS_COL] / df[LIVE_ATTEND_COL] * 100).round(1),
        np.nan
    )

    df['module'] = df['Batch'].apply(assign_module)
    df['batch_month'] = df['Batch'].apply(parse_batch_month)
    df['instructor_name'] = df['instructor_name'].fillna('Unknown').str.strip()

    allsub = df[df['module'].notna() & (df['batch_month'] >= CUTOFF_MONTH)].copy()
    allsub = allsub.drop_duplicates(['Batch', 'lecture_id', 'instructor_name'])

    # ---------- 3. class_no / week_no PER BATCH ----------
    lo = (allsub[['Batch', 'lecture_id', 'date']]
          .drop_duplicates()
          .sort_values(['Batch', 'date', 'lecture_id']))
    lo['class_no'] = lo.groupby('Batch').cumcount() + 1
    allsub = allsub.merge(lo[['Batch', 'lecture_id', 'class_no']], on=['Batch', 'lecture_id'], how='left')
    allsub['week_no'] = ((allsub['date'] - allsub.groupby('Batch')['date'].transform('min')).dt.days // 7) + 1

    allsub['row_label'] = allsub['Batch'] + '  —  ' + allsub['instructor_name']

    if MIN_CLASSES_PER_ROW > 1:
        counts = allsub.groupby('row_label')['lecture_id'].nunique()
        allsub = allsub[allsub['row_label'].isin(counts[counts >= MIN_CLASSES_PER_ROW].index)]

    module_rank = {m: i for i, m in enumerate(MODULES)}

    # ---------- 4. SECTION 1 — BATCH VIEWS, grouped by Batch only (instructors merged) ----------
    # Multiple instructors teaching the same batch are now ONE row. Where more than
    # one instructor has a value for the same class/week, the cell is the mean of
    # their values (skipna). dropna=False on the pivot ensures a batch with zero
    # matched metric values still shows up as a blank row instead of disappearing.
    batch_meta = allsub[['Batch', 'module', 'batch_month']].drop_duplicates('Batch').copy()
    batch_meta['mod_rank'] = batch_meta['module'].map(module_rank)
    batch_meta = batch_meta.sort_values(['mod_rank', 'batch_month', 'Batch'])
    batch_order = batch_meta['Batch'].tolist()

    def flat_table_by_batch(period_col, prefix, agg):
        vals = allsub.pivot_table(index='Batch', columns=period_col,
                                   values='response_rate', aggfunc=agg, dropna=False).round(1)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        vals = vals.reindex(batch_order)
        out = batch_meta[['Batch', 'module']].merge(vals.reset_index(), on='Batch', how='right')
        out = out.rename(columns={'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Module'] + pcols + ['Avg']]

    coc = flat_table_by_batch('class_no', 'Class', 'mean')   # DOD: each cell = that session's response rate
    wow = flat_table_by_batch('week_no',  'Week',  'mean')   # WOW: avg response rate across the week's sessions
    print(f"M12 DOD: {coc.shape} | M12 WOW: {wow.shape}")
    print(f"Modules present in DOD: {sorted(coc['Module'].dropna().unique())}")
    print(f"Power BI rows in data before pivot: {allsub[allsub['module']=='Power BI']['Batch'].nunique()} | in final DOD table: {coc[coc['Module']=='Power BI'].shape[0]}")

    # ---------- 4b. SECTION 2 — INSTRUCTOR-GROUPED VIEW (unchanged: Batch+Instructor granularity) ----------
    meta = allsub[['row_label', 'Batch', 'instructor_name', 'module', 'batch_month']].drop_duplicates('row_label').copy()
    meta['mod_rank'] = meta['module'].map(module_rank)
    meta = meta.sort_values(['mod_rank', 'batch_month', 'instructor_name'])

    def flat_table_by_instructor(period_col, prefix, agg):
        vals = allsub.pivot_table(index='row_label', columns=period_col,
                                   values='response_rate', aggfunc=agg).round(1)
        vals.columns = [f'{prefix} {int(c)}' for c in vals.columns]
        out = meta[['row_label', 'Batch', 'instructor_name', 'module']].merge(
            vals.reset_index(), on='row_label', how='left')
        out = out.rename(columns={'instructor_name': 'Instructor', 'module': 'Module'})
        pcols = [c for c in out.columns if c.startswith(prefix + ' ')]
        out['Avg'] = out[pcols].mean(axis=1, skipna=True).round(1)
        return out[['Batch', 'Instructor', 'Module'] + pcols + ['Avg']]

    coc_full = flat_table_by_instructor('class_no', 'Class', 'first')
    wow_full = flat_table_by_instructor('week_no',  'Week',  'mean')

    def instructor_view(flat_df, prefix):
        period_cols = [c for c in flat_df.columns if c.startswith(prefix + ' ')] + ['Avg']
        instructor_order = flat_df['Instructor'].drop_duplicates().tolist()
        rows = []
        for instr in instructor_order:
            g = flat_df[flat_df['Instructor'] == instr]
            instr_modules = ', '.join(sorted(g['Module'].dropna().unique()))

            header = {'Instructor — Batch': f'▸ {instr}', 'Module': instr_modules}
            for c in period_cols:
                header[c] = ''
            rows.append(header)
            for _, r in g.iterrows():
                sub = {'Instructor — Batch': f'    {r["Batch"]}', 'Module': r['Module']}
                for c in period_cols:
                    sub[c] = r[c]
                rows.append(sub)
        return pd.DataFrame(rows)[['Instructor — Batch', 'Module'] + period_cols]

    coc_instructor = instructor_view(coc_full, 'Class')
    wow_instructor = instructor_view(wow_full, 'Week')
    print(f"Instructor DOD: {coc_instructor.shape} | Instructor WOW: {wow_instructor.shape}")

    # ---------- 5. WRITE ----------
    out_book = safe_open_by_key(OUTPUT_SHEET_KEY)

    def get_or_create_ws(book, title, rows=400, cols=60):
        try:
            ws = book.worksheet(title)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = book.add_worksheet(title=title, rows=rows, cols=cols)
        return ws

    set_with_dataframe(get_or_create_ws(out_book, DOD_TAB), coc, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_TAB), wow, include_index=False, include_column_header=True)

    set_with_dataframe(get_or_create_ws(out_book, DOD_INSTRUCTOR_TAB), coc_instructor, include_index=False, include_column_header=True)
    set_with_dataframe(get_or_create_ws(out_book, WOW_INSTRUCTOR_TAB), wow_instructor, include_index=False, include_column_header=True)

    print("=== M12 DONE ✓ ===")

except Exception as e:
    print(f"❌ Pipeline failed: {e}")
    traceback.print_exc()
    sys.exit(1)

mins, secs = divmod(time.time() - start_time, 60)
print(f"\n🎯 Lecture Quality Views Pipeline completed successfully in {int(mins)}m {int(secs)}s")
sys.exit(0)
