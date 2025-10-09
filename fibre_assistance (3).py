import os
import json
import sqlite3
import hashlib
import sys
import traceback
import csv
import re
import tkinter as tk
from tkinter import ttk, messagebox, filedialog, StringVar
import tkinter.font as tkfont
import webbrowser
import requests
import threading
import tempfile
import atexit
import ctypes

# --- NEW/UPDATED: ADD after existing imports (BeautifulSoup already imported above) ---

# ---------- VMR crawler (embedded; not a module import) ----------
import time
from datetime import datetime
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# same base host you already use elsewhere
VMR_BASE = "https://cadprdwebw001.optus.com.au/vmr"

def _vmr_make_session(timeout=20, total_retries=3, backoff=0.5) -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=5, pool_maxsize=10)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({
        "User-Agent": "FibreAssist-VMR/1.0 (+python-requests)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    sess.request_timeout = timeout
    return sess

def _vmr_get_html(session: requests.Session, url: str, params=None) -> str:
    r = session.get(url, params=params, timeout=session.request_timeout)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def _vmr_crawl_fibretrace(vmr_numeric_id: str, out_dir: str = "_fibre_cache") -> Path:
    """Implements steps 2–7; returns saved HTML file path."""
    if not re.fullmatch(r"\d+", (vmr_numeric_id or "").strip()):
        raise ValueError("VMR ID must be numeric.")
    vmr_id = vmr_numeric_id.strip()

    sess = _vmr_make_session()

    # Step 2: Result.aspx
    result_url = f"{VMR_BASE}/Result.aspx"
    html2 = _vmr_get_html(sess, result_url, params={"keywords": f"70|{vmr_id}"})

    # Step 3: WorkFolder.aspx?id=<NUM>
    work_id = None
    s2 = BeautifulSoup(html2, _BS_PARSER)
    for a in s2.find_all("a", href=True):
        m = re.search(r"WorkFolder\.aspx\?id=(\d+)", a["href"], re.IGNORECASE)
        if m:
            work_id = m.group(1); break
    if not work_id:
        m = re.search(r"WorkFolder\.aspx\?id=(\d+)", html2, re.IGNORECASE)
        if m: work_id = m.group(1)
    if not work_id:
        raise RuntimeError("WorkFolder id not found in Result.aspx")

    # Step 4: WorkFolder.aspx
    work_url = f"{VMR_BASE}/WorkFolder.aspx"
    html4 = _vmr_get_html(sess, work_url, params={"id": work_id})

    # Step 5: setFibreTrace('<FT_ID>',0)
    m = re.search(r"setFibreTrace\(\s*'([^']+)'\s*,\s*0\s*\)", html4, re.IGNORECASE)
    if not m:
        s4 = BeautifulSoup(html4, _BS_PARSER)
        for tag in s4.find_all(attrs={"onclick": True}):
            mm = re.search(r"setFibreTrace\(\s*'([^']+)'\s*,\s*0\s*\)", tag.get("onclick",""), re.IGNORECASE)
            if mm: m = mm; break
    if not m:
        raise RuntimeError("FibreTrace id not found on WorkFolder page")
    ft_id = m.group(1)

    # Step 6: FibreTrace.aspx?id=<FT_ID>:0:A
    fibre_url = f"{VMR_BASE}/FibreTrace.aspx"
    fibre_param = f"{ft_id}:0:A"
    html6 = _vmr_get_html(sess, fibre_url, params={"id": fibre_param})

    # Step 7: save (put alongside app so .exe can read it)
    try:
        base_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.getcwd()
    except Exception:
        base_dir = os.getcwd()
    out_dir_abs = os.path.join(base_dir, out_dir)
    os.makedirs(out_dir_abs, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe_ft = re.sub(r'[<>:"/\\|?*]', "_", ft_id)
    filename = f"vmr_fibretrace_{vmr_id}_{safe_ft}_{ts}.html"
    path = Path(out_dir_abs) / filename
    with open(path, "w", encoding="utf-8") as f:
        f.write(html6)
    return path

# ---------- Flexible header mapping (HTML -> CSV-equivalent fields) ----------
# Tweak these patterns if your FibreTrace table uses different labels.
# Left side = our target field (what your CSV logic expects BEFORE it normalizes),
# Right side = list of possible header names (case-insensitive contains match).
HTML_FIELD_MAP = {
    "Cable#":                 ["cable#", "no", "seq", "index"],
    "A-End":                  ["a-end", "a end", "a_end", "origin", "from"],
    "Fibre Cable":            ["fibre cable", "cable", "os name", "os_name", "fibre/os", "os"],
    "Name":                   ["name"],  # <-- NEW: we will parse metrics from this
    "B-End":                  ["b-end", "b end", "b_end", "destination", "to"],
    "Connect/Disconnect":     ["connect/disconnect", "conn", "action", "status"],
    "EO":                     ["eo", "exchange", "owner eo", "build eo"],
    "Length":                 ["length", "span", "distance", "m"],
}

# --- NEW: helper to extract metrics from the 'Name' column text ---
_NAME_LEN_RE = re.compile(r'(\d+(?:\.\d+)?)\s*m\b', re.IGNORECASE)
_NAME_TOTFIB_RE = re.compile(r'(\d+)\s*fibres?\b', re.IGNORECASE)
_NAME_WK_RE = re.compile(r'(\d+)\s*WK\b', re.IGNORECASE)
_NAME_SP_RE = re.compile(r'(\d+)\s*SP\b', re.IGNORECASE)

# >>> UPDATED: robust HTML parser selection for .exe builds
from bs4 import BeautifulSoup

# prefer lxml if present (faster), else fall back to stdlib html.parser
_BS_PARSER = "lxml"
try:
    import lxml  # noqa: F401
except Exception:
    _BS_PARSER = "html.parser"

# >>> NEW: cross-section helpers (single source of truth for parse/filter/alerts)

VMR_BASE_URL = "https://cadprdwebw001.optus.com.au/vmr/"
VMR_Cable_URL = VMR_BASE_URL + "CrossSectionReview.aspx?id="

def _html_clean(text):
    import re
    if text is None:
        return ""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()

def _table_extract(tbl):
    headers, rows = [], []
    if not tbl:
        return headers, rows
    first_tr = tbl.find("tr")
    trs = tbl.find_all("tr")
    if first_tr:
        ths = first_tr.find_all(["th"])
        if ths:
            headers = [_html_clean(th.get_text(" ", strip=True)) for th in ths]
            trs = trs[1:]
        else:
            tds = first_tr.find_all("td")
            if tds:
                headers = [f"Col {i+1}" for i in range(len(tds))]
    for tr in trs:
        cells = tr.find_all(["td", "th"])
        if not cells: 
            continue
        row = [_html_clean(c.get_text(" ", strip=True)) for c in cells]
        if headers and len(row) != len(headers):
            if len(row) < len(headers):
                row += [""] * (len(headers) - len(row))
            else:
                row = row[:len(headers)]
        rows.append(row)
    return headers, rows

# >>> NEW: VMR HTML → CSV-like converter (so we can reuse process_csv())
import tempfile
from bs4 import BeautifulSoup
import re

# ---------- VMR HTML → CSV-like (matches what process_csv expects) ----------
from bs4 import BeautifulSoup
import tempfile, csv, re

def _firstline_text(td):
    parts = []
    for child in td.children:
        if getattr(child, "name", None) == "br":
            break
        if getattr(child, "name", None) == "img":
            continue
        if isinstance(child, str):
            parts.append(child.strip())
        else:
            if child.name == "span" and "rs" in (child.get("class") or []):
                continue
            parts.append(child.get_text(strip=True))
    s = " ".join(p for p in parts if p)
    return re.sub(r"\s+", " ", s).strip()

def _cd_text(td):
    """
    Parse the small C/D table inside the 'C/D' cell.
    Output like: 'Connect3:1' or 'Disconnect289:121; Connect121:301'
    (semicolon-separated if multiple lines).
    """
    if td is None:
        return ""
    out = []
    for row in td.find_all("tr"):
        img = row.find("img")
        if not img:
            continue
        src = (img.get("src") or "").lower()
        prefix = "Connect" if "connect" in src else ("Disconnect" if "disconnect" in src else "")
        b = row.find("b")
        num = b.get_text(strip=True) if b else ""
        if prefix and num:
            out.append(f"{prefix}{num}")
    # de-duplicate while keeping order
    seen, dedup = set(), []
    for s in out:
        if s not in seen:
            seen.add(s); dedup.append(s)
    return "; ".join(dedup)


def _eo_only(td):
    txt = td.get_text(" ", strip=True)
    m = re.search(r"\bEO\d+\b", txt)
    return m.group(0) if m else ""

def _parse_len_from_nameblock(name_text):
    # finds max 'XX.XXm' and fibres 'NN fibres'/'NNfibres'
    m_all_len = re.findall(r"(\d+(?:\.\d+)?)\s*m\b", name_text, flags=re.I)
    length_max = max([float(x) for x in m_all_len]) if m_all_len else None
    m_fib = re.search(r"(\d+)\s*fibres?\b", name_text, flags=re.I)
    tot_fib = int(m_fib.group(1)) if m_fib else None
    return length_max, tot_fib

def _derive_ft_from_summary(html_text):
    soup = BeautifulSoup(html_text, _BS_PARSER)
    table = soup.find("table", id="gvFibreTraceSummary")
    if not table:
        return None, None
    # the middle column is "Name"
    for tr in table.find_all("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) >= 3:
            name = _firstline_text(tds[1]).strip()
            up = name.upper()
            if up.startswith("L_"): return "Local", name
            if up.startswith("J_"): return "Junction", name
            if up.startswith("T_"): return "Trunk", name
            return None, name
    return None, None

# === NEW helpers (place above _vmr_html_to_csv_like_tempfile) =================

# Keywords used to identify the Fibre Trace Details table, adapted from html_extractor.py
_FTD_KEYWORDS = ["A End", "A-End", "B End", "B-End", "Fibre Cable", "Name", "Connect", "Disconnect", "C/D", "EO", "Length"]

def _header_match_score(cells):
    import re
    joined = " | ".join(cells or [])
    score = 0
    for kw in _FTD_KEYWORDS:
        if re.search(r"\b" + re.escape(kw) + r"\b", joined, flags=re.IGNORECASE):
            score += 1
    return score

def _pick_fibretrace_table_bs4(html_text):
    from bs4 import BeautifulSoup
    import re
    soup = BeautifulSoup(html_text, _BS_PARSER)
    tables = soup.find_all("table")
    if not tables:
        return None

    previews = []
    for t in tables:
        rows = []
        for tr in t.find_all("tr"):  # (no recursive=False; walk all descendants)
            cells = tr.find_all(["th", "td"])
            if not cells:
                continue
            txts = [re.sub(r"\s+", " ", c.get_text(" ", strip=True)) for c in cells]
            rows.append(txts)
        previews.append(rows)

    # (1) best header score
    best_idx, best_score = None, -1
    for idx, rows in enumerate(previews):
        if not rows:
            continue
        header = next((r for r in rows if any(x.strip() for x in r)), [])
        score = _header_match_score(header)
        if score > best_score:
            best_score, best_idx = score, idx
    if best_idx is not None and best_score >= 2:
        return tables[best_idx]

    # (2) any that looks like fibre-path: contains connect/disconnect or n:n and >=5 cols
    for idx, rows in enumerate(previews):
        if not rows:
            continue
        any_conn = any(any(re.search(r"\b(connect|disconnect)\b", c, re.I) or re.search(r"\b\d+\s*:\s*\d+\b", c)
                           for c in r) for r in rows)
        max_cols = max((len(r) for r in rows), default=0)
        if any_conn and max_cols >= 5:
            return tables[idx]

    # (3) seventh table if present
    if len(tables) >= 7:
        return tables[6]

    # (4) largest by rows
    largest_idx = max(range(len(previews)), key=lambda i: len(previews[i]))
    return tables[largest_idx]



# === REPLACE this whole function =============================================

# === REPLACE this whole function =============================================
def _vmr_html_to_csv_like_tempfile(html_text):
    """
    Robust HTML → temp CSV that mirrors the CSV your existing process_csv(...) expects.
      • Marker row: ["Fibre Trace Details"]
      • For EACH cable row:
            [seq, A-End, Fibre Cable, B-End, Connect/Disconnect, EO, Length]
        + spacer row
        + "xx.xx m, NNfibres" in col 3

    Guarantees the first cable row is included even with nested <tr>/<td> wrappers.
    """
    table = _pick_fibretrace_table_bs4(html_text)
    if not table:
        raise ValueError("Fibre Trace Details table not found.")

    all_trs = table.find_all("tr")
    header_idx = None
    for i, tr in enumerate(all_trs):
        ths = tr.find_all("th")
        if ths and any("name" in th.get_text(strip=True).lower() or "fibre" in th.get_text(strip=True).lower()
                       for th in ths):
            header_idx = i
            break

    hdr_lc = []
    if header_idx is not None:
        hdr_lc = [re.sub(r"\s+", " ", th.get_text(" ", strip=True)).lower() for th in all_trs[header_idx].find_all("th")]
        start_idx = header_idx + 1
    else:
        start_idx = 0

    def idx_for(*names, default=None):
        for nm in names:
            if nm in hdr_lc:
                return hdr_lc.index(nm)
        return default

    idx_a   = idx_for("a end","a-end")
    idx_nm  = idx_for("name","fibre cable","fibre","fiber")
    idx_b   = idx_for("z end","b end","b-end")
    idx_cd  = idx_for("c/d","connect/disconnect","c d")
    idx_eo  = idx_for("eo")
    idx_len = idx_for("length(m)","length")

    if any(x is None for x in (idx_a, idx_nm, idx_b, idx_cd, idx_eo, idx_len)):
        idx_a   = 2 if idx_a   is None else idx_a
        idx_nm  = 3 if idx_nm  is None else idx_nm
        idx_b   = 4 if idx_b   is None else idx_b
        idx_cd  = 8 if idx_cd  is None else idx_cd
        idx_eo  = 9 if idx_eo  is None else idx_eo
        idx_len = 10 if idx_len is None else idx_len

    def firstline_text_from_cell(cell):
        if cell is None: return ""
        txt = cell.get_text("\n", strip=True)
        return txt.split("\n", 1)[0].strip()

    def eo_only_from_cell(cell):
        if cell is None: return ""
        txt = cell.get_text(" ", strip=True)
        m = re.search(r"\bEO\d+\b", txt)
        return m.group(0) if m else ""

    def parse_len_from_nameblock(text):
        m_all = re.findall(r"(\d+(?:\.\d+)?)\s*m\b", text, flags=re.I)
        length_max = max([float(x) for x in m_all]) if m_all else None
        m_fib = re.search(r"(\d+)\s*fibres?\b", text, flags=re.I)
        total = int(m_fib.group(1)) if m_fib else None
        return length_max, total

    def normalize_cd(cell, fallback_text):
        parsed = _cd_text(cell) if cell else ""
        if parsed:
            return parsed
        t = (fallback_text or "").strip()
        if not t:
            return ""
        t = t.replace("\r", "\n")
        parts = [p.strip() for p in t.split("\n") if p.strip()]
        t = "; ".join(parts)
        t = re.sub(r"\bC\s*:?", "Connect", t, flags=re.I)
        t = re.sub(r"\bD\s*:?", "Disconnect", t, flags=re.I)
        t = re.sub(r"\bconnect\b", "Connect", t, flags=re.I)
        t = re.sub(r"\bdisconnect\b", "Disconnect", t, flags=re.I)
        t = re.sub(r"(Connect|Disconnect)\s+", r"\1", t)
        if re.fullmatch(r"(\d+\s*:\s*\d+)(\s*;\s*\d+\s*:\s*\d+)*", t):
            pairs = re.findall(r"\d+\s*:\s*\d+", t)
            return "; ".join("Connect" + p.replace(" ", "") for p in pairs)
        return t

    out_rows = []
    out_rows.append(["Fibre Trace Details"])
    seq = 0

    for tr in all_trs[start_idx:]:
        tds = tr.find_all(["td","th"])
        if not tds:
            continue

        def cell_at(i): return tds[i] if 0 <= i < len(tds) else None

        name_cell = cell_at(idx_nm)
        if name_cell is None:
            continue

        a_end = firstline_text_from_cell(cell_at(idx_a))
        b_end = firstline_text_from_cell(cell_at(idx_b))

        name_full = name_cell.get_text(" ", strip=True)
        anchor = name_cell.find("a")
        cable = anchor.get_text(strip=True) if anchor else firstline_text_from_cell(name_cell)
        if "(#" in name_full and "(#" not in cable:
            mm = re.search(r"\(#\s*\d+\s*\)", name_full)
            if mm:
                cable = f"{cable.strip()} {mm.group(0)}"

        cd_fallback_text = cell_at(idx_cd).get_text("\n", strip=True) if cell_at(idx_cd) else ""
        connect_disc = normalize_cd(cell_at(idx_cd), cd_fallback_text)

        eo = eo_only_from_cell(cell_at(idx_eo))

        length_str = ""
        if cell_at(idx_len):
            raw = (cell_at(idx_len).get_text(strip=True) or "").replace(",", "")
            if re.match(r"^\d+(\.\d+)?$", raw):
                length_str = f"{float(raw):.2f}"
        if not length_str:
            length_max, _ = parse_len_from_nameblock(name_full)
            if isinstance(length_max, (int, float)):
                length_str = f"{length_max:.2f}"

        seq += 1

        length_max, tot_fib = parse_len_from_nameblock(name_full)
        fibres_line = f"{length_max:.2f}m, {tot_fib}fibres" if (length_max is not None and tot_fib) else ""

        out_rows.append([str(seq), a_end, cable, b_end, connect_disc, eo, length_str])
        out_rows.append(["", "", "", "", "", "", ""])
        out_rows.append(["", "", fibres_line, "", "", "", ""])

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode="w", encoding="cp1252", newline="")
    w = csv.writer(tmp)
    for r in out_rows:
        w.writerow(r)
    tmp.close()
    return tmp.name

def _infer_ft_from_csv_summary_name(csv_path):
    """
    Open the raw CSV text and find the 'Fibre Trace Summary' block.
    Read the first data row's 'Name' cell and infer L_/J_/T_.
    Very tolerant to delimiters/quotes.
    """
    try:
        raw = open(csv_path, "r", encoding="utf-8", errors="ignore").read()
    except Exception:
        raw = open(csv_path, "r", encoding="cp1252", errors="ignore").read()

    lines = [ln.strip() for ln in raw.splitlines()]
    # find the line that begins the summary section
    start = None
    for i, ln in enumerate(lines):
        if ln.lower().startswith("fibre trace summary"):
            start = i; break
    if start is None:
        return None

    # find header (contains 'name')
    hdr_idx = None
    for j in range(start+1, min(start+10, len(lines))):
        if "name" in lines[j].lower():
            hdr_idx = j; break
    if hdr_idx is None or hdr_idx+1 >= len(lines):
        return None

    # first data row after header
    data_ln = lines[hdr_idx+1]
    # naive split on comma/semicolon/tab/pipe; pick the longest split
    parts = max([re.split(r"[,\t;|]", data_ln), [data_ln]], key=len)
    # assume 'Name' column is the middle field
    name = ""
    if len(parts) >= 3:
        name = parts[1].strip().strip('"').strip()
    else:
        name = parts[0].strip().strip('"').strip()

    up = name.upper()
    if up.startswith("L_"): return "Local"
    if up.startswith("J_"): return "Junction"
    if up.startswith("T_"): return "Trunk"
    return None


# >>> UPDATED: use the robust parser choice above
def parse_gridview2(html_text: str):
    """
    Extract headers/rows from the VMR cross-section page.
    Works with either lxml (if bundled) or stdlib html.parser (fallback).
    """
    soup = BeautifulSoup(html_text, _BS_PARSER)

    # Adjust selectors to match your page (unchanged from your version):
    table = soup.find("table", id="GridView2") or soup.find("table", {"class": "GridView2"})
    if not table:
        return [], []

    # headers
    headers = []
    thead = table.find("thead")
    if thead:
        ths = thead.find_all("th")
        headers = [th.get_text(strip=True) for th in ths]
    if not headers:
        first_tr = table.find("tr")
        if first_tr:
            headers = [th.get_text(strip=True) for th in first_tr.find_all(["th", "td"])]

    # body rows
    rows = []
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if not tds:
            continue
        rows.append([td.get_text(strip=True) for td in tds])

    return headers, rows


def filter_rows_by_tray_range(rows, tray_range_text):
    # tray_range_text like "1-6" (1-based inclusive)
    import re
    s = (tray_range_text or "").strip()
    if not s:
        return rows
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", s)
    if not m:
        return []
    a, b = int(m.group(1)), int(m.group(2))
    if a > b: 
        a, b = b, a
    a = max(1, a); b = min(len(rows), b)
    if a > b: 
        return []
    return rows[a-1:b]

def rows_have_alert(headers, rows_subset):
    # Highlight rule:
    # - OS Name starts with 'T_'
    # - OR Bearer ID contains 'OTS' or 'DWDM' (case-insensitive)
    colmap = {h.lower(): i for i, h in enumerate(headers or [])}
    idx_os = colmap.get("os name")
    idx_bearer = colmap.get("bearer id")
    for r in rows_subset:
        os_name = (r[idx_os] if idx_os is not None and idx_os < len(r) else "").strip().upper()
        bearer = (r[idx_bearer] if idx_bearer is not None and idx_bearer < len(r) else "").strip().upper()
        if os_name.startswith("T_"): 
            return True
        if "OTS" in bearer or "DWDM" in bearer:
            return True
    return False

# >>> NEW: simple temp-file cache so we never re-crawl after Process click

# >>> NEW/UPDATED: simple temp-file cache so we never re-crawl after Process click
class CrossSectionCache:
    def __init__(self):
        # Put cache next to the running app (works for .exe and .py)
        try:
            if getattr(sys, "frozen", False):
                # PyInstaller onefile / onefolder — the exe path is stable & writable if user launched from a user dir
                base_dir = os.path.dirname(sys.executable) or os.getcwd()
            else:
                base_dir = os.path.dirname(os.path.abspath(__file__))
        except Exception:
            base_dir = os.getcwd()

        self.cache_dir = os.path.join(base_dir, "_fibre_cache")
        os.makedirs(self.cache_dir, exist_ok=True)

        self.index_file = os.path.join(self.cache_dir, "index.json")
        self._index = self._load_index()
        atexit.register(self.clear)

    def _load_index(self):
        if os.path.exists(self.index_file):
            try:
                with open(self.index_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_index(self):
        try:
            with open(self.index_file, "w", encoding="utf-8") as f:
                json.dump(self._index, f)
        except Exception:
            pass

    def _path_for(self, seg_id: str) -> str:
        # canonical filename used for direct fallback
        safe = str(seg_id).strip()
        return os.path.join(self.cache_dir, f"{safe}.html")

    def clear(self):
        # Remove cached files when Process is clicked again or app closes
        try:
            for name in os.listdir(self.cache_dir):
                fp = os.path.join(self.cache_dir, name)
                try:
                    os.remove(fp)
                except Exception:
                    pass
            self._index.clear()
            self._save_index()
        except Exception:
            pass

    def put_html(self, seg_id, html_text):
        path = self._path_for(seg_id)
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(html_text)
        except Exception:
            return [], []
        headers, rows = parse_gridview2(html_text)
        self._index[seg_id] = {
            "path": path,
            "headers": headers,
            "rows_len": len(rows),
            "has_alert_by_tray": {}
        }
        self._save_index()
        return headers, rows

    def has(self, seg_id):
        # robust check: prefer on-disk presence
        path = self._path_for(seg_id)
        if os.path.exists(path):
            return True
        meta = self._index.get(seg_id)
        return bool(meta) and os.path.exists(meta.get("path", ""))

    def get_html(self, seg_id):
        """
        Robust read:
        1) Try index.json mapping
        2) Fallback to '<cache_dir>/<SEGMENT_ID>.html'
        """
        # 1) via index.json
        meta = self._index.get(seg_id)
        if meta:
            p = meta.get("path", "")
            if p and os.path.exists(p):
                try:
                    with open(p, "r", encoding="utf-8") as f:
                        return f.read()
                except Exception:
                    pass

        # 2) direct fallback (handles cases where index wasn't flushed but .html exists)
        p2 = self._path_for(seg_id)
        if os.path.exists(p2):
            try:
                with open(p2, "r", encoding="utf-8") as f:
                    return f.read()
            except Exception:
                return None
        return None

    def headers_for(self, seg_id):
        meta = self._index.get(seg_id, {})
        headers = meta.get("headers", [])
        if headers:
            return headers
        # Recompute from HTML if needed
        html = self.get_html(seg_id)
        if not html:
            return []
        headers, rows = parse_gridview2(html)
        meta["headers"] = headers or []
        meta["rows_len"] = len(rows or [])
        self._index[seg_id] = meta
        self._save_index()
        return headers or []

    def rows_for(self, seg_id):
        """
        Return the parsed rows for a cached seg_id, recomputing from HTML if necessary.
        """
        html = self.get_html(seg_id)
        if not html:
            return []
        headers, rows = parse_gridview2(html)
        # update cache metadata
        meta = self._index.get(seg_id, {})
        meta["headers"] = headers or meta.get("headers", [])
        meta["rows_len"] = len(rows or [])
        self._index[seg_id] = meta
        self._save_index()
        return rows or []

    def set_tray_alert(self, seg_id, tray_str, flag):
        meta = self._index.get(seg_id)
        if meta is not None:
            meta.setdefault("has_alert_by_tray", {})[tray_str] = bool(flag)
            self._save_index()

    def tray_has_alert(self, seg_id, tray_str):
        meta = self._index.get(seg_id)
        if not meta:
            return False
        return bool(meta.get("has_alert_by_tray", {}).get(tray_str, False))

########################################################################
# Cross Section Details viewer (callable function)
########################################################################
from bs4 import BeautifulSoup

def _clean_cell_text(text: str) -> str:
    import re
    if text is None:
        return ""
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text

def _extract_table(table):
    headers, rows = [], []
    if not table:
        return headers, rows
    first_tr = table.find("tr")
    trs = table.find_all("tr")
    if first_tr:
        ths = first_tr.find_all(["th"])
        if ths:
            headers = [_clean_cell_text(th.get_text(" ", strip=True)) for th in ths]
            trs = trs[1:]
        else:
            tds = first_tr.find_all("td")
            if tds:
                headers = [f"Col {i+1}" for i in range(len(tds))]
    for tr in trs:
        cells = tr.find_all(["td", "th"])
        if not cells:
            continue
        row = [_clean_cell_text(c.get_text(" ", strip=True)) for c in cells]
        if headers and len(row) != len(headers):
            if len(row) < len(headers):
                row += [""] * (len(headers) - len(row))
            else:
                row = row[:len(headers)]
        rows.append(row)
    return headers, rows

def _parse_gridview2(html: str):
    soup = BeautifulSoup(html, _BS_PARSER)
    grid2 = soup.find(id="GridView2")
    return _extract_table(grid2)

def _should_highlight(values, idx_os, idx_bearer) -> bool:
    try:
        os_name = (values[idx_os] if idx_os is not None else "").strip()
        bearer = (values[idx_bearer] if idx_bearer is not None else "").strip()
    except Exception:
        os_name, bearer = "", ""
    if os_name.upper().startswith("T_"):
        return True
    bup = bearer.upper()
    return ("OTS" in bup) or ("DWDM" in bup)

def _filter_by_tray_range(rows, tray_range: str):
    # tray_range like "1-6", "49-54" — interpreted as 1-based row numbers in GridView2
    import re
    s = (tray_range or "").strip()
    if not s:
        return rows
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", s)
    if not m:
        raise ValueError('Range must be "start-end", e.g. "1-6"')
    a, b = int(m.group(1)), int(m.group(2))
    if a > b:
        a, b = b, a
    a = max(1, a)
    b = min(len(rows), b)
    if a > b:
        return []
    # convert 1-based to 0-based slice
    return rows[a-1:b]

def open_cross_section_viewer(id_value: str, tray_range: str = ""):
    """
    Fetches the CrossSectionReview.aspx page for the given SEGMENT_ID (id_value),
    parses GridView2, filters by tray_range (1-based row range), and displays
    a Tkinter Toplevel window with a Treeview including row highlighting:
     - OS Name starts with "T_"
     - Bearer ID contains "OTS" or "DWDM" (case-insensitive)
    """

    url = VMR_Cable_URL + str(id_value)

    try:
        headers_http = {"User-Agent": "Mozilla/5.0 (compatible; FibreAssistance/1.0)"}
        resp = requests.get(url, headers=headers_http, timeout=30, verify=True)
        resp.raise_for_status()
        headers, rows = _parse_gridview2(resp.text)
        view_rows = _filter_by_tray_range(rows, tray_range)
    except Exception as e:
        messagebox.showerror("Cross Section Viewer", f"Failed to load/parse:\n{url}\n\n{e}")
        return

    win = tk.Toplevel()
    win.title(f"Cross Section Details – {id_value}  [{tray_range or 'ALL'}]")
    win.geometry("1100x700")

    # toolbar
    top = ttk.Frame(win)
    top.pack(fill="x", padx=10, pady=8)
    ttk.Label(top, text=f"ID: {id_value}   URL: {url}").pack(side="left")
    ttk.Label(top, text=f"Showing {len(view_rows)} row(s) of {len(rows)}").pack(side="right")

    # table
    frame = ttk.Frame(win)
    frame.pack(fill="both", expand=True, padx=10, pady=(0,10))
    tree = ttk.Treeview(frame, columns=tuple(headers or []), show="headings", height=24)
    # CHANGED: use classic Tk scrollbars with wider grip (match Fibre Check)
    vsb = tk.Scrollbar(frame, orient="vertical", command=tree.yview, width=18)
    hsb = tk.Scrollbar(frame, orient="horizontal", command=tree.xview, width=18)
    tree.configure(yscroll=vsb.set, xscroll=hsb.set)
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(0, weight=1)

    # columns + highlight style
    for col in (headers or []):
        tree.heading(col, text=col)
        # initial width; we will auto-size after inserting rows
        tree.column(col, width=max(90, min(360, len(col)*10)), anchor="center", stretch=False)
    tree.tag_configure("alert", background="#fff3cd")

    colmap = {h.lower(): i for i, h in enumerate(headers or [])}
    idx_os = colmap.get("os name")
    idx_bearer = colmap.get("bearer id")

    for r in view_rows:
        tag = ("alert",) if _should_highlight(r, idx_os, idx_bearer) else ()
        vals = r if len(r) == len(headers) else (r + [""]*(len(headers)-len(r)))[:len(headers)]
        tree.insert("", "end", values=vals, tags=tag)

    # NEW: auto-fit columns to content just like Fibre Check
    def _autosize_columns(t):
        style = ttk.Style()
        body_font_name = style.lookup("Treeview", "font") or "TkDefaultFont"
        heading_font_name = style.lookup("Treeview.Heading", "font") or body_font_name
        body_font = tkfont.nametofont(body_font_name)
        heading_font = tkfont.nametofont(heading_font_name)
        t.update_idletasks()
        cols = list(t["columns"])
        for col in cols:
            header_text = t.heading(col)["text"]
            header_w = heading_font.measure(header_text) + 24
            col_index = cols.index(col)
            max_w = header_w
            for iid in t.get_children(""):
                vals = t.item(iid, "values")
                text = str(vals[col_index]) if col_index < len(vals) else ""
                max_w = max(max_w, body_font.measure(text) + 24)
            computed = max(80, max_w)
            t.column(col, width=computed, minwidth=computed, stretch=False)

    _autosize_columns(tree)

    # close on Esc
    win.bind("<Escape>", lambda e: win.destroy())


########################################################################
# REUSABLE DOWNLOAD LOGIC
########################################################################

def download_file(
    url, 
    destination, 
    proxies, 
    retry_count=0, 
    on_success=None, 
    on_progress=None,
    on_error=None
):
    """
    Download a file with progress reporting and error handling.
    """
    try:
        response = requests.get(url, stream=True, proxies=proxies, timeout=10)
        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            with open(destination, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
                        downloaded_size += len(chunk)
                        if total_size > 0 and on_progress:
                            progress_pct = (downloaded_size / total_size) * 100
                            on_progress(destination, progress_pct)
            if on_success:
                on_success(destination)
            return True
        else:
            error_msg = (
                f"Failed to download {destination}. "
                f"HTTP status code: {response.status_code}"
            )
            if on_error:
                on_error(error_msg)
    except Exception as e:
        if on_error:
            on_error(f"Error downloading {destination}: {e}")
    return False

########################################################################
# Fibre Database Update Tool
########################################################################

class FibreDatabaseUpdater:
    def __init__(self, parent):
        """
        Create the 'Fibre Database Update Tool' UI inside `parent`.
        """
        self.parent = parent
        self.current_dir = os.getcwd()
        self.db_path = os.path.join(self.current_dir, 'database.db')

        # Filenames
        self.cable_filename = "optus_fiber.geojson"
        self.splice_filename = "SpliceCases.geojson"

        # For download progress display
        self.download_progress_var = StringVar(value="")

        self.build_ui()
        self.update_file_status_labels()

    def build_ui(self):
        # --- modified: centered, URL on its own line with two-line gap ---
        instruction_frame = ttk.Frame(self.parent)
        instruction_frame.pack(pady=10, fill='x')

        # Container to center contents
        inner = ttk.Frame(instruction_frame)
        inner.pack(anchor="center")

        # Static instruction text (same style)
        part_text = (
            "We will use optus_fiber.geojson and SpliceCases.geojson.\n"
            "If they the 'Download' button doesn't work, you can download them here:"
        )
        part_label = ttk.Label(
            inner,
            text=part_text,
            foreground="blue",
            font=("Arial", 12),
            justify="center",
            wraplength=600
        )
        part_label.pack()

        # Two-line gap before URL
        spacer = ttk.Label(inner, text="\n")
        spacer.pack()

        # Clickable URL on its own centered line
        url = "https://athena-ipne.optusnet.com.au/ipne_data/ce/"
        link_label = tk.Label(
            inner,
            text=url,
            fg="blue",
            cursor="hand2",
            font=("Arial", 12, "underline"),
            wraplength=600,
            borderwidth=0,
            justify="center"
        )
        link_label.pack()
        link_label.bind("<Button-1>", lambda e: webbrowser.open_new(url))
        # --- end modified block ---

        status_frame = ttk.Frame(self.parent)
        status_frame.pack(pady=5, padx=20, fill='x')

        # Cable status row
        cable_label = ttk.Label(status_frame, text="optus_fiber.geojson:", width=25)
        cable_label.grid(row=0, column=0, sticky=tk.W)
        self.cable_status_label = ttk.Label(status_frame, text="", foreground="red")
        self.cable_status_label.grid(row=0, column=1, sticky=tk.W)

        # Splice status row
        splice_label = ttk.Label(status_frame, text="SpliceCases.geojson:", width=25)
        splice_label.grid(row=1, column=0, sticky=tk.W)
        self.splice_status_label = ttk.Label(status_frame, text="", foreground="red")
        self.splice_status_label.grid(row=1, column=1, sticky=tk.W)

        # Download button
        download_button = ttk.Button(
            self.parent,
            text="Download",
            command=self.on_download
        )
        download_button.pack(pady=10)

        # Download progress label
        progress_label = ttk.Label(self.parent, textvariable=self.download_progress_var, foreground="green")
        progress_label.pack()

        # Run Update button
        run_button = ttk.Button(
            self.parent,
            text="Run Update",
            command=self.on_run_update
        )
        run_button.pack(pady=20)

        credit_label = ttk.Label(self.parent, text="developed by Jian", foreground="gray", font=("Arial", 10))
        credit_label.pack(side="bottom", pady=5)

    def update_file_status_labels(self):
        import datetime

        def build_status(path, label):
            if os.path.exists(path):
                mtime = os.path.getmtime(path)
                # When was it modified?
                dt = datetime.datetime.fromtimestamp(mtime)
                today = datetime.date.today()
                days_ago = max(0, (today - dt.date()).days)

                # Decide text + colour
                if days_ago == 0:
                    status_text = (
                        f"Up to date — Last modified: {self.format_time(mtime)} (today)"
                    )
                    colour = "green"
                else:
                    plural = "day" if days_ago == 1 else "days"
                    status_text = (
                        f"Last modified: {self.format_time(mtime)} — {days_ago} {plural} ago. "
                        f"File is not up to date and there are risks using them."
                    )
                    colour = "red"

                label.config(text=status_text, foreground=colour)
            else:
                label.config(text="Not found", foreground="red")

        cable_path = os.path.join(self.current_dir, self.cable_filename)
        splice_path = os.path.join(self.current_dir, self.splice_filename)

        build_status(cable_path, self.cable_status_label)
        build_status(splice_path, self.splice_status_label)

    def format_time(self, timestamp):
        import datetime
        dt = datetime.datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def on_download(self):
        response = messagebox.askyesno(
            "Download Confirmation",
            "This will download optus_fiber.geojson and SpliceCases.geojson to the current directory.\nContinue?"
        )
        if not response:
            return

        self.download_progress_var.set("Starting download...")

        threading.Thread(target=self.perform_downloads).start()

    def perform_downloads(self):
        proxies = {
            'http': 'http://extranetproxy1.optus.com.au:8080',
            'https': 'http://extranetproxy1.optus.com.au:8080',
        }

        cable_url = "https://athena-ipne.optusnet.com.au/ipne_data/ce/optus_fiber.geojson"
        splice_url = "https://athena-ipne.optusnet.com.au/ipne_data/ce/SpliceCases.geojson"

        def on_progress(file_dest, pct):
            self.parent.after(0, lambda: self.download_progress_var.set(
                f"Downloading {os.path.basename(file_dest)}: {pct:.2f}%"
            ))

        def on_success(file_dest):
            pass

        def on_error(msg):
            self.parent.after(0, lambda: messagebox.showerror("Download Error", msg))

        cable_dest = os.path.join(self.current_dir, self.cable_filename)
        success_cable = download_file(
            cable_url,
            cable_dest,
            proxies,
            retry_count=0,
            on_success=on_success,
            on_progress=on_progress,
            on_error=on_error
        )

        if success_cable:
            splice_dest = os.path.join(self.current_dir, self.splice_filename)
            download_file(
                splice_url,
                splice_dest,
                proxies,
                retry_count=0,
                on_success=on_success,
                on_progress=on_progress,
                on_error=on_error
            )

        self.parent.after(0, self.update_file_status_labels)
        self.parent.after(0, lambda: self.download_progress_var.set("Download completed."))

    def on_run_update(self):
        cable_file = os.path.join(self.current_dir, self.cable_filename)
        splice_file = os.path.join(self.current_dir, self.splice_filename)

        if not os.path.exists(cable_file) and not os.path.exists(splice_file):
            messagebox.showerror(
                "No Files Found",
                "Neither optus_fiber.geojson nor SpliceCases.geojson found in the current directory.\n"
                "Please download before running the update."
            )
            return

        self.run_tool(
            cable_file if os.path.exists(cable_file) else None,
            splice_file if os.path.exists(splice_file) else None
        )

    ###########################################
    # Database update methods and hashing functions
    ###########################################
    def generate_cable_hash(self, properties, geometry):
        stable_properties = {
            'NAME': properties.get('NAME'),
            'OWNER': properties.get('OWNER'),
            'SPAN_LENGTH': properties.get('SPAN_LENGTH'),
            'IOF': properties.get('IOF'),
            'LINK1': properties.get('LINK1'),
            'LINK2': properties.get('LINK2'),
            'EO': properties.get('EO'),
            'SEGMENT_ID': properties.get('SEGMENT_ID'),
            'BUILD_DATE': properties.get('BUILD_DATE'),
            'CONSTRUCT_TYPE': properties.get('CONSTRUCT_TYPE'),
        }
        data_string = json.dumps(stable_properties, sort_keys=True) + json.dumps(geometry)
        return hashlib.md5(data_string.encode()).hexdigest()

    def generate_splicecase_hash(self, properties, geometry):
        stable_properties = {
            'NAME': properties.get('NAME'),
            'ADDRESS': properties.get('ADDRESS'),
            'SUBURB': properties.get('SUBURB'),
            'BUTTSPLICE': properties.get('BUTTSPLICE'),
            'MODEL': properties.get('MODEL'),
            'MANHOLE': properties.get('MANHOLE'),
            'OWNER': properties.get('OWNER'),
            'EO': properties.get('EO'),
            'BUILDDATE': properties.get('BUILDDATE'),
            'JOBNUMBER': properties.get('JOBNUMBER'),
        }
        data_string = json.dumps(stable_properties, sort_keys=True) + json.dumps(geometry)
        return hashlib.md5(data_string.encode()).hexdigest()

    def update_cable_data(self, cursor, data):
        changes = {'new': 0, 'updated': 0, 'unchanged': 0}
        for feature in data['features']:
            properties = feature['properties']
            geometry = feature['geometry']
            generated_id = self.generate_cable_hash(properties, geometry)
            cursor.execute('SELECT CABLE_STATUS, FIBRES, PROTECTED FROM Cable WHERE generated_id = ?', (generated_id,))
            existing_record = cursor.fetchone()

            if existing_record:
                if (existing_record[0] != properties.get('CABLE_STATUS') or
                    existing_record[1] != properties.get('FIBRES') or
                    existing_record[2] != properties.get('PROTECTED')):
                    cursor.execute('''
                        UPDATE Cable
                        SET CABLE_STATUS = ?, FIBRES = ?, PROTECTED = ?
                        WHERE generated_id = ?
                    ''', (
                        properties.get('CABLE_STATUS'),
                        properties.get('FIBRES'),
                        properties.get('PROTECTED'),
                        generated_id
                    ))
                    changes['updated'] += 1
                else:
                    changes['unchanged'] += 1
            else:
                cursor.execute('''
                    INSERT INTO Cable (
                        NAME, CABLE_STATUS, FIBRES, OWNER, SPAN_LENGTH,
                        IOF, PROTECTED, LINK1, LINK2, EO, ID,
                        SEGMENT_ID, BUILD_DATE, CONSTRUCT_TYPE, geometry, generated_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    properties.get('NAME'),
                    properties.get('CABLE_STATUS'),
                    properties.get('FIBRES'),
                    properties.get('OWNER'),
                    properties.get('SPAN_LENGTH'),
                    properties.get('IOF'),
                    properties.get('PROTECTED'),
                    properties.get('LINK1'),
                    properties.get('LINK2'),
                    properties.get('EO'),
                    properties.get('ID'),
                    properties.get('SEGMENT_ID'),
                    properties.get('BUILD_DATE'),
                    properties.get('CONSTRUCT_TYPE'),
                    json.dumps(feature.get('geometry')),
                    generated_id
                ))
                changes['new'] += 1
        return changes

    def update_splicecases_data(self, cursor, data):
        changes = {'new': 0, 'updated': 0, 'unchanged': 0}
        for feature in data['features']:
            properties = feature['properties']
            geometry = feature['geometry']
            generated_id = self.generate_splicecase_hash(properties, geometry)
            cursor.execute('SELECT RESTRICTED, RS_CODE, RS_COMMENTS, VMR_LINK FROM SpliceCases WHERE generated_id = ?', (generated_id,))
            existing_record = cursor.fetchone()

            if existing_record:
                if (existing_record[0] != properties.get('RESTRICTED') or
                    existing_record[1] != properties.get('RS_CODE') or
                    existing_record[2] != properties.get('RS_COMMENTS') or
                    existing_record[3] != properties.get('VMR_LINK')):
                    cursor.execute('''
                        UPDATE SpliceCases
                        SET RESTRICTED = ?, RS_CODE = ?, RS_COMMENTS = ?, VMR_LINK = ?
                        WHERE generated_id = ?
                    ''', (
                        properties.get('RESTRICTED'),
                        properties.get('RS_CODE'),
                        properties.get('RS_COMMENTS'),
                        properties.get('VMR_LINK'),
                        generated_id
                    ))
                    changes['updated'] += 1
                else:
                    changes['unchanged'] += 1
            else:
                cursor.execute('''
                    INSERT INTO SpliceCases (
                        NAME, ADDRESS, SUBURB, BUTTSPLICE, RESTRICTED,
                        RS_CODE, RS_COMMENTS, MODEL, MANHOLE, OWNER,
                        VMR_LINK, EO, BUILDDATE, JOBNUMBER, ID, geometry, generated_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    properties.get('NAME'),
                    properties.get('ADDRESS'),
                    properties.get('SUBURB'),
                    properties.get('BUTTSPLICE'),
                    properties.get('RESTRICTED'),
                    properties.get('RS_CODE'),
                    properties.get('RS_COMMENTS'),
                    properties.get('MODEL'),
                    properties.get('MANHOLE'),
                    properties.get('OWNER'),
                    properties.get('VMR_LINK'),
                    properties.get('EO'),
                    properties.get('BUILDDATE'),
                    properties.get('JOBNUMBER'),
                    properties.get('ID'),
                    json.dumps(feature.get('geometry')),
                    generated_id
                ))
                changes['new'] += 1
        return changes

    def run_tool(self, cable_file, splicecase_file):
        """Update the SQLite database using the GeoJSON files."""
        conn = None
        # --- Modified: use a temporary new database file ---
        new_db_path = os.path.join(self.current_dir, 'database_new.db')
        try:
            if not cable_file and not splicecase_file:
                messagebox.showerror(
                    "No Files Selected",
                    "Please ensure at least one GeoJSON file exists to perform the update."
                )
                return

            # Connect to the **new** database, not the live one
            conn = sqlite3.connect(new_db_path)
            cursor = conn.cursor()

            # Create tables if they don't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Cable (
                    NAME TEXT,
                    CABLE_STATUS TEXT,
                    FIBRES INTEGER,
                    OWNER TEXT,
                    SPAN_LENGTH REAL,
                    IOF TEXT,
                    PROTECTED TEXT,
                    LINK1 TEXT,
                    LINK2 TEXT,
                    EO TEXT,
                    ID TEXT,
                    SEGMENT_ID TEXT,
                    BUILD_DATE TEXT,
                    CONSTRUCT_TYPE TEXT,
                    geometry TEXT,
                    generated_id TEXT UNIQUE
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS SpliceCases (
                    NAME TEXT,
                    ADDRESS TEXT,
                    SUBURB TEXT,
                    BUTTSPLICE TEXT,
                    RESTRICTED TEXT,
                    RS_CODE TEXT,
                    RS_COMMENTS TEXT,
                    MODEL TEXT,
                    MANHOLE TEXT,
                    OWNER TEXT,
                    VMR_LINK TEXT,
                    EO TEXT,
                    BUILDDATE TEXT,
                    JOBNUMBER TEXT,
                    ID TEXT,
                    geometry TEXT,
                    generated_id TEXT UNIQUE
                )
            ''')

            conn.execute('BEGIN')

            total_changes = {
                'cable': {'new': 0, 'updated': 0, 'unchanged': 0},
                'splicecases': {'new': 0, 'updated': 0, 'unchanged': 0}
            }

            if cable_file:
                with open(cable_file, 'r', encoding='ascii') as f:
                    cable_data = json.load(f)
                total_changes['cable'] = self.update_cable_data(cursor, cable_data)

            if splicecase_file:
                with open(splicecase_file, 'r', encoding='ISO-8859-1') as f:
                    splicecases_data = json.load(f)
                total_changes['splicecases'] = self.update_splicecases_data(cursor, splicecases_data)

            conn.commit()

            # Build the summary message
            message = f"Update Complete\n\nDatabase Location:\n{self.db_path}\n\n"
            if cable_file:
                message += (
                    f"Cable changes:\n"
                    f"- {total_changes['cable']['new']} new\n"
                    f"- {total_changes['cable']['updated']} updated\n"
                    f"- {total_changes['cable']['unchanged']} unchanged\n\n"
                )
            if splicecase_file:
                message += (
                    f"SpliceCase changes:\n"
                    f"- {total_changes['splicecases']['new']} new\n"
                    f"- {total_changes['splicecases']['updated']} updated\n"
                    f"- {total_changes['splicecases']['unchanged']} unchanged\n"
                )

            # --- Modified: swap in the new DB on success ---
            conn.close()
            if os.path.exists(self.db_path):
                os.remove(self.db_path)
            os.rename(new_db_path, self.db_path)

            messagebox.showinfo("Update Complete", message)

        except FileNotFoundError as e:
            messagebox.showerror("File Not Found", f"An input file could not be found: {e}")
        except json.JSONDecodeError as e:
            messagebox.showerror("JSON Error", f"Invalid JSON in input file: {e}")
        except sqlite3.Error as e:
            # If anything goes wrong with SQLite, roll back and delete the bad temp DB
            if conn:
                conn.rollback()
                conn.close()
            if os.path.exists(new_db_path):
                os.remove(new_db_path)
            messagebox.showerror("Database Error", f"A database error occurred: {e}")
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            # Clean up temp on unexpected errors
            if conn:
                conn.close()
            if os.path.exists(new_db_path):
                os.remove(new_db_path)
            messagebox.showerror("Unexpected Error", f"An unexpected error occurred: {e}")


########################################################################
# Fibre Check Tool
########################################################################

class FibreProcessor:
    def __init__(self, parent):
        """
        Create the 'Fibre Check Tool' UI inside `parent`.
        """
        self.parent = parent
        self.parent_frame = ttk.Frame(parent, padding="10")
        self.parent_frame.pack(fill='both', expand=True)

        # real toplevel (used by protocol hooks / dialogs)
        self.root = self.parent_frame.winfo_toplevel()

        self.fibre_type = tk.StringVar()
        self.current_selection = None
        self.create_ui()

        # Database path (using current directory)
        self.db_path = os.path.join(os.getcwd(), "database.db")

        # cross-section cache
        self.cs_cache = CrossSectionCache()

        # progress bar (hidden until used) — use parent_frame, not main_frame
        self.progress_frame = ttk.Frame(self.parent_frame)
        self.progress = ttk.Progressbar(self.progress_frame, orient="horizontal", mode="determinate", length=260)
        self.progress_label = ttk.Label(self.progress_frame, text="Crawling…")
        self.progress.grid(row=0, column=0, padx=(0, 8))
        self.progress_label.grid(row=0, column=1)

        # ensure cache is purged when window closes
        try:
            self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        except Exception:
            pass

    # --- REPLACE the entire create_ui() in class FibreProcessor with this ---

    def create_ui(self):
        # Source selection (CSV only, VMR disabled for now)
        source_frame = ttk.Frame(self.parent_frame)
        source_frame.grid(row=0, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=(0,4))
        ttk.Label(source_frame, text="Source:").grid(row=0, column=0, padx=(0,6))
        self.source_var = tk.StringVar(value="CSV")
        self.source_combo = ttk.Combobox(
            source_frame,
            textvariable=self.source_var,
            values=["CSV"],     # VMR removed from choices
            state="disabled",   # disabled for users (CSV only)
            width=10
        )
        self.source_combo.grid(row=0, column=1, padx=(0,10))

        # CSV file input (shown for CSV)
        self.file_frame = ttk.Frame(self.parent_frame)
        self.file_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        ttk.Label(self.file_frame, text="Select Input CSV File:").grid(row=0, column=0, padx=5)
        self.input_entry = ttk.Entry(self.file_frame, width=52)
        self.input_entry.grid(row=0, column=1, padx=5)
        ttk.Button(self.file_frame, text="Browse...", command=self.browse_file).grid(row=0, column=2, padx=5)

        # VMR ID input (hidden/disabled while VMR is disabled)
        self.vmr_frame = ttk.Frame(self.parent_frame)
        self.vmr_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        ttk.Label(self.vmr_frame, text="VMR Job/WO ID (digits):").grid(row=0, column=0, padx=5)
        self.vmr_id_entry = ttk.Entry(self.vmr_frame, width=20)
        self.vmr_id_entry.grid(row=0, column=1, padx=5)
        # Hide VMR frame since source is CSV-only now
        self.vmr_frame.grid_remove()

        # Connect VMR (for Cross-Section crawl) — allow for CSV too
        conn_frame = ttk.Frame(self.parent_frame)
        conn_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        self.crawl_enabled = tk.BooleanVar(value=True)
        self.crawl_check = ttk.Checkbutton(conn_frame, text="Connect VMR", variable=self.crawl_enabled)
        self.crawl_check.grid(row=0, column=0, padx=5)

        # Process
        ttk.Button(self.parent_frame, text="Process", command=self.process_data)\
            .grid(row=4, column=0, columnspan=3, pady=10)

        # Results table
        self.create_treeview(self.parent_frame)
        self.tree.bind("<Motion>", lambda e: "break" if self.tree.identify_region(e.x, e.y) == "separator" else None)

        ttk.Label(self.parent_frame, text="developed by Jian", foreground="gray")\
            .grid(row=6, column=0, columnspan=3, pady=(10, 0))


        self.parent_frame.columnconfigure(0, weight=1)
        self.parent_frame.rowconfigure(5, weight=1)
        self.setup_copy_functionality()

        self._toggle_source_inputs()  # initialize states

    # === ADD inside class FibreProcessor =========================================
    def browse_file(self):
        """
        Open a file picker and put the chosen path into the 'Select Input CSV File' entry.
        """
        # Import here to avoid surprises if tkinter.filedialog wasn't imported at module top
        try:
            from tkinter import filedialog as fd
        except Exception:
            import tkinter.filedialog as fd

        filetypes = [
            ("CSV files", "*.csv;*.CSV"),
            ("Excel-exported CSV", "*.xls.csv;*.XLS.CSV"),
            ("All files", "*.*"),
        ]
        path = fd.askopenfilename(title="Select input CSV", filetypes=filetypes)
        if not path:
            return
        try:
            self.input_entry.delete(0,  tk.END)
            self.input_entry.insert(0, path)
        except Exception:
            # Fallback: if entry not yet created for some reason, ignore silently
            pass

    # --- REPLACE the entire _toggle_source_inputs() in class FibreProcessor with this ---

    def _toggle_source_inputs(self):
        src = (self.source_var.get() or "CSV").upper()
        if src == "CSV":
            # enable CSV inputs
            for child in self.file_frame.winfo_children():
                child.configure(state="normal")
            # disable VMR ID
            for child in self.vmr_frame.winfo_children():
                child.configure(state="disabled")
            # Connect VMR checkbox: leave as-is (user choice)
        else:  # VMR
            # disable CSV inputs
            for child in self.file_frame.winfo_children():
                child.configure(state="disabled")
            # enable VMR ID, default Connect VMR ON
            for child in self.vmr_frame.winfo_children():
                child.configure(state="normal")
            try:
                self.crawl_enabled.set(True)
            except Exception:
                pass

    def create_treeview(self, parent):
        self.row_meta = {}  # item_id -> {"segment_id": "..."}

        # NOTE: moved from row=3 to row=5 so it doesn't overlap type_frame (CSV controls)
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=5, column=0, columnspan=3,
                        sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)

        # Columns: added "IOF" and "DWDM/T_ found" between Tube and Fibre Tray
        columns = (
            "A-End", "Fibre Cable", "B-End", "Connect/Disconnect",
            "EO", "Length", "Tube", "RS Type", "IOF", "DWDM/T_ found",
            "Fibre Tray", "Commentary"
        )

        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')

        # More-visible scrollbars
        vsb = tk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview, width=18)
        hsb = tk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview, width=18)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        vsb.grid(row=0, column=1, sticky=(tk.N, tk.S))
        hsb.grid(row=1, column=0, sticky=(tk.W, tk.E))

        # Headings + initial widths (centered, non-resizable)
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=max(90, min(360, len(col)*10)), anchor="center", stretch=False)

        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        # Double-click
        self.tree.bind("<Double-1>", self.on_tree_double_click)
        # no background tag for alerts anymore; labeling via columns


    def setup_copy_functionality(self):
        self.context_menu = tk.Menu(self.parent_frame, tearoff=0)
        self.context_menu.add_command(label="Copy", command=self.copy_selection)
        self.tree.bind("<Button-3>", self.show_context_menu)
        self.tree.bind("<Control-c>", self.copy_selection)
        self.tree.bind("<ButtonRelease-1>", self.on_select)

    # >>> NEW: window close cleanup
    def _on_close(self):
        try:
            self.cs_cache.clear()
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass

    # ---------------------------------------------------------------------
    # Helper: extract cross-section table headers + rows from cached HTML
    # ---------------------------------------------------------------------
    def _extract_cross_section_table(html_text):
        """
        Parse the Cross Section HTML (WorkFolder.aspx or FibreTrace.aspx)
        and extract (headers, rows) from the first large <table> with grid lines.
        Returns (headers:list[str], rows:list[list[str]]).
        Safe fallback if cached headers/rows are missing.
        """
        from bs4 import BeautifulSoup
        import re

        soup = BeautifulSoup(html_text, "lxml")

        # Try common IDs first
        tbl = soup.find("table", id="gvCrossSection") \
            or soup.find("table", id="GridView1") \
            or soup.find("table", id="MainContent_GridView1")

        # If none, pick the widest visible table (most cells)
        if not tbl:
            all_tables = soup.find_all("table")
            tbl = max(all_tables, key=lambda t: len(t.find_all("td")), default=None)

        if not tbl:
            return [], []

        headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
        rows = []
        for tr in tbl.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            cells = [re.sub(r"\s+", " ", td.get_text(" ", strip=True)) for td in tds]
            rows.append(cells)
        return headers, rows


    # ---------------------------------------------------------------------
    # Helper: auto-size Treeview columns to content width
    # ---------------------------------------------------------------------
    from tkinter import font as tkfont

    def _autosize_columns(self, tree):
        # get font from the style instead of widget
        style = ttk.Style()
        try:
            font_name = style.lookup("Treeview", "font")
            if not font_name:
                font_name = "TkDefaultFont"
            font = tkfont.nametofont(font_name)
        except Exception:
            font = tkfont.nametofont("TkDefaultFont")

        for col in tree["columns"]:
            max_width = font.measure(col)
            for item in tree.get_children():
                text = str(tree.set(item, col))
                max_width = max(max_width, font.measure(text))
            tree.column(col, width=max_width + 20)

    def on_tree_double_click(self, event):
        item = self.tree.identify_row(event.y)
        colid = self.tree.identify_column(event.x)
        if not item or not colid:
            return

        col_index = int(colid.replace('#', '')) - 1
        values = self.tree.item(item, 'values')
        columns = self.tree["columns"]
        col_name = columns[col_index]
        meta = getattr(self, "row_meta", {}).get(item, {})
        seg_id = (meta.get("segment_id") or "").strip()

        # Open VMR page when double-clicking Fibre Cable
        if col_name == "Fibre Cable":
            if seg_id:
                webbrowser.open_new(VMR_Cable_URL + seg_id)
            else:
                messagebox.showwarning("No SEGMENT_ID", "SEGMENT_ID not found for this cable.")
            return

        # Only open Cross Section when double-clicking Fibre Tray
        if col_name != "Fibre Tray":
            return

        # Tray range shown in main table (e.g., "1-6")
        try:
            tray_idx = columns.index("Fibre Tray")
        except ValueError:
            tray_idx = -1
        tray_range = (values[tray_idx] if 0 <= tray_idx < len(values) else "").strip()

        # ---- HARD GUARDS (as required) ----
        if not self.crawl_enabled.get():
            messagebox.showinfo("Connect VMR is OFF", "Turn on 'Connect VMR' to open the cross section.")
            return
        if not tray_range:
            # Display rule hides tray unless current OR previous displayed row has C/D.
            # When hidden, do not open cross section.
            messagebox.showinfo("No Fibre Tray", "This row does not show a Fibre Tray value. Cross section opens only when a tray value is displayed.")
            return
        if not seg_id:
            messagebox.showwarning("No SEGMENT_ID", "SEGMENT_ID not found for this cable.")
            return

        # Get cached or live HTML + parsed table
        try:
            html_text = self.cs_cache.get_html(seg_id)
            if not html_text:
                resp = requests.get(VMR_Cable_URL + seg_id, headers={"User-Agent": "FibreAssist/1.0"}, timeout=30, verify=True)
                resp.raise_for_status()
                self.cs_cache.put_html(seg_id, resp.text)
                html_text = resp.text

            headers = self.cs_cache.headers_for(seg_id)
            rows = self.cs_cache.rows_for(seg_id)
            if not headers or not rows:
                headers, rows = self._extract_cross_section_table(html_text)
        except Exception as e:
            messagebox.showerror("Parse Error", str(e))
            return

        # ---- Apply your rule *without* inferring tray when hidden ----
        # Decide full vs tray-filtered using UI column "DWDM/T_ found" if present; else legacy tag.
        try:
            dwdm_idx = columns.index("DWDM/T_ found")
        except ValueError:
            dwdm_idx = -1
        val_dwdm = (values[dwdm_idx] if 0 <= dwdm_idx < len(values) else "").strip().upper()
        row_tags = self.tree.item(item, "tags") or ()
        has_alert = (val_dwdm == "Y") or ("cs_alert" in row_tags)

        # Filter helper (expects "N-M")
        def _filter_rows_by_tray_range(_rows, rng):
            try:
                a, b = [int(x) for x in str(rng).split("-", 1)]
            except Exception:
                return _rows[:]  # if malformed, safest to show full
            lo, hi = min(a, b), max(a, b)

            # find a "fibre number" column heuristically
            import re
            # pick the column with the most 1..N integers
            best_ci, best_hits = None, -1
            for ci in range(len(headers or [])):
                hits = 0
                for r in _rows:
                    if ci < len(r):
                        m = re.search(r"\b(\d{1,4})\b", str(r[ci]))
                        if m:
                            hits += 1
                if hits > best_hits:
                    best_ci, best_hits = ci, hits

            if best_ci is None or best_hits <= 0:
                return _rows[:]  # cannot determine fibre column; show full

            def in_tray(n):
                if n is None:
                    return False
                tray_no = ((n - 1) // 6) + 1
                return ((tray_no - 1) * 6 + 1) >= lo and ((tray_no - 1) * 6 + 6) <= hi

            out = []
            for r in _rows:
                m = re.search(r"\b(\d{1,4})\b", str(r[best_ci])) if best_ci < len(r) else None
                n = int(m.group(1)) if m else None
                if n is not None:
                    # keep fibres whose tray bucket falls fully within the range
                    if ((n - 1) // 6) * 6 + 1 >= lo and ((n - 1) // 6) * 6 + 6 <= hi:
                        out.append(r)
            return out or _rows[:]  # never return empty silently

        full_view = has_alert
        subset = rows[:] if full_view else _filter_rows_by_tray_range(rows, tray_range)

        # Drop any "Tag" column coming from VMR
        def strip_tag_col(_headers, _rows):
            if not _headers:
                return _headers, _rows
            try:
                i = [h.strip().lower() for h in _headers].index("tag")
            except ValueError:
                return _headers, _rows
            new_headers = _headers[:i] + _headers[i+1:]
            new_rows = [r[:i] + r[i+1:] if i < len(r) else r[:] for r in _rows]
            return new_headers, new_rows

        headers, subset = strip_tag_col(headers, subset)

        # --- Add "Tray" column as first col in popup ---
        import re as _re
        def _fibre_num_of_row(r, hdrs):
            # best-effort: choose the column with most integers (same as above)
            best_ci, best_hits = None, -1
            for ci in range(len(hdrs or [])):
                hits = 0
                for rr in subset:
                    if ci < len(rr):
                        mm = _re.search(r"\b(\d{1,4})\b", str(rr[ci]))
                        if mm:
                            hits += 1
                if hits > best_hits:
                    best_ci, best_hits = ci, hits
            if best_ci is None:
                return None
            mm = _re.search(r"\b(\d{1,4})\b", str(r[best_ci])) if best_ci < len(r) else None
            return int(mm.group(1)) if mm else None

        def _tray_of(n):
            try:
                return str(((int(n) - 1) // 6) + 1)
            except Exception:
                return ""

        headers2 = ["Tray"] + list(headers or [])
        rows2 = []
        for r in subset:
            n = _fibre_num_of_row(r, headers)
            rows2.append([_tray_of(n)] + r)

        # ---- UI window ----
        win = tk.Toplevel(self.root)
        win.title(f"Cross Section Details – {seg_id} [{'Full Table' if full_view else tray_range}]")
        win.geometry("1200x720")

        top = ttk.Frame(win); top.pack(fill="x", padx=10, pady=8)
        ttk.Label(top, text=f"ID: {seg_id}   URL: {VMR_Cable_URL}{seg_id}").pack(side="left")
        if full_view:
            ttk.Label(top, text="Showing entire cross-section (DWDM/Trunk found).", foreground="#B00020").pack(side="right")

        table_frame = ttk.Frame(win); table_frame.pack(fill="both", expand=True, padx=10, pady=(0,10))
        tree = ttk.Treeview(table_frame, columns=tuple(headers2 or []), show="headings", height=26)
        vsb = tk.Scrollbar(table_frame, orient="vertical", command=tree.yview, width=18)
        hsb = tk.Scrollbar(table_frame, orient="horizontal", command=tree.xview, width=18)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew"); vsb.grid(row=0, column=1, sticky="ns"); hsb.grid(row=1, column=0, sticky="ew")
        table_frame.columnconfigure(0, weight=1); table_frame.rowconfigure(0, weight=1)

        tree.tag_configure("alert", background="#fff3cd")

        for col in (headers2 or []):
            tree.heading(col, text=col)
            tree.column(col, width=max(90, min(360, len(col)*10)), anchor="center", stretch=False)

        # Highlight DWDM/Trunk rows
        colmap = {h.lower(): i for i, h in enumerate(headers2 or [])}
        idx_os = colmap.get("os name")
        idx_bearer = colmap.get("bearer id")

        def row_is_alert(r):
            os_name = (r[idx_os] if idx_os is not None and idx_os < len(r) else "").strip().upper()
            bearer  = (r[idx_bearer] if idx_bearer is not None and idx_bearer < len(r) else "").strip().upper()
            if os_name.startswith("T_"):
                return True
            if "OTS" in bearer or "DWDM" in bearer:
                return True
            return False

        for r in rows2:
            tree.insert("", "end", values=r, tags=("alert",) if row_is_alert(r) else ())

        # NOTE: this calls the INSTANCE method; ensure its signature is def _autosize_columns(self, tree, ...)
        self._autosize_columns(tree)
        win.bind("<Escape>", lambda e: win.destroy())


    def on_select(self, event):
        selection = self.tree.selection()
        if selection:
            item = selection[0]
            column = self.tree.identify_column(event.x)
            self.current_selection = (item, column)

    def show_context_menu(self, event):
        try:
            self.tree.selection_set(self.tree.identify_row(event.y))
            self.current_selection = (self.tree.identify_row(event.y), self.tree.identify_column(event.x))
            self.context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.context_menu.grab_release()

    def copy_selection(self, event=None):
        try:
            if self.current_selection:
                item, column = self.current_selection
                if item and column:
                    col_num = int(column.replace('#', '')) - 1
                    value = self.tree.item(item)['values'][col_num]
                    self.parent.clipboard_clear()
                    self.parent.clipboard_append(value)
                    self.parent.update()
        except Exception as e:
            print(f"Copy failed: {e}")

    def select_input_file(self):
        input_file = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if input_file:
            self.input_entry.delete(0, tk.END)
            self.input_entry.insert(0, input_file)

    def adjust_column_widths(self):
        # Use the actual fonts that Treeview and its headings use
        style = ttk.Style()
        body_font_name = style.lookup("Treeview", "font") or "TkDefaultFont"
        heading_font_name = style.lookup("Treeview.Heading", "font") or body_font_name
        body_font = tkfont.nametofont(body_font_name)
        heading_font = tkfont.nametofont(heading_font_name)

        # Make sure all rows are laid out before measuring
        self.tree.update_idletasks()

        for column in self.tree["columns"]:
            # Width of the header
            header_text = self.tree.heading(column)["text"]
            header_width = heading_font.measure(header_text) + 24  # a little padding

            # Max width of any cell in this column
            col_index = self.tree["columns"].index(column)
            max_content_width = header_width
            for item_id in self.tree.get_children(""):
                values = self.tree.item(item_id, "values")
                text = str(values[col_index]) if col_index < len(values) else ""
                max_content_width = max(max_content_width, body_font.measure(text) + 24)

            # Apply width and prevent the layout from re-stretching it
            computed = max(80, max_content_width)
            self.tree.column(column, width=computed, minwidth=computed, stretch=False, anchor="center")

    def process_csv(self, input_file):
        with open(input_file, 'r', encoding='cp1252') as csvfile:
            reader = csv.reader(csvfile)
            data = list(reader)

        # Find the index where the Fibre Trace Details start.
        start_index = None
        self.show_next_tray = False
        for i, row in enumerate(data):
            if any("Fibre Trace Details" in cell for cell in row):
                start_index = i + 1
                break

        if start_index is None:
            raise ValueError("'Fibre Trace Details' not found in the file.")

        data = data[start_index:]

        # ---------- UPDATED HEADERS: add "IOF" and "DWDM/T_ found" next to "Tube" ----------
        headers = [
            "Cable#", "A-End", "Fibre Cable", "B-End",
            "Connect/Disconnect", "EO", "Length",
            "Tube", "RS Type", "IOF", "DWDM/T_ found", "Fibre Tray"
        ]

        data[0] = headers

        processed_data = [headers]
        selected_fibres_list = []

        i = 1
        while i < len(data):
            row = data[i]
            if row[0].strip():
                cable_section = [row]
                i += 1
                while i < len(data) and not data[i][0].strip():
                    cable_section.append(data[i])
                    i += 1

                cable_info = cable_section[0]
                cable_num = cable_info[0]
                a_end = cable_info[1]

                # --- existing clean-up for Fibre Cable (keep up to ")") ---
                fibre_cable_raw = cable_info[2]
                if ")" in fibre_cable_raw:
                    fibre_cable = fibre_cable_raw.split(")")[0] + ")"
                else:
                    fibre_cable = fibre_cable_raw

                b_end = cable_info[3]

                # --- existing clean-up for Connect/Disconnect (remove first char after "t" if not a space) ---
                connect_disconnect_raw = cable_info[4]
                if "t" in connect_disconnect_raw:
                    t_index = connect_disconnect_raw.index("t")
                    if t_index + 1 < len(connect_disconnect_raw):
                        if connect_disconnect_raw[t_index + 1] != " ":
                            connect_disconnect = (
                                connect_disconnect_raw[:t_index + 1] +
                                connect_disconnect_raw[t_index + 2:]
                            )
                        else:
                            connect_disconnect = connect_disconnect_raw
                    else:
                        connect_disconnect = connect_disconnect_raw
                else:
                    connect_disconnect = connect_disconnect_raw

                eo = cable_info[5] if len(cable_info) > 5 else ""
                length = cable_info[6] if len(cable_info) > 6 else ""

                # Extract selected fibre number from "Fibre Cable" text, e.g. "... (#37)"
                selected_fibre = None
                m = re.search(r'\(#\s*(\d+)\s*\)', fibre_cable)
                if m:
                    selected_fibre = int(m.group(1))
                else:
                    selected_fibre = 0  # fallback; keeps code resilient

                selected_fibres_list.append(selected_fibre)

                total_fibres = None
                if len(cable_section) >= 3:
                    total_fibres_row = cable_section[2]
                    if len(total_fibres_row) >= 3:
                        total_fibres_cell = total_fibres_row[2]
                        fibres_match = re.search(r'(\d+\.?\d*)m, (\d+)fibres', total_fibres_cell)
                        if fibres_match:
                            total_fibres = int(fibres_match.group(2))

                # ---------- CAN2000 tube classification (unchanged rules, FSS override removed) ----------
                tube = "Non-CAN2000"
                if fibre_cable.startswith("BLS") and total_fibres and total_fibres >= 144:
                    if selected_fibre <= 24:
                        tube = "Trunk"
                    elif selected_fibre > total_fibres - 24:
                        tube = "Local"
                    else:
                        tube = "Junction"
                else:
                    a_end_type = "AJL" if "AJL" in a_end else "BJL" if "BJL" in a_end else "FJL" if "FJL" in a_end else None
                    b_end_type = "AJL" if "AJL" in b_end else "BJL" if "BJL" in b_end else "FJL" if "FJL" in b_end else None

                    if (a_end_type == "BJL" and b_end_type == "BJL") or (a_end_type == "FJL" and b_end_type == "BJL") or (a_end_type == "BJL" and b_end_type == "FJL"):
                        if selected_fibre <= 24:
                            tube = "Trunk"
                        elif total_fibres and selected_fibre > total_fibres - 24:
                            tube = "Local"
                        else:
                            tube = "Junction"
                    elif ((a_end_type == "AJL" and b_end_type == "BJL") or
                          (a_end_type == "BJL" and b_end_type == "AJL")):
                        if total_fibres == 144:
                            if 49 <= selected_fibre <= 72 or 121 <= selected_fibre <= 144:
                                tube = "Local"
                            else:
                                tube = "Junction"
                        else:
                            if selected_fibre and total_fibres and selected_fibre > total_fibres - 24:
                                tube = "Local"
                            else:
                                tube = "Junction"

                # --- NEW: FSS exception to CAN2000 ---
                # If the cable is FSS, force Non-CAN2000 UNLESS both ends are BJL splice cases.
                if self._is_fss_cable(fibre_cable):
                    if not (self._is_bjl_splice_case(a_end) and self._is_bjl_splice_case(b_end)):
                        tube = "Non-CAN2000"


                # ---------- Fibre Tray calculation (groups of 6) ----------
                tray_start = ((max(selected_fibre, 1) - 1) // 6) * 6 + 1
                tray_end = tray_start + 5
                fibre_tray = f"{tray_start}-{tray_end}"

                # ---------- UPDATED DISPLAY RULE for Fibre Tray ----------
                # Show tray only if current row OR previous displayed row has non-empty Connect/Disconnect.
                curr_has_conn = bool(str(connect_disconnect).strip())
                prev_has_conn = False
                if len(processed_data) > 1:  # previous displayed row exists
                    prev_row = processed_data[-1]
                    # safe access: our rows are always the fixed header schema
                    prev_has_conn = bool(str(prev_row[4]).strip())
                display_tray = fibre_tray if (curr_has_conn or prev_has_conn) else ""

                # ---------- include IOF + DWDM/T_ placeholders and Fibre Tray in output row ----------
                processed_data.append([
                    cable_num, a_end, fibre_cable, b_end, connect_disconnect, eo, length,
                    tube, "", "",  # IOF, DWDM/T_ found (to be filled later)
                    display_tray
                ])


            else:
                i += 1

        return processed_data, selected_fibres_list
# --- NEW: add inside class FibreProcessor (e.g., after process_csv) ---

    def _parse_fibretrace_table(self, html_text: str):
        """
        Returns (headers, rows) from the main FibreTrace table.
        We try GridView2 first; otherwise first sizeable table on the page.
        """
        soup = BeautifulSoup(html_text, _BS_PARSER)
        tbl = soup.find(id="GridView2") or soup.find("table", id="MainContent_GridView2")
        if not tbl:
            # fallback: pick the widest table as the trace table
            candidates = soup.find_all("table")
            tbl = max(candidates, key=lambda t: len(t.find_all("tr")) * len(t.find_all("td")), default=None)
        return _table_extract(tbl)

    def _map_html_row(self, headers, row):
        """
        Map a single HTML row (list) to a dict with CSV-like keys via HTML_FIELD_MAP.
        Also parses the 'Name' column for embedded metrics (length/fibres/WK/SP).
        """
        hl = [h.lower() for h in headers]

        # Build a header index lookup for all targets
        idx_map = {}
        for tgt, aliases in HTML_FIELD_MAP.items():
            found = None
            for i, h in enumerate(hl):
                if any(a.lower() in h for a in aliases):
                    found = i; break
            idx_map[tgt] = found

        out = {k: "" for k in HTML_FIELD_MAP.keys()}

        for tgt, idx in idx_map.items():
            if idx is not None and idx < len(row):
                out[tgt] = row[idx]

        # ---- NEW: parse Name column details and attach structured values ----
        name_text = out.get("Name", "")
        meta = self._extract_from_name(name_text)
        # Keep meta fields for downstream normalization
        out["_name_length_m"]   = meta["length_m"]
        out["_name_total_fib"]  = meta["total_fibres"]
        out["_name_wk"]         = meta["wk"]
        out["_name_sp"]         = meta["sp"]

        return out
    
    # --- ADD these helpers near your other parsing utilities (once) ---
    @staticmethod
    def _infer_fibre_type_from_summary_name(name: str) -> str:
        """
        Map Fibre Trace Summary 'Name' prefix to Fibre Type:
        L_ -> Local, J_ -> Junction, T_ -> Trunk
        """
        if not name:
            return "Local"  # safe default
        s = name.strip().upper()
        if s.startswith("L_"): return "Local"
        if s.startswith("J_"): return "Junction"
        if s.startswith("T_"): return "Trunk"
        return "Local"  # fallback
    @staticmethod
    def _parse_summary_name(html_text: str) -> str:
        """
        Parse Fibre Trace Summary table and extract the 'Name' field (fibre path name).
        Returns empty string if not found.
        """
        soup = BeautifulSoup(html_text, _BS_PARSER)

        # Try explicit IDs first
        summary_tbl = soup.find(id="GridView1") or soup.find("table", id="MainContent_GridView1")
        if not summary_tbl:
            # Fallback: find table with a header containing 'Fibre Trace Summary'
            for cap in soup.find_all(["caption", "h2", "h3", "div"]):
                if cap.get_text(strip=True).lower().startswith("fibre trace summary"):
                    parent_table = cap.find_next("table")
                    if parent_table:
                        summary_tbl = parent_table
                        break
        if not summary_tbl:
            # Last resort: pick a table that has a 'Name' header and a few rows
            candidates = []
            for tbl in soup.find_all("table"):
                ths = [th.get_text(strip=True) for th in tbl.find_all("th")]
                if any(h.strip().lower() == "name" for h in ths) and len(tbl.find_all("tr")) >= 2:
                    candidates.append(tbl)
            if candidates:
                summary_tbl = candidates[0]

        if not summary_tbl:
            return ""

        # Extract headers/rows
        headers, rows = _table_extract(summary_tbl)
        headers_lower = [h.lower() for h in headers]
        if not headers or "name" not in headers_lower:
            return ""

        name_idx = headers_lower.index("name")
        for r in rows:
            if name_idx < len(r) and r[name_idx].strip():
                return r[name_idx].strip()

        return ""


    # --- REPLACE this whole method inside class FibreProcessor ---
    @staticmethod
    def _extract_from_name(name_text: str) -> dict:
        """
        Parse tokens in a 'Name' cell like:
        '00.00m, 890.00m, 312fibres, 12WK 60SP'
        Returns: {"length_m": float|None, "total_fibres": int|None, "wk": int|None, "sp": int|None}
        """
        import re

        NAME_LEN_RE    = re.compile(r'(\d+(?:\.\d+)?)\s*m\b', re.IGNORECASE)
        NAME_TOTFIB_RE = re.compile(r'(\d+)\s*fibres?\b', re.IGNORECASE)
        NAME_WK_RE     = re.compile(r'(\d+)\s*WK\b', re.IGNORECASE)
        NAME_SP_RE     = re.compile(r'(\d+)\s*SP\b', re.IGNORECASE)

        if not name_text:
            return {"length_m": None, "total_fibres": None, "wk": None, "sp": None}

        # length(s): choose the maximum in case of start/end markers
        lengths = [float(x) for x in NAME_LEN_RE.findall(name_text)]
        length_m = max(lengths) if lengths else None

        tot = None
        m = NAME_TOTFIB_RE.search(name_text)
        if m:
            tot = int(m.group(1))

        wk = None
        m = NAME_WK_RE.search(name_text)
        if m:
            wk = int(m.group(1))

        sp = None
        m = NAME_SP_RE.search(name_text)
        if m:
            sp = int(m.group(1))

        return {"length_m": length_m, "total_fibres": tot, "wk": wk, "sp": sp}

    # === ADD inside class FibreProcessor (near your other @staticmethod helpers) ===
    @staticmethod
    def _is_fss_cable(name: str) -> bool:
        return bool(name and "FSS" in name.upper())

    @staticmethod
    def _is_bjl_splice_case(s: str) -> bool:
        return bool(s and "BJL" in s.upper())

    @staticmethod
    def _name_marks_iof(name: str) -> bool:
        """
        Treat any cable name containing _AP, _MA, _SB, _SM as IOF by naming rule.
        """
        if not name:
            return False
        up = name.upper()
        return any(tag in up for tag in ["_AP", "_MA", "_SB", "_SM"])


    # --- REPLACE your current process_vmr() with this version ---

    def process_vmr(self, vmr_id: str):
        """
        Crawl FibreTrace for `vmr_id`, parse Fibre Trace Summary to infer path name/type,
        parse Fibre Trace Details to rows, normalize, and return.
        Also sets self.fibre_type automatically based on the summary 'Name' (L_/J_/T_).
        """
        if not re.fullmatch(r"\d+", (vmr_id or "").strip()):
            raise ValueError("VMR ID must be numeric.")

        # Fetch & load HTML
        html_path = _vmr_crawl_fibretrace(vmr_id)
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()

        # --- NEW: infer fibre type from the Fibre Trace Summary 'Name'
        summary_name = self._parse_summary_name(html)
        inferred_type = self._infer_fibre_type_from_summary_name(summary_name)
        # Set fibre_type var so downstream commentary/tube mismatch logic can still read it
        self.fibre_type.set(inferred_type)

        # Parse the main details table
        headers, rows = self._parse_fibretrace_table(html)
        if not headers or not rows:
            raise RuntimeError("Could not find a valid FibreTrace table in the VMR page.")

        # Map & normalize (uses Name parsing for length/fibres as previously wired)
        mapped = [self._map_html_row(headers, r) for r in rows]
        processed_data, selected_fibres = self._normalize_vmr_rows(mapped)

        # Since Source=VMR, ensure CSV-only controls are visually hidden (defensive)
        try:
            self.type_frame.grid_remove()
        except Exception:
            pass

        return processed_data, selected_fibres


    def _normalize_vmr_rows(self, basic_rows):
        """
        Convert the minimal norm rows to exactly what process_csv() outputs:
        headers = ["Cable#", "A-End", "Fibre Cable", "B-End", "Connect/Disconnect", "EO", "Length", "Tube", "Fibre Tray"]
        We now also honor parsed values from 'Name' (length, fibres, WK/SP) when available.
        """
        headers = [
            "Cable#", "A-End", "Fibre Cable", "B-End",
            "Connect/Disconnect", "EO", "Length",
            "Tube", "RS Type", "IOF", "DWDM/T_ found", "Fibre Tray"
        ]
        out = [headers]
        selected = []

        # NOTE: we stashed the meta on each mapped dict; to reach it here, we’ll
        #       temporarily carry those dicts instead of plain strings.
        #       So basic_rows should be built from mapped dicts (we already do this in process_vmr()).
        #       If you've kept basic_rows as literal strings, keep reading—we’ll seamlessly handle both.

        # If caller has passed the enriched dicts (preferred path):
        if basic_rows and isinstance(basic_rows[0], list) and basic_rows[0] and basic_rows[0][0] == "Cable#":
            # current (older) path with simple lists -> keep behavior
            for row in basic_rows[1:]:
                cable_num, a_end, fibre_cable_raw, b_end, conn, eo, length = (row + [""]*7)[:7]
                fibre_cable = fibre_cable_raw.split(")")[0] + ")" if ")" in fibre_cable_raw else fibre_cable_raw

                m = re.search(r'\(#\s*(\d+)\s*\)', fibre_cable)
                sel = int(m.group(1)) if m else 0
                selected.append(sel)

                tube = "Non-CAN2000"
                tray = ""
                if str(conn).strip() and sel > 0:
                    start = ((sel - 1)//6)*6 + 1
                    end = start + 5
                    tray = f"{start}-{end}"

                out.append([cable_num, a_end, fibre_cable, b_end, conn, eo, length, tube, tray])

            return out, selected

        # NEW preferred path: process_vmr() will pass the mapped dicts directly
        # (so we can use the parsed meta from Name).
        for i, m in enumerate(basic_rows, start=1):
            # m is a mapped dict produced by _map_html_row()
            a_end       = m.get("A-End", "")
            fibre_raw   = m.get("Fibre Cable", "") or m.get("Name", "")
            b_end       = m.get("B-End", "")
            conn        = m.get("Connect/Disconnect", "")
            eo          = m.get("EO", "")
            length_html = (m.get("Length", "") or "").strip()

            # Preferred cable text
            fibre_cable = fibre_raw.split(")")[0] + ")" if ")" in fibre_raw else fibre_raw

            # Selected fibre number (if present)
            mm = re.search(r'\(#\s*(\d+)\s*\)', fibre_cable)
            sel = int(mm.group(1)) if mm else 0
            selected.append(sel)

            # Decide Length: if HTML Length missing, use parsed Name length
            length_val = length_html
            if not length_val:
                name_len = m.get("_name_length_m")
                if isinstance(name_len, (int, float)):
                    # format similar to CSV (e.g., '890.00m')
                    length_val = f"{name_len:.2f}m"

            # Reserve these if you later want to compute tube or commentary:
            total_fib = m.get("_name_total_fib")
            wk_fib    = m.get("_name_wk")
            sp_fib    = m.get("_name_sp")
            # (No change to tube rules here to keep this minimal and safe.)
            tube = "Non-CAN2000"

            tray = ""
            if str(conn).strip() and sel > 0:
                start = ((sel - 1)//6)*6 + 1
                end = start + 5
                tray = f"{start}-{end}"

            out.append([str(i), a_end, fibre_cable, b_end, conn, eo, length_val, tube, tray])

        return out, selected

    def process_data(self):
        """
        Process button:
        CSV  → infer Fibre Type from the CSV's Fibre Trace Summary Name (L_/J_/T_) and parse via process_csv(...)
        VMR  → crawl FibreTrace HTML, infer Fibre Type from Summary Name (L_/J_/T_), convert Details HTML → CSV-like,
                then reuse process_csv(...).
        Post-processing (parity, cross-section crawl, commentary, tagging, UI insert) remains identical.
        """
        import re, sqlite3, traceback, requests
        from tkinter import messagebox

        src = (self.source_var.get() or "CSV").upper()

        try:
            if src == "CSV":
                # ---- CSV path ----
                input_file = (self.input_entry.get() or "").strip()
                if not input_file:
                    messagebox.showerror("Error", "Please select an input CSV file first.")
                    return

                # Infer fibre type from CSV's Fibre Trace Summary 'Name' (L_/J_/T_)
                ft = _infer_ft_from_csv_summary_name(input_file)
                if ft:
                    self.fibre_type.set(ft)

                # Parse with existing CSV logic
                processed_data, selected_fibres = self.process_csv(input_file)

            else:
                # ---- VMR path ----
                vmr_id = (self.vmr_id_entry.get() or "").strip()
                if not re.fullmatch(r"\d+", vmr_id):
                    messagebox.showerror("Error", "Please enter a numeric VMR Job/WO ID.")
                    return

                # Crawl FibreTrace HTML (saved next to exe/py so double-click viewer can reuse)
                html_path = _vmr_crawl_fibretrace(vmr_id)
                with open(html_path, "r", encoding="utf-8", errors="ignore") as fh:
                    html_text = fh.read()

                # Fibre Type from Fibre Trace Summary → Name (prefix L_/J_/T_)
                auto_ft, _path_name = _derive_ft_from_summary(html_text)
                if auto_ft:
                    self.fibre_type.set(auto_ft)

                # Convert the Details table HTML → CSV-like (EO strict 'EOxxxxxx', Length 'xx.xx')
                tmp_csv = _vmr_html_to_csv_like_tempfile(html_text)

                # Reuse existing CSV parser end-to-end
                processed_data, selected_fibres = self.process_csv(tmp_csv)

            # ===== 1) Majority parity (unchanged) =====
            even = sum(1 for n in selected_fibres if isinstance(n, int) and n > 0 and n % 2 == 0)
            odd  = sum(1 for n in selected_fibres if isinstance(n, int) and n > 0 and n % 2 == 1)
            majority_parity = "even" if even > odd else ("odd" if odd > even else None)

            # ===== 2) Clear UI table =====
            for iid in self.tree.get_children():
                self.tree.delete(iid)
            self.row_meta = {}

            # Reset cross-section cache
            try:
                self.cs_cache.clear()
                self.cs_cache = CrossSectionCache()
            except Exception:
                pass

            # ===== 3) Build the crawl list ONLY for rows that display a Fibre Tray =====
            segid_cache = {}
            seg_ids_needed = set()
            seg_by_row_index = {}
            tray_by_row_index = {}
            to_crawl = []

            # figure out column positions once
            pd_headers = [h.strip() for h in processed_data[0]] if processed_data and processed_data[0] else []
            # make sure the UI has a "Fibre Tray" column
            try:
                tray_col = pd_headers.index("Fibre Tray")
            except ValueError:
                tray_col = len(pd_headers)
                pd_headers.append("Fibre Tray")
                processed_data[0] = pd_headers
                # make every row at least as wide as the header and add an empty tray cell
                for k in range(1, len(processed_data)):
                    processed_data[k] = _ensure_width(processed_data[k], len(pd_headers))
                    processed_data[k][tray_col] = ""


            # After pd_headers and tray_col are defined
            def _ensure_width(row, target_len):
                # pad the row so it has at least target_len cells
                if row is None:
                    return [""] * target_len
                if len(row) < target_len:
                    row += [""] * (target_len - len(row))
                return row


            # other columns we need
            try:
                name_col = pd_headers.index("Fibre Cable")
            except ValueError:
                name_col = None

            try:
                cd_col = pd_headers.index("Connect/Disconnect")
            except ValueError:
                cd_col = None

            # helper: tray range from the "(#n)" selector in the cable name
            def _tray_from_cable_name(cable_name: str) -> str:
                m = re.search(r'\(#\s*(\d+)\s*\)', str(cable_name or ""))
                if not m:
                    return ""
                sel = int(m.group(1))
                start = ((sel - 1) // 6) * 6 + 1
                end = start + 5
                return f"{start}-{end}"

            show_next = False
            last_tray = ""

            for i in range(1, len(processed_data)):
                row = processed_data[i]
                # make sure row is long enough so we can safely set row[tray_col]
                row = _ensure_width(row, len(pd_headers))
                processed_data[i] = row  # keep the padded row

                curr_cd = (row[cd_col] if cd_col is not None and cd_col < len(row) else "")
                curr_cd = (curr_cd or "").strip()

                if curr_cd:
                    cable_name = row[name_col] if (name_col is not None and len(row) > name_col) else ""
                    tray_here = _tray_from_cable_name(cable_name)  # your helper that maps "(#n)" → "1-6", etc.
                    row[tray_col] = tray_here
                    last_tray = tray_here
                    show_next = True
                elif show_next:
                    row[tray_col] = last_tray
                    show_next = False
                else:
                    row[tray_col] = ""



            # STRICT: only treat tray as present if the UI column "Fibre Tray" has a non-empty value
            def _tray_from_row(row):
                row = _ensure_width(row, len(pd_headers))
                if tray_col is not None and row and len(row) > tray_col:
                    t = (row[tray_col] or "").strip()
                    if t:
                        return t
                return ""

            def _segid_for_cable(cable_name):
                key = (cable_name or "").strip()
                if "(" in key:
                    key = key.split("(")[0].strip()
                if key in segid_cache:
                    return segid_cache[key]
                try:
                    conn_tmp = sqlite3.connect(self.db_path)
                    cur = conn_tmp.cursor()
                    cd = self.fetch_cable_data(cur, key)
                    conn_tmp.close()
                except Exception:
                    cd = None
                segid = (cd or {}).get("SEGMENT_ID", "") if cd else ""
                segid_cache[key] = segid
                return segid

            # walk rows and collect ONLY those with a non-empty tray
            for i in range(1, len(processed_data)):
                row = processed_data[i]
                if name_col is None or not row or len(row) <= name_col:
                    continue

                fibre_tray = _tray_from_row(row)
                if not fibre_tray:
                    continue  # Skip rows without visible Fibre Tray

                cable_name = row[name_col]
                seg_id = _segid_for_cable(cable_name)
                if not seg_id:
                    continue

                seg_by_row_index[i] = seg_id
                tray_by_row_index[i] = fibre_tray

                if seg_id not in seg_ids_needed:
                    seg_ids_needed.add(seg_id)
                    to_crawl.append((seg_id, VMR_Cable_URL + seg_id))




            # ===== 4) Optional cross-section crawling (Connect VMR) =====
            if self.crawl_enabled.get() and to_crawl:
                self.progress["maximum"] = len(to_crawl)
                self.progress["value"] = 0
                self.progress_label.configure(text="Crawling Cross Sections…")
                self.progress_frame.grid(row=4, column=0, sticky="w", padx=6, pady=(4, 2))
                self.parent_frame.update_idletasks()

                for idx, (seg_id, url) in enumerate(to_crawl, start=1):
                    try:
                        resp = requests.get(
                            url,
                            headers={"User-Agent": "Mozilla/5.0 (FibreAssist/1.0)"},
                            timeout=30,
                            verify=True
                        )
                        resp.raise_for_status()
                        headers, rows = self.cs_cache.put_html(seg_id, resp.text)

                        # precompute alert flags per tray for fast UI labeling
                        for row_idx, _seg in seg_by_row_index.items():
                            if _seg != seg_id:
                                continue
                            tray = (tray_by_row_index.get(row_idx, "") or "").strip()
                            if not tray:
                                continue
                            subset = filter_rows_by_tray_range(rows, tray)
                            flag = rows_have_alert(headers, subset)
                            self.cs_cache.set_tray_alert(seg_id, tray, flag)


                    except Exception:
                        # swallow crawl errors per-segment; continue UI update
                        pass
                    finally:
                        self.progress["value"] = idx
                        self.parent_frame.update_idletasks()

                self.progress_frame.grid_remove()

            # ===== 5) Populate UI table + commentary & tags =====
            columns = self.tree["columns"]
            try:
                commentary_idx = columns.index("Commentary")
            except ValueError:
                commentary_idx = len(columns) - 1

            # NEW: indexes for our added columns
            try:
                iof_idx = columns.index("IOF")
            except ValueError:
                iof_idx = None
            try:
                dwdm_idx = columns.index("DWDM/T_ found")
            except ValueError:
                dwdm_idx = None

            conn = None
            cursor = None
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
            except Exception:
                conn = None
                cursor = None

            for i in range(1, len(processed_data)):
                row = processed_data[i]

                # Build the row for display (your code may already do this)
                # === REPLACE START: build display row by header name (no hard indices) ===
                pd_headers = processed_data[0] if processed_data and processed_data[0] else []

                def _idx(name, default=None):
                    try:
                        return pd_headers.index(name)
                    except ValueError:
                        return default

                def _val(r, i):
                    if i is None:
                        return ""
                    return r[i] if (0 <= i < len(r)) else ""

                a_end_idx      = _idx("A-End", 1)
                fibre_idx      = _idx("Fibre Cable", 2)
                b_end_idx      = _idx("B-End", 3)
                cd_idx         = _idx("Connect/Disconnect", 4)
                eo_idx         = _idx("EO", 5)
                length_idx     = _idx("Length", 6)
                tube_idx       = _idx("Tube", 7)
                rs_type_idx_pd = _idx("RS Type")              # may be None if header not present
                iof_idx_pd     = _idx("IOF")                  # may be None if header not present
                dwdm_idx_pd    = _idx("DWDM/T_ found")        # may be None if header not present
                tray_idx_pd    = _idx("Fibre Tray")           # may be None if header not present

                display_row = [
                    _val(row, a_end_idx),          # A-End
                    _val(row, fibre_idx),          # Fibre Cable
                    _val(row, b_end_idx),          # B-End
                    _val(row, cd_idx),             # Connect/Disconnect
                    _val(row, eo_idx),             # EO
                    _val(row, length_idx),         # Length
                    _val(row, tube_idx),           # Tube
                    _val(row, rs_type_idx_pd),     # RS Type (placeholder; will be filled if C/D present)
                    _val(row, iof_idx_pd),         # IOF (placeholder; will be filled later)
                    _val(row, dwdm_idx_pd),        # DWDM/T_ found (placeholder; will be filled later)
                    _val(row, tray_idx_pd),        # Fibre Tray
                    ""                             # Commentary
                ]
                item_id = self.tree.insert("", "end", values=display_row)

                # Stash any per-row metadata
                seg_id = seg_by_row_index.get(i, "")
                self.row_meta[item_id] = {"segment_id": seg_id}

                # Indices for writing back into the Treeview (UI) later in this loop
                columns = self.tree["columns"]
                try:
                    commentary_idx = columns.index("Commentary")
                except ValueError:
                    commentary_idx = len(columns) - 1
                try:
                    iof_idx = columns.index("IOF")
                except ValueError:
                    iof_idx = None
                try:
                    dwdm_idx = columns.index("DWDM/T_ found")
                except ValueError:
                    dwdm_idx = None
                try:
                    rs_type_ui_idx = columns.index("RS Type")
                except ValueError:
                    rs_type_ui_idx = None

                # Normalized values for subsequent logic
                fibre_cable  = _val(row, fibre_idx)
                a_end        = _val(row, a_end_idx)
                b_end        = _val(row, b_end_idx)
                connect_disc = _val(row, cd_idx)
                tube_value   = _val(row, tube_idx)
                # === REPLACE END ===


                tags = []

                # === DWDM/T_ label based on cached cross-section alert ===
                # Look up Fibre Tray by header name (resilient to column order)
                pd_headers = processed_data[0] if processed_data and processed_data[0] else []
                try:
                    tray_col_pd = pd_headers.index("Fibre Tray")
                except ValueError:
                    tray_col_pd = len(row) - 2  # safe fallback

                # Robust tray string (prefer mapped tray, else from row by header name)
                try:
                    tray_col_pd = pd_headers.index("Fibre Tray")
                except ValueError:
                    tray_col_pd = tray_idx_pd  # may be None; safe with _val()
                tray_str = tray_by_row_index.get(i, "") or _val(row, tray_col_pd)

                # ---- Enforce DISPLAY rule for Fibre Tray column (forward-peek) ----
                # If current row has non-empty Connect/Disconnect, show tray for current + next row.
                def _get_cd_from_src_row(r):
                    try:
                        cd_idx = pd_headers.index("Connect/Disconnect")
                    except ValueError:
                        cd_idx = 4  # fallback
                    return (str(_val(r, cd_idx) or "").strip())

                curr_cd = _get_cd_from_src_row(row)

                # Decide whether to show for this row
                allow_tray = bool(curr_cd) or bool(self.show_next_tray)
                display_tray = tray_str if allow_tray else ""

                # Update the UI cell
                try:
                    tray_ui_idx = columns.index("Fibre Tray")
                except ValueError:
                    tray_ui_idx = None
                if tray_ui_idx is not None:
                    current_vals = list(self.tree.item(item_id, "values"))
                    while len(current_vals) <= tray_ui_idx:
                        current_vals.append("")
                    current_vals[tray_ui_idx] = display_tray
                    self.tree.item(item_id, values=tuple(current_vals))

                # Arm "show next" flag if current has C/D
                self.show_next_tray = bool(curr_cd)


                # >>> NEW: actually put display_tray into the UI cell <<<
                try:
                    tray_ui_idx = columns.index("Fibre Tray")
                except ValueError:
                    tray_ui_idx = None
                if tray_ui_idx is not None:
                    current_vals = list(self.tree.item(item_id, "values"))
                    while len(current_vals) <= tray_ui_idx:
                        current_vals.append("")
                    current_vals[tray_ui_idx] = display_tray
                    self.tree.item(item_id, values=tuple(current_vals))

                has_trunk = False
                if seg_id and tray_str and self.cs_cache:
                    try:
                        has_trunk = bool(self.cs_cache.tray_has_alert(seg_id, tray_str))
                    except Exception:
                        has_trunk = False

                # Write "Y" or "" to the "DWDM/T_ found" column of the UI row
                if dwdm_idx is not None:
                    current_vals = list(self.tree.item(item_id, "values"))
                    while len(current_vals) <= dwdm_idx:
                        current_vals.append("")
                    current_vals[dwdm_idx] = "Y" if has_trunk else ""
                    self.tree.item(item_id, values=tuple(current_vals))
                else:
                    # Fallback for older builds that still rely on tags
                    if has_trunk:
                        tags.append("cs_alert")

                # If DWDM/T_ is found, also append the exact warning to Commentary
                if has_trunk:
                    current_vals = list(self.tree.item(item_id, "values"))
                    note = "DWDM/Trunk Circuits found, DO NO USE, if can't avoid, ask permission from IPNE Fibre Planning Dropbox."
                    existing_comm = (current_vals[commentary_idx] if commentary_idx < len(current_vals) else "").strip()
                    new_comm = f"{existing_comm} | {note}" if existing_comm else note
                    while len(current_vals) <= commentary_idx:
                        current_vals.append("")
                    current_vals[commentary_idx] = new_comm
                    self.tree.item(item_id, values=tuple(current_vals))

                # (We will fill IOF after DB lookup below)



                # Commentary from DB lookups (unchanged)
                if cursor:
                    commentary_parts = []
                    cable_num   = row[0]
                    a_end       = row[1]
                    fibre_cable = row[2].split("(")[0] if "(" in row[2] else row[2]
                    b_end       = row[3]
                    connect_disc= row[4]

                    # Cable-level commentary
                    try:
                        cable_data = self.fetch_cable_data(cursor, fibre_cable)
                    except Exception:
                        cable_data = None

                    if cable_data is not None:
                        name_upper     = (cable_data.get('NAME') or "").upper()
                        status         = (cable_data.get('CABLE_STATUS') or "")
                        owner          = (cable_data.get('OWNER') or "")
                        iof            = (cable_data.get('IOF') or "")
                        construct_type = (cable_data.get('CONSTRUCT_TYPE') or "")

                        if ("ZLS" in name_upper) or (status == "PD"):
                            commentary_parts.append("Cable is being decommissioned, DO NO USE")
                        if status == "DF":
                            commentary_parts.append("Cable is Defective, DO NOT USE")
                        if status == "PA":
                            commentary_parts.append("Cable is New Build, try to avoid or add FAD3 of its EO to FAD3 of your EO.")
                        if owner and owner.upper() != "OPTUS":
                            commentary_parts.append("Cable is not owned by Optus")
                        # NEW IOF rule:
                        # • DB IOF == "Y" → IOF
                        # • OR name contains _AP/_MA/_SB/_SM → IOF
                        is_ioF_name = self._name_marks_iof(fibre_cable)
                        is_ioF_db   = (str(iof).strip().upper() == "Y")
                        is_iof      = is_ioF_db or is_ioF_name

                        # Write IOF label into the table column
                        if iof_idx is not None:
                            current_vals = list(self.tree.item(item_id, "values"))
                            while len(current_vals) <= iof_idx:
                                current_vals.append("")
                            current_vals[iof_idx] = "Y" if is_iof else ""
                            self.tree.item(item_id, values=tuple(current_vals))

                        if is_iof:
                            commentary_parts.append("Cable is IOF, ask for permission from Fibre Planning Team")
                        if (construct_type or "").upper() == "BU":
                            commentary_parts.append("Cable is Buried")
                        if (construct_type or "").upper() == "AR":
                            commentary_parts.append("Cable is built Aerial")
                        if (cable_data.get('NAME') or "").startswith("OF"):
                            commentary_parts.append("Cable is 'OF', DO NOT USE")

                    # Splice-case commentary if the row is a connect/disconnect
                    if (connect_disc or "").strip() != "":
                        b_end_for_search = b_end.rsplit("@", 1)[0] if "@" in (b_end or "") else (b_end or "")
                        try:
                            splice_data = self.fetch_splicecase_data(cursor, b_end_for_search.strip())
                        except Exception:
                            splice_data = None

                        if splice_data is None:
                            commentary_parts.append("Cannot splice at this Splice Case")
                        else:
                            if (splice_data.get('BUTTSPLICE') or "").upper() == "Y":
                                commentary_parts.append("Splice Case is Butt Spice")
                            rs_code = (splice_data.get('RS_CODE') or "").upper()
                            # === NEW: set "RS Type" column with rs_code when Connect/Disconnect is present ===
                            try:
                                rs_type_idx = self.tree["columns"].index("RS Type")
                            except ValueError:
                                rs_type_idx = None

                            if rs_type_idx is not None:
                                current_vals = list(self.tree.item(item_id, "values"))
                                while len(current_vals) <= rs_type_idx:
                                    current_vals.append("")
                                current_vals[rs_type_idx] = rs_code  # e.g., "", "RS-NO", "RS-RB", etc.
                                self.tree.item(item_id, values=tuple(current_vals))

                            restricted = (splice_data.get('RESTRICTED') or "").upper() == "Y"
                            if restricted and rs_code != "RS-NO":
                                commentary_parts.append(f"Splice Case is {rs_code}, ask fibre SME/field Ops for permission.")
                            elif rs_code == "RS-NO":
                                commentary_parts.append(f"Splice Case is {rs_code}, DO NOT SPLICE.")
                            elif rs_code == "RS-RB":
                                commentary_parts.append(f"Splice Case is {rs_code}, DO NOT USE fibres in ring-barked tubes.")
                            rs_comments_lower = (splice_data.get('RS_COMMENTS') or "").lower()
                            manhole_upper = (splice_data.get('MANHOLE') or "").upper()
                            if "substation" in rs_comments_lower:
                                commentary_parts.append("Splice Case is in substation, avoid as much as possible. If can't, ask fibre SME/field Ops for permission")
                            if ("citipower" in rs_comments_lower) or ("CP_" in manhole_upper):
                                commentary_parts.append("Splice Case is in citipower pit, avoid as much as possible. If can't, ask fibre SME/field Ops for permission")
                            if ("etsa" in rs_comments_lower) or ("ET_" in manhole_upper):
                                commentary_parts.append("Splice Case is in ETSA pit, DO NOT SPLICE.")
                            if "tunnel" in rs_comments_lower:
                                commentary_parts.append("Splice Case is in tunnel, DO NOT SPLICE")

                    # Fibre type advisory + CAN2000 mismatch highlight
                    selected_fibre_type = (self.fibre_type.get() or "").strip()
                    # tube = (row[7] or "").strip()
                    tube = (tube_value or "").strip()


                    # 1) Keep your existing advisory text
                    if selected_fibre_type == "Local" and tube == "Trunk":
                        commentary_parts.append("If no alternative local fibre, disconnect the Trunk ranges and connect to Local ranges. Leave a helix note about this change. Otherwise, ask fibre SME for approval and attach the approval email to helix note.")
                    elif selected_fibre_type == "Local" and tube == "Junction":
                        commentary_parts.append("If no alternative local fibre, ask fibre SME for approval and attach the approval email to helix note.")
                    elif selected_fibre_type == "Junction" and tube == "Trunk":
                        commentary_parts.append("If no alternative junction fibre, disconnect the Trunk ranges and connect to Junction ranges. Leave a helix note about this change.Otherwise, ask fibre SME for approval and attach the approval email to helix note.")
                    elif selected_fibre_type == "Trunk" and tube in ("Local", "Junction"):
                        commentary_parts.append("If no alternative trunk fibre, proceed with your design.")

                    # 2) Highlight ONLY when tube is a CAN2000 tube and mismatches selected type
                    #    (Local/Junction/Trunk are the CAN2000 tubes; anything else—including "Non-CAN2000"—is ignored)
                    can2000_tubes = {"Local", "Junction", "Trunk"}
                    if selected_fibre_type in can2000_tubes and tube in can2000_tubes and tube != selected_fibre_type:
                        # mark this row; your final styling already sets tube_mismatch = yellow
                        tags.append("tube_mismatch")


                    if commentary_parts:
                        joined = "; ".join(commentary_parts)
                        if joined and not joined.endswith(";"):
                            joined += ";"
                        current_vals = list(self.tree.item(item_id, "values"))
                        existing_comm = (current_vals[commentary_idx] if commentary_idx < len(current_vals) else "").strip()
                        merged_text = f"{existing_comm} {joined}".strip() if existing_comm else joined
                        while len(current_vals) <= commentary_idx:
                            current_vals.append("")
                        current_vals[commentary_idx] = merged_text
                        self.tree.item(item_id, values=tuple(current_vals))

                if tags:
                    self.tree.item(item_id, tags=tuple(tags))

            # ===== 6) Final styling =====
            self.tree.tag_configure("tube_mismatch", background="yellow")
            self.tree.tag_configure("parity_mismatch", background="lightblue")
            self.tree.tag_configure("cs_alert", background="salmon")
            self.adjust_column_widths()

            try:
                if conn:
                    conn.close()
            except Exception:
                pass

        except Exception as e:
            traceback.print_exc()
            messagebox.showerror("Error", f"An error occurred: {e}")


    def fetch_cable_data(self, cursor, cable_name):
        query = """
        SELECT NAME, CABLE_STATUS, OWNER, IOF, CONSTRUCT_TYPE, SEGMENT_ID
        FROM Cable
        WHERE UPPER(NAME) = UPPER(?)
        LIMIT 1
        """
        cursor.execute(query, (cable_name.strip(),))
        result = cursor.fetchone()
        if result:
            return {
                'NAME': result[0],
                'CABLE_STATUS': result[1] if result[1] else "",
                'OWNER': result[2] if result[2] else "",
                'IOF': result[3] if result[3] else "",
                'CONSTRUCT_TYPE': result[4] if result[4] else "",
                'SEGMENT_ID': result[5] if len(result) > 5 and result[5] else "",
            }
        return None


    def fetch_splicecase_data(self, cursor, splice_name):
        query = """
        SELECT NAME, BUTTSPLICE, RESTRICTED, RS_CODE, RS_COMMENTS, MANHOLE
        FROM SpliceCases
        WHERE UPPER(NAME) = UPPER(?)
        LIMIT 1
        """
        cursor.execute(query, (splice_name.strip(),))
        result = cursor.fetchone()
        if result:
            return {
                'NAME': result[0],
                'BUTTSPLICE': result[1] if result[1] else "",
                'RESTRICTED': result[2] if result[2] else "",
                'RS_CODE': result[3] if result[3] else "",
                'RS_COMMENTS': result[4] if result[4] else "",
                'MANHOLE': result[5] if result[5] else "",
            }
        return None

########################################################################
# Main script to bring both tools together
########################################################################

########################################################################
# Fibre Path Converter Tool
########################################################################

class FibrePathConverter:
    def __init__(self, parent):
        """
        Create the 'Fibre Path Converter' UI inside `parent`.
        """
        self.parent = parent
        self.parent_frame = ttk.Frame(parent, padding="10")
        self.parent_frame.pack(fill='both', expand=True)
        self.create_ui()

    def create_ui(self):
        # File selection row
        file_frame = ttk.Frame(self.parent_frame)
        file_frame.pack(fill='x', pady=5)
        ttk.Label(file_frame, text="Select Input CSV File:").grid(row=0, column=0, padx=5)
        self.input_entry = ttk.Entry(file_frame, width=50)
        self.input_entry.grid(row=0, column=1, padx=5)
        ttk.Button(file_frame, text="Browse", command=self.select_input_file).grid(row=0, column=2, padx=5)

        # Process button
        ttk.Button(self.parent_frame, text="Process", command=self.process).pack(pady=10)

        # Output text area
        self.text = tk.Text(self.parent_frame, height=10, wrap='word')
        self.text.pack(fill='both', expand=True, padx=5, pady=5)

        # Copy-to-clipboard button
        ttk.Button(self.parent_frame, text="Copy to Clipboard", command=self.copy_to_clipboard).pack(pady=(0,10))

        # Developer credit
        ttk.Label(self.parent_frame, text="developed by Jian", foreground="gray").pack(side="bottom", pady=5)

    def select_input_file(self):
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if file_path:
            self.input_entry.delete(0, tk.END)
            self.input_entry.insert(0, file_path)

    def process(self):
        input_file = self.input_entry.get()
        if not input_file:
            messagebox.showwarning("Warning", "Please select an input file.")
            return
        try:
            # Reuse existing CSV‐parsing logic
            processed_data, _ = FibreProcessor.process_csv(self, input_file)

            # Extract the 'Fibre Cable' column, dropping anything from "(" onward
            fibre_list = []
            for row in processed_data[1:]:
                cable = row[2] or ""
                cable = cable.split('(')[0].strip()
                fibre_list.append(cable)

            result = ",".join(fibre_list)

            # Display
            self.text.delete("1.0", tk.END)
            self.text.insert(tk.END, result)
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {e}")

    def copy_to_clipboard(self):
        text = self.text.get("1.0", tk.END).strip()
        if text:
            self.parent.clipboard_clear()
            self.parent.clipboard_append(text)
            self.parent.update()

def main():

    # --- add these lines ---
    def resource_path(relative_path):
        """Get absolute path to resource, works for dev and for PyInstaller bundle."""
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

    # ensure Windows taskbar shows the correct icon/group
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("com.fibre.assistance")

    root = tk.Tk()
    root.title("Fibre Assistance v1.10")

    # try to set the .ico; don’t crash if it’s missing
    try:
        root.iconbitmap(resource_path("icon.ico"))
    except Exception:
        pass

    notebook = ttk.Notebook(root)
    notebook.pack(fill='both', expand=True)

    # Fibre Database Update tab
    update_db_frame = ttk.Frame(notebook)
    notebook.add(update_db_frame, text="Fibre Database Update")
    FibreDatabaseUpdater(update_db_frame)

    # Fibre Check tab
    fibre_check_frame = ttk.Frame(notebook)
    notebook.add(fibre_check_frame, text="Fibre Check")
    FibreProcessor(fibre_check_frame)

    # Fibre Path Converter tab  <-- NEW!
    converter_frame = ttk.Frame(notebook)
    notebook.add(converter_frame, text="Fibre Path Converter")
    FibrePathConverter(converter_frame)

    root.mainloop()

if __name__ == "__main__":
    main()