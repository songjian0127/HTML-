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

def parse_gridview2(html_text):
    soup = BeautifulSoup(html_text, "lxml")
    grid2 = soup.find(id="GridView2")
    return _table_extract(grid2)

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

class CrossSectionCache:
    def __init__(self):
        self._dir = tempfile.TemporaryDirectory()
        self._index = {}  # seg_id -> {"path": ..., "headers": [...], "has_alert_by_tray": {tray_str: bool}}
        atexit.register(self.clear)

    def clear(self):
        try:
            self._index.clear()
            self._dir.cleanup()
        except Exception:
            pass

    def put_html(self, seg_id, html_text):
        path = f"{self._dir.name}/{seg_id}.html"
        with open(path, "w", encoding="utf-8") as f:
            f.write(html_text)
        headers, rows = parse_gridview2(html_text)
        self._index[seg_id] = {"path": path, "headers": headers, "rows_len": len(rows), "has_alert_by_tray": {}}
        return headers, rows

    def has(self, seg_id):
        return seg_id in self._index and os.path.exists(self._index[seg_id]["path"])

    def get_html(self, seg_id):
        meta = self._index.get(seg_id)
        if not meta: 
            return None
        with open(meta["path"], "r", encoding="utf-8") as f:
            return f.read()

    def headers_for(self, seg_id):
        meta = self._index.get(seg_id)
        return (meta or {}).get("headers", [])

    def set_tray_alert(self, seg_id, tray_str, flag):
        meta = self._index.get(seg_id)
        if meta is not None:
            meta["has_alert_by_tray"][tray_str] = bool(flag)

    def tray_has_alert(self, seg_id, tray_str):
        meta = self._index.get(seg_id)
        if not meta: 
            return False
        return bool(meta["has_alert_by_tray"].get(tray_str, False))

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
    soup = BeautifulSoup(html, "lxml")
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

    def create_ui(self):
        # File selection frame
        file_frame = ttk.Frame(self.parent_frame)
        file_frame.grid(row=0, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        ttk.Label(file_frame, text="Select Input CSV File:").grid(row=0, column=0, padx=5)
        self.input_entry = ttk.Entry(file_frame, width=50)
        self.input_entry.grid(row=0, column=1, padx=5)
        ttk.Button(file_frame, text="Browse", command=self.select_input_file).grid(row=0, column=2, padx=5)

        # Fibre type selection
        type_frame = ttk.Frame(self.parent_frame)
        type_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        ttk.Label(type_frame, text="Select Fibre Type:").grid(row=0, column=0, padx=5)
        fibre_types = ["Local", "Junction", "Trunk"]
        self.type_combo = ttk.Combobox(type_frame, textvariable=self.fibre_type, values=fibre_types, state="readonly")
        self.type_combo.grid(row=0, column=1, padx=5)

        # >>> NEW: Checkbox to enable/disable web crawling (default ON)
        self.crawl_enabled = tk.BooleanVar(value=True)
        self.crawl_check = ttk.Checkbutton(
            type_frame,
            text="Connect VMR",
            variable=self.crawl_enabled
        )
        self.crawl_check.grid(row=0, column=2, padx=10)

        # Process button
        ttk.Button(self.parent_frame, text="Process", command=self.process_data).grid(row=2, column=0, columnspan=3, pady=10)

        # Create Treeview for results
        self.create_treeview(self.parent_frame)
        self.tree.bind("<Motion>", lambda e: "break" if self.tree.identify_region(e.x, e.y) == "separator" else None)

        # Developer label
        ttk.Label(self.parent_frame, text="developed by Jian", foreground="gray").grid(row=4, column=0, columnspan=3, pady=(10, 0))

        # Configure grid weights
        self.parent_frame.columnconfigure(0, weight=1)
        self.parent_frame.rowconfigure(3, weight=1)

        # Setup copy functionality for the Treeview
        self.setup_copy_functionality()


    def create_treeview(self, parent):
        self.row_meta = {}  # item_id -> {"segment_id": "..."}
        tree_frame = ttk.Frame(parent)
        tree_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)

        # REMOVED "Cable#"
        columns = ("A-End", "Fibre Cable", "B-End", "Connect/Disconnect",
                "EO", "Length", "Tube", "Fibre Tray", "Commentary")
        self.tree = ttk.Treeview(tree_frame, columns=columns, show='headings')

        # Make scrollbars more visible: use classic Tk scrollbars with a larger width
        vsb = tk.Scrollbar(tree_frame, orient="vertical", command=self.tree.yview, width=18)
        hsb = tk.Scrollbar(tree_frame, orient="horizontal", command=self.tree.xview, width=18)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        vsb.grid(row=0, column=1, sticky=(tk.N, tk.S))
        hsb.grid(row=1, column=0, sticky=(tk.W, tk.E))

        # Headings + a small initial width (we’ll autosize after data insert)
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120, anchor='center')

        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        # Double-click behaviour
        self.tree.bind("<Double-1>", self.on_tree_double_click)

        # Highlight style for DWDM/T_
        self.tree.tag_configure("cs_alert", background="#fff3cd")  # light amber

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
        seg_id = meta.get("segment_id", "")

        if col_name == "Fibre Cable":
            if not seg_id:
                messagebox.showwarning("No SEGMENT_ID", "SEGMENT_ID not found for this cable.")
                return
            webbrowser.open_new(f"{VMR_Cable_URL}{seg_id}")
            return

        if col_name != "Fibre Tray":
            return

        # Use column name to get the correct index (layout-safe)
        try:
            tray_idx = columns.index("Fibre Tray")
        except ValueError:
            tray_idx = -1
        tray_range = (values[tray_idx] if tray_idx >= 0 and tray_idx < len(values) else "").strip()
        if not tray_range:
            messagebox.showinfo("No tray value", "No Fibre Tray range on this row.")
            return
        if not seg_id:
            messagebox.showwarning("No SEGMENT_ID", "SEGMENT_ID not found for this cable.")
            return

        # OPEN FROM CACHE ONLY (no new crawling)
        html_text = self.cs_cache.get_html(seg_id)
        if not html_text:
            messagebox.showerror("No Cached Data", "Cross section data not loaded yet. Click Process first.")
            return

        # Build viewer window from cached content
        try:
            headers, rows = parse_gridview2(html_text)
        except Exception as e:
            messagebox.showerror("Parse Error", str(e))
            return

        win = tk.Toplevel(self.root)
        win.title(f"Cross Section Details – {seg_id} [{tray_range}]")
        win.geometry("1100x700")

        top = ttk.Frame(win)
        top.pack(fill="x", padx=10, pady=8)
        ttk.Label(top, text=f"ID: {seg_id}   URL: {VMR_Cable_URL}{seg_id}").pack(side="left")

        table_frame = ttk.Frame(win)
        table_frame.pack(fill="both", expand=True, padx=10, pady=(0,10))
        tree = ttk.Treeview(table_frame, columns=tuple(headers or []), show="headings", height=24)
        # (already CHANGED previously): thicker Tk scrollbars to match Fibre Check
        vsb = tk.Scrollbar(table_frame, orient="vertical", command=tree.yview, width=18)
        hsb = tk.Scrollbar(table_frame, orient="horizontal", command=tree.xview, width=18)
        tree.configure(yscroll=vsb.set, xscroll=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)
        tree.tag_configure("alert", background="#fff3cd")

        for col in (headers or []):
            tree.heading(col, text=col)
            # initial width; we will auto-size after inserting rows
            tree.column(col, width=max(90, min(360, len(col)*10)), anchor="center", stretch=False)
        self.tree.bind("<Motion>", lambda e: "break" if self.tree.identify_region(e.x, e.y) == "separator" else None)

        subset = filter_rows_by_tray_range(rows, tray_range)
        colmap = {h.lower(): i for i, h in enumerate(headers or [])}
        idx_os = colmap.get("os name")
        idx_bearer = colmap.get("bearer id")

        for r in subset:
            os_name = (r[idx_os] if idx_os is not None and idx_os < len(r) else "").strip().upper()
            bearer = (r[idx_bearer] if idx_bearer is not None and idx_bearer < len(r) else "").strip().upper()
            alert = os_name.startswith("T_") or ("OTS" in bearer) or ("DWDM" in bearer)
            vals = r if len(r) == len(headers) else (r + [""]*(len(headers)-len(r)))[:len(headers)]
            tree.insert("", "end", values=vals, tags=("alert",) if alert else ())

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
        for i, row in enumerate(data):
            if any("Fibre Trace Details" in cell for cell in row):
                start_index = i + 1
                break

        if start_index is None:
            raise ValueError("'Fibre Trace Details' not found in the file.")

        data = data[start_index:]

        # ---------- UPDATED: add "Fibre Tray" to the headers (next to Tube) ----------
        headers = ["Cable#", "A-End", "Fibre Cable", "B-End", "Connect/Disconnect", "EO", "Length", "Tube", "Fibre Tray"]
        data[0] = headers

        processed_data = [headers]
        selected_fibres_list = []  # for parity checking

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

                # ---------- include Fibre Tray in output row ----------
                processed_data.append([
                    cable_num, a_end, fibre_cable, b_end, connect_disconnect, eo, length,
                    tube, display_tray
                ])
            else:
                i += 1

        return processed_data, selected_fibres_list

    def process_data(self):
        """
        Runs when the 'Process' button is clicked.

        Expects the input CSV path in self.input_entry.
        Builds the table with 'process_csv', computes majority parity, then
        performs the one-time crawl + tray alert/cache logic, and adds DB-backed
        commentary (Cable/Splice) + fibre-type vs tube guidance.
        """
        import sqlite3
        import requests

        # ----- Build processed_data + selected_fibres (from your existing CSV parser)
        input_file = (self.input_entry.get() or "").strip()
        if not input_file:
            messagebox.showerror("Error", "Please select an input CSV file first.")
            return

        try:
            processed_data, selected_fibres = self.process_csv(input_file)
        except Exception as e:
            messagebox.showerror("Error", f"Failed to build table: {e}")
            return

        # Compute majority parity from selected_fibres
        even = sum(1 for n in selected_fibres if isinstance(n, int) and n > 0 and n % 2 == 0)
        odd  = sum(1 for n in selected_fibres if isinstance(n, int) and n > 0 and n % 2 == 1)
        majority_parity = "even" if even > odd else ("odd" if odd > even else None)

        # Clear old rows from the view
        for iid in self.tree.get_children():
            self.tree.delete(iid)

        # Reset per-row metadata store for double-click actions
        self.row_meta = {}

        # Before inserting rows: wipe previous cache & show progress UI
        try:
            self.cs_cache.clear()
            self.cs_cache = CrossSectionCache()
        except Exception:
            pass

        # Collect crawl targets only for rows that have a Fibre Tray value
        to_crawl = []
        seg_by_row_index = {}   # i -> seg_id   (i is 1..n for processed_data rows, header is 0)
        tray_by_row_index = {}  # i -> "start-end"
        segid_cache = {}

        def _segid_for_cable(cable_name):
            key = (cable_name or "").strip()
            if "(" in key:
                key = key.split("(")[0].strip()
            if key in segid_cache:
                return segid_cache[key]
            try:
                conn_tmp = sqlite3.connect(self.db_path)
                cur = conn_tmp.cursor()
                cd = self.fetch_cable_data(cur, key)  # must return dict incl. SEGMENT_ID
                conn_tmp.close()
            except Exception:
                cd = None
            segid = (cd or {}).get("SEGMENT_ID", "") if cd else ""
            segid_cache[key] = segid
            return segid

        for i in range(1, len(processed_data)):  # skip header at 0
            row = processed_data[i]
            fibre_tray = (row[8] or "").strip()  # "Fibre Tray" in processed_data (with Cable# still present)
            if not fibre_tray:
                continue
            seg_id = _segid_for_cable(row[2])    # "Fibre Cable" in processed_data (index 2 with Cable# present)
            if not seg_id:
                continue
            seg_by_row_index[i] = seg_id
            tray_by_row_index[i] = fibre_tray
            if seg_id not in [x[0] for x in to_crawl]:
                to_crawl.append((seg_id, VMR_Cable_URL + seg_id))

        # Progress bar
        if self.crawl_enabled.get() and to_crawl:
            if to_crawl:
                self.progress["maximum"] = len(to_crawl)
                self.progress["value"] = 0
                self.progress_label.configure(text="Crawling Cross Sections…")
                self.progress_frame.grid(row=2, column=0, sticky="w", padx=6, pady=(4, 2))
                self.parent_frame.update_idletasks()

            # Crawl once per SEGMENT_ID
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

                    for row_idx, _seg in seg_by_row_index.items():
                        if _seg != seg_id:
                            continue
                        tray = tray_by_row_index.get(row_idx, "")
                        subset = filter_rows_by_tray_range(rows, tray)
                        flag = rows_have_alert(headers, subset)
                        self.cs_cache.set_tray_alert(seg_id, tray, flag)

                except Exception:
                    pass
                finally:
                    self.progress["value"] = idx
                    self.parent_frame.update_idletasks()

            if to_crawl:
                self.progress_frame.grid_remove()
        else:
            # Skip crawling; leave cache empty
            print("Web crawling disabled by user.")

        # Insert into Treeview (drop "Cable#" for display) and append commentary if needed
        columns = self.tree["columns"]
        try:
            commentary_idx = columns.index("Commentary")
        except ValueError:
            commentary_idx = len(columns) - 1  # fallback to last

        # Open a single DB connection for Cable/Splice commentary (from "old" logic)
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

            # UI shows no "Cable#": take row[1:] and add a blank Commentary slot
            display_row = row[1:] + [""]  # ["A-End", ..., "Fibre Tray", "Commentary"]

            item_id = self.tree.insert("", "end", values=display_row)

            # Save SEGMENT_ID for double-click behaviour
            seg_id = seg_by_row_index.get(i)
            if not seg_id:
                seg_id = _segid_for_cable(row[2])  # processed_data still has "Fibre Cable" at index 2
            self.row_meta[item_id] = {"segment_id": seg_id or ""}

            # Existing highlight logic (uses processed_data, not tree columns)
            tags = []
            tube_type = row[7]
            if (tube_type != "Non-CAN2000" and tube_type != self.fibre_type.get()):
                tags.append("tube_mismatch")

            fibre_number = selected_fibres[i - 1]  # rows are offset by header
            if majority_parity:
                if majority_parity == "even" and fibre_number % 2 != 0:
                    tags.append("parity_mismatch")
                elif majority_parity == "odd" and fibre_number % 2 == 0:
                    tags.append("parity_mismatch")

            # --- DWDM/T_ commentary based on cached tray subset (from "new")
            tray_str = (row[8] or "").strip()  # processed_data fibre tray (index 8 with Cable# present)
            if seg_id and tray_str and self.cs_cache.tray_has_alert(seg_id, tray_str):
                tags.append("cs_alert")
                current_vals = list(self.tree.item(item_id, "values"))
                note = "Fibre Tray has DWDM/Trunk Circuit, DO NOT USE, if can't avoid, ask for permission from Fibre Planning dropbox."
                existing_comm = (current_vals[commentary_idx] if commentary_idx < len(current_vals) else "").strip()
                new_comm = f"{existing_comm} | {note}" if existing_comm else note
                while len(current_vals) <= commentary_idx:
                    current_vals.append("")
                current_vals[commentary_idx] = new_comm
                self.tree.item(item_id, values=tuple(current_vals))

            # --- Cable/Splice commentary + fibre-type vs tube guidance (restored from "old")
            if cursor:
                commentary_parts = []

                cable_num   = row[0]
                a_end       = row[1]
                fibre_cable = row[2].split("(")[0] if "(" in row[2] else row[2]
                b_end       = row[3]
                connect_disc= row[4]

                # Cable data commentary
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
                    # if status == "PV":
                    #     commentary_parts.append("Cable is 'Pending Verified', please check if the build EO is completed")
                    if status == "DF":
                        commentary_parts.append("Cable is Defective, DO NOT USE")
                    if status == "PA":
                        commentary_parts.append("Cable is New Build, try to avoid or add FAD3 of its EO to FAD3 of your EO.")
                    if owner and owner.upper() != "OPTUS":
                        commentary_parts.append("Cable is not owned by Optus")
                    if iof and iof.upper() == "Y":
                        commentary_parts.append("Cable is IOF, ask for permission from Fibre Planning Team")
                    if (construct_type or "").upper() == "BU":
                        commentary_parts.append("Cable is Buried")
                    if (construct_type or "").upper() == "AR":
                        commentary_parts.append("Cable is built Aerial")
                    if (cable_data.get('NAME') or "").startswith("OF"):
                        commentary_parts.append("Cable is 'OF', DO NOT USE")

                # SpliceCase commentary (only if Connect/Disconnect not blank)
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

                # Selected fibre type vs tube guidance
                selected_fibre_type = self.fibre_type.get()
                tube = row[7]
                if selected_fibre_type == "Local" and tube == "Trunk":
                    commentary_parts.append("If no alternative local fibre, disconnect the Trunk ranges and connect to Local ranges. Leave a helix note about this change. Otherwise, ask fibre SME for approval and attach the approval email to helix note.")
                elif selected_fibre_type == "Local" and tube == "Junction":
                    commentary_parts.append("If no alternative local fibre, ask fibre SME for approval and attach the approval email to helix note.")
                elif selected_fibre_type == "Junction" and tube == "Trunk":
                    commentary_parts.append("If no alternative junction fibre, disconnect the Trunk ranges and connect to Junction ranges. Leave a helix note about this change.Otherwise, ask fibre SME for approval and attach the approval email to helix note.")
                elif selected_fibre_type == "Trunk" and tube in ("Local", "Junction"):
                    commentary_parts.append("If no alternative trunk fibre, proceed with your design.")

                if commentary_parts:
                    joined = "; ".join(commentary_parts)
                    if joined and not joined.endswith(";"):
                        joined += ";"

                    # Append to any existing commentary (e.g., cs_alert)
                    current_vals = list(self.tree.item(item_id, "values"))
                    existing_comm = (current_vals[commentary_idx] if commentary_idx < len(current_vals) else "").strip()
                    merged = f"{existing_comm} {joined}".strip() if existing_comm else joined
                    while len(current_vals) <= commentary_idx:
                        current_vals.append("")
                    current_vals[commentary_idx] = merged
                    self.tree.item(item_id, values=tuple(current_vals))

            if tags:
                self.tree.item(item_id, tags=tuple(tags))

        # Configure tag styles and auto-fit columns
        self.tree.tag_configure("tube_mismatch", background="yellow")
        self.tree.tag_configure("parity_mismatch", background="lightblue")
        self.tree.tag_configure("cs_alert", background="salmon")

        # Auto-fit columns to smallest size that fits content
        self.adjust_column_widths()

        # Close DB connection if opened
        try:
            if conn:
                conn.close()
        except Exception:
            pass

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
    root.title("Fibre Assistance v1.9")

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