#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VMR Cable Analyzer - Modified V7
Updates:
1. Robust CSV Reading: Handles both UTF-8 and Windows-1252 encodings to fix '0xa0' errors.
2. Logic: Retains V6 logic (Columns, Cleaning Ends, Spare Count, BSS/Backbone rules).
"""

import argparse
import csv
import re
import sys
import time
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---- Configuration ----------------------------------------------------------

BASE_URL = "https://cadprdwebw001.optus.com.au/vmr"

@dataclass
class CableData:
    name: str
    vmr_id: Optional[str] = None
    url: str = ""
    length: str = ""
    summary_spares: str = ""  # Extracted from 'xxxSP' in summary table
    a_end: str = ""
    z_end: str = ""
    cable_type: str = "Unknown"
    total_fibres: int = 0
    trunk_spares: int = 0
    junction_spares: int = 0
    status: str = "Processed"

# ---- Connection Helpers -----------------------------------------------------

def make_session(timeout=30, total_retries=3, backoff=0.5) -> requests.Session:
    sess = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=["GET", "HEAD", "OPTIONS"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)

    sess.headers.update({
        "User-Agent": "VMR-Cable-Analyzer/7.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    sess.request_timeout = timeout
    return sess

def get_html(session: requests.Session, url: str, params=None) -> str:
    try:
        r = session.get(url, params=params, timeout=session.request_timeout)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        print(f"  [!] HTTP Request failed: {e}")
        return ""

# ---- Parsing Logic ----------------------------------------------------------

def find_cable_id_exact_match(html_content: str, cable_name: str) -> Optional[str]:
    """
    Parses the search result HTML to find the exact cable name and extract its ID.
    """
    soup = BeautifulSoup(html_content, "html.parser")
    links = soup.find_all("a", href=True)
    target_clean = cable_name.strip().upper()
    
    for link in links:
        link_text = link.get_text(strip=True).upper()
        href = link['href']
        
        # Exact match check
        if link_text == target_clean:
            # Pattern: javascript:data_control.setCable('THE_ID')
            match = re.search(r"data_control\.setCable\(\s*'([^']+)'\s*\)", href)
            if match:
                return match.group(1)
    return None

def clean_end_text(text: str) -> str:
    """
    Cleans the A/B End text by removing everything after '@' or '#' 
    and stripping whitespace.
    """
    if not text:
        return ""
    # Split by '@', take first part
    text = text.split('@')[0]
    # Split by '#', take first part
    text = text.split('#')[0]
    return text.strip()

def parse_summary_table(soup: BeautifulSoup, cable_data: CableData) -> str:
    """
    Parses GridView1 to extract A End, B End, Length, Spare Count (xxxSP), and Type.
    """
    table = soup.find("table", id="GridView1")
    if not table:
        return "Error: No Summary Table"

    rows = table.find_all("tr")
    if len(rows) < 2:
        return "Error: Empty Summary Table"

    cols = rows[1].find_all("td")
    if len(cols) < 3:
        return "Error: Malformed Summary Table"

    # 1. Extract Ends and Clean them
    raw_a = cols[0].get_text(" ", strip=True)
    raw_z = cols[2].get_text(" ", strip=True)
    
    cable_data.a_end = clean_end_text(raw_a)
    cable_data.z_end = clean_end_text(raw_z)

    # 2. Parse Middle Column (Name, Length, Spares)
    col_1_text = cols[1].get_text(" ", strip=True)
    
    # Extract Length (e.g., "703.00m")
    len_match = re.search(r"([\d,]+\.?\d*m)", col_1_text, re.IGNORECASE)
    if len_match:
        cable_data.length = len_match.group(1)
    else:
        cable_data.length = "Unknown"

    # Extract "Spare Fibre" from text like "15SP" or "134SP"
    sp_match = re.search(r"(\d+)SP", col_1_text, re.IGNORECASE)
    if sp_match:
        cable_data.summary_spares = sp_match.group(1)
    else:
        cable_data.summary_spares = "0"

    # 3. Determine Cable Type
    a_upper = cable_data.a_end.upper()
    z_upper = cable_data.z_end.upper()
    name_upper = cable_data.name.upper()

    has_bjl_a = "BJL" in a_upper
    has_bjl_z = "BJL" in z_upper
    has_ajl_a = "AJL" in a_upper
    has_ajl_z = "AJL" in z_upper
    has_bss_name = "BSS" in name_upper

    # Rule: Both contain BJL -> Backbone
    if has_bjl_a and has_bjl_z:
        return "CAN2000 Backbone"

    # Rule: "BSS" in name AND One End BJL AND Other End NOT (BJL or AJL) -> Backbone
    if has_bss_name:
        if has_bjl_a and not has_bjl_z and not has_ajl_z:
            return "CAN2000 Backbone"
        if has_bjl_z and not has_bjl_a and not has_ajl_a:
            return "CAN2000 Backbone"
    
    # Rule: If not backbone, check if either contains AJL -> Access
    if has_ajl_a or has_ajl_z:
        return "CAN2000 Access"

    return "Non-CAN2000"

def parse_fibres_and_tubes(soup: BeautifulSoup) -> List[List[dict]]:
    """
    Parses GridView2. Returns grouped tubes based on buffer color.
    """
    table = soup.find("table", id="GridView2")
    if not table:
        return []

    rows = table.find_all("tr")
    data_rows = rows[1:] # Skip header
    
    all_fibres = []
    
    for row in data_rows:
        cols = row.find_all("td")
        if len(cols) < 12:
            continue
        try:
            seq_text = cols[1].get_text(strip=True)
            seq = int(seq_text) if seq_text.isdigit() else 0
            buffer_color = cols[3].get_text(strip=True)
            st_status = cols[7].get_text(strip=True)
            
            all_fibres.append({
                "seq": seq,
                "buffer": buffer_color,
                "st": st_status
            })
        except (ValueError, IndexError):
            continue

    # Group by Buffer color
    tubes = []
    if not all_fibres:
        return tubes

    current_tube = []
    last_buffer = None

    for fibre in all_fibres:
        if last_buffer is None:
            current_tube.append(fibre)
            last_buffer = fibre['buffer']
        elif fibre['buffer'] == last_buffer:
            current_tube.append(fibre)
        else:
            tubes.append(current_tube)
            current_tube = [fibre]
            last_buffer = fibre['buffer']
    
    if current_tube:
        tubes.append(current_tube)

    return tubes

def is_spare(fibre: dict) -> bool:
    # Rule: Status is "SP" or empty
    val = fibre['st'].strip().upper()
    return val == "SP" or val == ""

def calculate_stats(cable_data: CableData, tubes: List[List[dict]]):
    """
    Applies Logic to count spares in Trunk vs Junction ranges.
    """
    all_fibres_flat = [f for tube in tubes for f in tube]
    cable_data.total_fibres = len(all_fibres_flat)

    if "Non-CAN2000" in cable_data.cable_type or "Error" in cable_data.cable_type:
        return 

    trunk_range_fibres = []
    junction_range_fibres = []
    
    num_tubes = len(tubes)

    if cable_data.cable_type == "CAN2000 Backbone":
        # Backbone Rule: 
        # First 2 tubes = Trunk
        # Last 2 tubes = Local (Ignored)
        # Middle = Junction
        
        if num_tubes >= 1:
            trunk_tubes = tubes[:2] # Up to first 2
            for tube in trunk_tubes:
                trunk_range_fibres.extend(tube)
        
        if num_tubes > 4:
            junction_tubes = tubes[2:-2]
            for tube in junction_tubes:
                junction_range_fibres.extend(tube)

    elif cable_data.cable_type == "CAN2000 Access":
        # Access Rule:
        
        if cable_data.total_fibres == 144:
            # Special Case 144: 
            # Local: 49-72 and 121-144 (Ignored)
            # Junction: Remaining
            for f in all_fibres_flat:
                seq = f['seq']
                is_local = (49 <= seq <= 72) or (121 <= seq <= 144)
                if not is_local:
                    junction_range_fibres.append(f)
        else:
            # Standard Access Rule:
            # Last 2 tubes = Local (Ignored)
            # Remaining (Beginning) = Junction
            if num_tubes > 2:
                junction_tubes = tubes[:-2]
                for tube in junction_tubes:
                    junction_range_fibres.extend(tube)

    # Count Spares
    cable_data.trunk_spares = sum(1 for f in trunk_range_fibres if is_spare(f))
    cable_data.junction_spares = sum(1 for f in junction_range_fibres if is_spare(f))

# ---- Main Process -----------------------------------------------------------

def process_cable(session: requests.Session, cable_name: str) -> CableData:
    data = CableData(name=cable_name)
    print(f"Processing: {cable_name}...", end=" ", flush=True)

    # Step 1: Search
    search_url = f"{BASE_URL}/Result.aspx"
    search_keyword = f"10|{cable_name}"
    html_search = get_html(session, search_url, params={"keywords": search_keyword})

    if not html_search:
        data.status = "Search Failed"
        print("Error (Search).")
        return data

    # Step 2: Extract ID (Strict Match)
    data.vmr_id = find_cable_id_exact_match(html_search, cable_name)
    
    if not data.vmr_id:
        data.status = "Not Found"
        print("Not Found (Exact Match).")
        return data

    # Build direct URL
    data.url = f"{BASE_URL}/CrossSectionReview.aspx?id={data.vmr_id}"

    # Step 3: Get Details
    html_details = get_html(session, data.url)
    if not html_details:
        data.status = "Details Failed"
        print("Error (Details).")
        return data

    soup = BeautifulSoup(html_details, "html.parser")

    # Step 4: Parse Summary Table
    data.cable_type = parse_summary_table(soup, data)

    # Step 5: Parse Fibres & Stats
    if "Error" not in data.cable_type:
        tubes = parse_fibres_and_tubes(soup)
        calculate_stats(data, tubes)
    
    print(f"Done ({data.cable_type}, Length: {data.length}, Spares: {data.summary_spares}).")
    return data

def read_input_file(path: Path) -> List[str]:
    """
    Reads a CSV file with robust encoding handling.
    Tries utf-8-sig first, then cp1252 (Excel default on Windows).
    """
    cables = []
    encodings = ['utf-8-sig', 'cp1252', 'latin1']
    
    for enc in encodings:
        try:
            with open(path, mode='r', encoding=enc) as f:
                reader = csv.reader(f)
                header = next(reader, None)
                
                # Logic to handle header row vs data row
                if header:
                    first_cell = header[0].strip()
                    # Heuristic: if it looks like a header "Cable", skip. 
                    # If it looks like data (has digits like 22BSS...), treat as data
                    if "Cable" in first_cell or "Name" in first_cell:
                        pass
                    else:
                        cables.append(first_cell)
                    
                    for row in reader:
                        if row:
                            cables.append(row[0].strip())
            return cables # Success
        except UnicodeDecodeError:
            continue # Try next encoding
        except Exception as e:
            print(f"Error reading CSV: {e}")
            sys.exit(1)
            
    print("Error: Could not decode input file with supported encodings.")
    sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="VMR Cable Analyzer V7")
    parser.add_argument("input_csv", help="Path to input CSV file containing cable names")
    parser.add_argument("-o", "--out", default="output.csv", help="Output CSV path")
    args = parser.parse_args()

    input_path = Path(args.input_csv)
    if not input_path.exists():
        print(f"Input file not found: {input_path}")
        sys.exit(1)

    # Read Cables
    cables_to_process = read_input_file(input_path)
    print(f"Found {len(cables_to_process)} cables to process.")

    session = make_session()
    results = []

    for cable in cables_to_process:
        result = process_cable(session, cable)
        results.append(result)
        time.sleep(0.1) # Polite delay

    # Write Output CSV with specific headers and order
    headers = [
        "Cable Name",
        "VMR link",
        "Cable length",
        "Fibre cable size",
        "Spare Fibre",
        "trunk_spares",
        "junction_spares",
        "Splice or site A end",
        "Splice or site B end"
    ]

    try:
        with open(args.out, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            
            for r in results:
                writer.writerow([
                    r.name,
                    r.url,
                    r.length,
                    r.total_fibres,
                    r.summary_spares,
                    r.trunk_spares,
                    r.junction_spares,
                    r.a_end,
                    r.z_end
                ])
        print(f"\nReport saved to: {args.out}")
    except Exception as e:
        print(f"Error writing output: {e}")

if __name__ == "__main__":
    main()