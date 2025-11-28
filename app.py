import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import io
import time
from dataclasses import dataclass, asdict
from typing import List, Optional

# ==========================================
# 1. CORE LOGIC (Ported from VMR Script)
# ==========================================

BASE_URL = "https://cadprdwebw001.optus.com.au/vmr"

@dataclass
class CableData:
    name: str
    vmr_id: Optional[str] = None
    url: str = ""
    length: str = ""
    summary_spares: str = ""
    a_end: str = ""
    z_end: str = ""
    cable_type: str = "Unknown"
    total_fibres: int = 0
    trunk_spares: int = 0
    junction_spares: int = 0
    status: str = "Processed"

def make_session(timeout=30):
    sess = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update({"User-Agent": "VMR-Web-Crawler/1.0"})
    return sess

def get_html(session, url, params=None):
    try:
        r = session.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.text
    except Exception:
        return ""

def clean_end_text(text):
    if not text: return ""
    text = text.split('@')[0].split('#')[0]
    return text.strip()

def parse_cable_logic(session, cable_name):
    # This encapsulates the specific logic from your original script
    data = CableData(name=cable_name)
    
    # 1. Search
    search_html = get_html(session, f"{BASE_URL}/Result.aspx", params={"keywords": f"10|{cable_name}"})
    if not search_html:
        data.status = "Search Failed"
        return data

    # 2. Extract ID
    soup = BeautifulSoup(search_html, "html.parser")
    links = soup.find_all("a", href=True)
    target_clean = cable_name.strip().upper()
    
    for link in links:
        if link.get_text(strip=True).upper() == target_clean:
            match = re.search(r"data_control\.setCable\(\s*'([^']+)'\s*\)", link['href'])
            if match:
                data.vmr_id = match.group(1)
                break
    
    if not data.vmr_id:
        data.status = "Not Found"
        return data

    # 3. Details
    data.url = f"{BASE_URL}/CrossSectionReview.aspx?id={data.vmr_id}"
    details_html = get_html(session, data.url)
    if not details_html:
        data.status = "Details Error"
        return data

    soup_details = BeautifulSoup(details_html, "html.parser")

    # 4. Summary Table Logic
    table = soup_details.find("table", id="GridView1")
    if table:
        rows = table.find_all("tr")
        if len(rows) >= 2:
            cols = rows[1].find_all("td")
            if len(cols) >= 3:
                data.a_end = clean_end_text(cols[0].get_text(strip=True))
                data.z_end = clean_end_text(cols[2].get_text(strip=True))
                col_1 = cols[1].get_text(" ", strip=True)
                
                len_m = re.search(r"([\d,]+\.?\d*m)", col_1, re.IGNORECASE)
                data.length = len_m.group(1) if len_m else "Unknown"
                
                sp_m = re.search(r"(\d+)SP", col_1, re.IGNORECASE)
                data.summary_spares = sp_m.group(1) if sp_m else "0"

    # 5. Determine Type
    a_up, z_up, n_up = data.a_end.upper(), data.z_end.upper(), data.name.upper()
    is_bjl_a, is_bjl_z = "BJL" in a_up, "BJL" in z_up
    is_ajl_a, is_ajl_z = "AJL" in a_up, "AJL" in z_up
    
    if is_bjl_a and is_bjl_z: data.cable_type = "CAN2000 Backbone"
    elif "BSS" in n_up and ((is_bjl_a and not is_bjl_z and not is_ajl_z) or (is_bjl_z and not is_bjl_a and not is_ajl_a)):
        data.cable_type = "CAN2000 Backbone"
    elif is_ajl_a or is_ajl_z: data.cable_type = "CAN2000 Access"
    else: data.cable_type = "Non-CAN2000"

    # 6. Fibres & Tubes Logic
    tubes = []
    tbl2 = soup_details.find("table", id="GridView2")
    if tbl2:
        all_fibres = []
        for r in tbl2.find_all("tr")[1:]:
            c = r.find_all("td")
            if len(c) >= 12:
                try:
                    all_fibres.append({
                        "seq": int(c[1].get_text(strip=True)) if c[1].get_text(strip=True).isdigit() else 0,
                        "buffer": c[3].get_text(strip=True),
                        "st": c[7].get_text(strip=True)
                    })
                except: continue
        
        # Group tubes
        curr, last_buf = [], None
        for f in all_fibres:
            if last_buf is None: curr, last_buf = [f], f['buffer']
            elif f['buffer'] == last_buf: curr.append(f)
            else:
                tubes.append(curr)
                curr, last_buf = [f], f['buffer']
        if curr: tubes.append(curr)

    # 7. Calculate Stats
    flat = [f for t in tubes for f in t]
    data.total_fibres = len(flat)
    
    trunk_fibres, junc_fibres = [], []
    num_tubes = len(tubes)

    if data.cable_type == "CAN2000 Backbone":
        if num_tubes >= 1: [trunk_fibres.extend(t) for t in tubes[:2]]
        if num_tubes > 4: [junc_fibres.extend(t) for t in tubes[2:-2]]
    elif data.cable_type == "CAN2000 Access":
        if data.total_fibres == 144:
            for f in flat:
                if not ((49 <= f['seq'] <= 72) or (121 <= f['seq'] <= 144)): junc_fibres.append(f)
        elif num_tubes > 2:
            [junc_fibres.extend(t) for t in tubes[:-2]]

    data.trunk_spares = sum(1 for f in trunk_fibres if f['st'].strip().upper() in ["SP", ""])
    data.junction_spares = sum(1 for f in junc_fibres if f['st'].strip().upper() in ["SP", ""])

    return data

# ==========================================
# 2. UI LAYOUT (Streamlit)
# ==========================================

st.set_page_config(page_title="VMR Cable Crawler", layout="wide", page_icon="📡")

# Custom CSS for "Modern" look
st.markdown("""
<style>
    .reportview-container { background: #f0f2f6; }
    h1 { color: #1e3a8a; }
    .stButton>button { width: 100%; border-radius: 5px; height: 3em; background-color: #2563eb; color: white; }
    .stDownloadButton>button { width: 100%; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

st.title("📡 VMR Cable Analyzer Pro")
st.markdown("Enter cable names below to crawl data from the VMR system.")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("1. Input Data")
    raw_input = st.text_area("Paste Cable Names (one per line):", height=300, placeholder="22BSS-123\n44BJL-999...")
    
    start_btn = st.button("🚀 Start Crawling")

    st.info("Note: Ensure you are connected to the Optus Network/VPN.")

with col2:
    st.subheader("2. Results")
    results_placeholder = st.empty()
    
    if start_btn and raw_input:
        # Prepare list
        cables = [line.strip() for line in raw_input.split('\n') if line.strip()]
        
        if not cables:
            st.warning("Please enter at least one cable name.")
        else:
            # Initialize
            session = make_session()
            progress_bar = st.progress(0)
            status_text = st.empty()
            results = []
            
            # Processing Loop
            for i, cable in enumerate(cables):
                status_text.text(f"Processing {i+1}/{len(cables)}: {cable}...")
                
                try:
                    data = parse_cable_logic(session, cable)
                    results.append(asdict(data))
                except Exception as e:
                    err_data = CableData(name=cable, status=f"Error: {str(e)}")
                    results.append(asdict(err_data))
                
                # Update progress
                progress_bar.progress((i + 1) / len(cables))
                time.sleep(0.05) 

            # Finalize
            progress_bar.empty()
            status_text.success(f"Completed! Processed {len(cables)} cables.")
            
            # Create DataFrame
            df = pd.DataFrame(results)
            
            # Rename columns to match user requirement
            rename_map = {
                "name": "Cable Name",
                "url": "VMR link",
                "length": "Cable length",
                "total_fibres": "Fibre cable size",
                "summary_spares": "Spare Fibre",
                "trunk_spares": "trunk_spares",
                "junction_spares": "junction_spares",
                "a_end": "Splice or site A end",
                "z_end": "Splice or site B end",
                "status": "Status"
            }
            # Reorder and rename
            final_df = df[rename_map.keys()].rename(columns=rename_map)
            
            # Show Table
            st.dataframe(final_df, use_container_width=True, height=400)
            
            # Export Options
            st.subheader("3. Export")
            c1, c2 = st.columns(2)
            
            # CSV Download
            csv_data = final_df.to_csv(index=False).encode('utf-8')
            c1.download_button(
                label="📥 Download CSV",
                data=csv_data,
                file_name="vmr_cable_report.csv",
                mime="text/csv"
            )
            
            # Excel Download (Binary)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                final_df.to_excel(writer, index=False, sheet_name='Cables')
            
            c2.download_button(
                label="📊 Download Excel",
                data=buffer.getvalue(),
                file_name="vmr_cable_report.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

            # Store in session state to prevent disappearance on reload (optional basic persistence)
            st.session_state['last_results'] = final_df

    elif 'last_results' in st.session_state:
        st.dataframe(st.session_state['last_results'], use_container_width=True)