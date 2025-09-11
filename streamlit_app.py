import re, io, requests, base64
from pathlib import Path
import pandas as pd
import numpy as np
import streamlit as st
import pycountry

# --------- CONFIG ----------
DATA_PATH = Path("data/Yakubu Analysis Updated.xlsx")

# Hard-coded IPC 3+ (0–1) by ISO-3
IPC3_BY_ISO3 = {
    "AFG":0.27,"AGO":0.49,"BDI":0.15,"BEN":0.03,"BFA":0.07,"BGD":0.16,"CAF":0.34,"CIV":0.05,
    "CMR":0.00,"COD":0.22,"CPV":0.00,"DJI":0.17,"DOM":0.09,"ECU":0.14,"ETH":0.30,"GHA":0.07,
    "GIN":0.13,"GMB":0.00,"GNB":0.07,"GTM":0.17,"HND":0.18,"HTI":0.48,"KEN":0.11,"LBN":0.21,
    "LBR":0.08,"LSO":0.19,"MDG":0.15,"MLI":0.00,"MOZ":0.28,"MRT":0.07,"MWI":0.20,"NAM":0.38,
    "NER":0.00,"NGA":0.12,"PAK":0.22,"PSE":1.00,"SDN":0.45,"SEN":0.00,"SLE":0.11,"SLV":0.13,
    "SOM":0.17,"SSD":0.47,"SWZ":0.16,"TCD":0.13,"TGO":0.10,"TLS":0.27,"TZA":0.10,"UGA":0.17,
    "YEM":0.49,"ZAF":0.16,"ZMB":0.29,"ZWE":0.27
}

# --------- Helpers ----------
def _clean_cols(df):
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [' '.join([str(x) for x in tup if str(x) != 'nan']).strip() for tup in df.columns.values]
    df.columns = [re.sub(r'\s+', ' ', str(c)).strip() for c in df.columns]
    return df

def _to_num(x):
    if pd.isna(x): return np.nan
    s = str(x).replace(",", "")
    s = re.sub(r'[\$\u20AC\u00A3%]', '', s)
    try: return float(s)
    except: return np.nan

def _col_for(patterns, cols):
    for p in patterns:
        for c in cols:
            if re.search(p, c, flags=re.I):
                return c
    return None

def _detect_base_cols(df):
    cols = list(df.columns)
    country = _col_for([r'^country$', r'country\s*name', r'location', r'iso.*name'], cols)
    cirv = _col_for([r'\bcirv\b', r'index.*risk', r'vulnerab'], cols)
    return country, cirv

def _detect_shock_cols(df, shock):
    cols = list(df.columns)
    amt = _col_for([fr'{shock}.*ufe\+?rr.*\$', fr'{shock}.*total.*\$', fr'{shock}.*alloc.*\$', fr'{shock}.*usd'], cols)
    cnt = _col_for([fr'{shock}.*(ufe\s*rr|ufe\+?rr).*(#|count|num|number)'], cols)
    ufe_amt = _col_for([fr'{shock}.*\bufe\b.*\$', fr'{shock}.*underfunded.*\$', fr'{shock}.*\bufe\b', fr'{shock}.*underfunded\s*emergency'], cols)
    rr_amt  = _col_for([fr'{shock}.*\brr\b.*\$', fr'{shock}.*rapid\s*response.*\$', fr'{shock}.*rapid\s*response'], cols)
    ufe_cnt = _col_for([fr'{shock}.*\bufe\b.*(#|count|num|number)'], cols)
    rr_cnt  = _col_for([fr'{shock}.*\brr\b.*(#|count|num|number)'], cols)
    fw = _col_for([fr'{shock}.*framework.*status', fr'{shock}.*aa\s*framework', fr'{shock}.*trigger.*status', fr'{shock}.*framework'], cols)
    tier = _col_for([fr'{shock}.*tier'], cols)
    return {"amt": amt, "cnt": cnt, "ufe_amt": ufe_amt, "rr_amt": rr_amt,
            "ufe_cnt": ufe_cnt, "rr_cnt": rr_cnt, "fw": fw, "tier": tier}

def _detect_funding_plan_col(df):
    cols = list(df.columns)
    patterns = [r'fund(ing)?\s*plan', r'(hnrp)\s*plan', r'flash\s*appeal',
                r'(response|humanitarian)\s*plan', r'\bappeal\b', r'\bhnrp\b']
    return _col_for(patterns, cols)

# ---------- Country normalization ----------
_ALIAS_TO_CANON = {
    "DR Congo":"Congo, The Democratic Republic of the",
    "Dem. Rep. Congo":"Congo, The Democratic Republic of the",
    "Democratic Republic of Congo":"Congo, The Democratic Republic of the",
    "Congo, Dem. Rep.":"Congo, The Democratic Republic of the",
    "Congo (Brazzaville)":"Congo",
    "Cote d'Ivoire": "Côte d'Ivoire",
    "Cote dIvoire":  "Côte d'Ivoire",
    "Ivory Coast":   "Côte d'Ivoire",
    "Swaziland":"Eswatini",
    "Burma":"Myanmar",
    "Czech Republic":"Czechia",
    "Macedonia":"North Macedonia",
    "Turkey":"Türkiye",
    "Turkiye":"Türkiye",
    "Laos":"Lao People's Democratic Republic",
    "Lao PDR":"Lao People's Democratic Republic",
    "Cape Verde":"Cabo Verde",
    "Palestine":"Palestine, State of",
    "Syria":"Syrian Arab Republic",
    "Moldova":"Moldova, Republic of",
    "Russia":"Russian Federation",
}
def _name_alias(nm): return _ALIAS_TO_CANON.get((nm or "").strip(), (nm or "").strip())
def _name_to_iso3(nm):
    nm = _name_alias(nm)
    try:
        return pycountry.countries.search_fuzzy(nm)[0].alpha_3.upper()
    except Exception:
        special = {
            "Congo, The Democratic Republic of the": "COD",
            "Cabo Verde": "CPV",
            "Côte d'Ivoire": "CIV",
            "Palestine, State of": "PSE",
            "Russian Federation": "RUS",
            "Syrian Arab Republic": "SYR",
            "Lao People's Democratic Republic": "LAO",
            "Moldova, Republic of": "MDA",
            "Türkiye": "TUR",
        }
        return special.get(nm, None)

# -------- Anticipation Hub tables -> hazard — org (Active / Under Dev) --------
def _extract_src_from_office_online_url(office_online_url):
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(office_online_url); q = parse_qs(parsed.query)
    return q.get("src", [None])[0]

@st.cache_data(show_spinner=False)
def load_aa_frameworks():
    dev_url = "https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fwww.anticipation-hub.org%2FDocuments%2FReports%2FOverview-report-2024%2FTable_A3._Frameworks_under_development_in_2024_FINAL_TABLE.xlsx&wdOrigin=BROWSELINK"
    act_url = "https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fwww.anticipation-hub.org%2FDocuments%2FReports%2FOverview-report-2024%2FTable_A2._Active_frameworks_in_2024_FINAL_TABLE.xlsx&wdOrigin=BROWSELINK"
    src_dev = _extract_src_from_office_online_url(dev_url)
    src_act = _extract_src_from_office_online_url(act_url)

    def _fetch(url):
        try:
            r = requests.get(url, timeout=60); r.raise_for_status()
            return pd.read_excel(io.BytesIO(r.content), engine="openpyxl")
        except Exception:
            return pd.DataFrame()

    df_dev = _fetch(src_dev) if src_dev else pd.DataFrame()
    df_act = _fetch(src_act) if src_act else pd.DataFrame()
    if df_dev.empty and df_act.empty:
        return pd.DataFrame(columns=["_iso3","status","_hazard","_org"])

    def clean_cols(cols):
        out=[]
        for c in cols:
            s = str(c).strip().lower()
            s = re.sub(r"[^\w\s]", "", s)
            s = re.sub(r"\s+", "_", s)
            out.append(s)
        return out

    if not df_dev.empty:
        df_dev.columns = clean_cols(df_dev.columns); df_dev["status"] = "Under Development"
    if not df_act.empty:
        df_act.columns = clean_cols(df_act.columns); df_act["status"] = "Active"

    df_all = pd.concat([df_dev, df_act], ignore_index=True, sort=False)
    cols = list(df_all.columns)

    ctry_col = _col_for([r'^country$', r'country.*name', r'country'], cols)
    df_all["_country_tmp"] = df_all[ctry_col].astype(str).str.strip() if ctry_col else ""
    df_all["_iso3"] = df_all["_country_tmp"].map(lambda x: _name_to_iso3(_name_alias(x)))

    # hazard-like columns
    hazard_candidates = [
        r'\bhazard(s)?\b', r'\bshock(s)?\b', r'\btrigger(s)?\b', r'\bevent\b',
        r'\bhazard_?type\b', r'\btype\b', r'\bperil\b', r'\brisk\b',
        r'\bhazard_?category\b', r'\bshock_?type\b', r'\bhazard_?name\b'
    ]
    haz_cols = []
    for pat in hazard_candidates:
        c = _col_for([pat], cols)
        if c and c not in haz_cols:
            haz_cols.append(c)

    org_candidates = [
        r'coordinat(ing|ion|or)', r'lead(_)?(agency|org|organization)',
        r'\borg(anization)?\b', r'\bagency\b', r'partner', r'institution',
        r'ministry', r'authority'
    ]
    org_cols = []
    for pat in org_candidates:
        c = _col_for([pat], cols)
        if c and c not in org_cols:
            org_cols.append(c)

    HAZ_KW = [
        "flood","floods","riverine flood","flash flood","coastal flood",
        "drought","dry spell","heatwave","heat wave","coldwave","cold wave",
        "storm","windstorm","severe storm","tropical storm","cyclone","hurricane","typhoon",
        "landslide","mudslide","avalanche",
        "earthquake","seismic","tsunami","volcano","volcanic eruption",
        "wildfire","bushfire","forest fire",
        "epidemic","pandemic","cholera","disease outbreak","outbreak",
        "food insecurity","locust","pluvial flood"
    ]
    HAZ_RE = re.compile(r'\b(' + "|".join(map(re.escape, HAZ_KW)) + r')\b', flags=re.I)

    def _tidy(s):
        s = (str(s) if not pd.isna(s) else "").strip()
        s = re.sub(r"\s+", " ", s)
        return s

    def build_hazard(row):
        vals = []
        for c in haz_cols:
            vals.append(_tidy(row.get(c, "")))
        vals = [v for v in vals if v]
        if vals:
            main = vals[0]
            extra = next((v for v in vals[1:] if v.lower() != main.lower()), "")
            return f"{main} / {extra}" if extra else main

        found = []
        for v in row.values:
            s = _tidy(v)
            if not s: 
                continue
            for m in HAZ_RE.findall(s):
                token = m.strip()
                if token and token.lower() not in [x.lower() for x in found]:
                    found.append(token)
        if found:
            return f"{found[0]} / {found[1]}" if len(found) >= 2 else found[0]
        return ""

    def build_org(row):
        for c in org_cols:
            val = _tidy(row.get(c, ""))
            if val:
                parts = re.split(r"[;/,\u2013\u2014\-]+", val)
                parts = [p.strip() for p in parts if p.strip()]
                if parts:
                    return parts[0]
        return ""

    out = df_all[["_iso3", "status"]].copy()
    out["_hazard"] = df_all.apply(build_hazard, axis=1)
    out["_org"]    = df_all.apply(build_org, axis=1)
    out = out[(out["_iso3"].notna()) & ((out["_hazard"].str.strip()!="") | (out["_org"].str.strip()!=""))]
    return out

AA_RAW = load_aa_frameworks()

def build_aa_maps():
    flags = {}
    details = {}
    if AA_RAW.empty:
        return flags, details
    tmp = AA_RAW.copy()
    tmp["_iso3"] = tmp["_iso3"].astype(str)
    for iso, sub in tmp.groupby("_iso3"):
        details.setdefault(iso, {"Active": [], "Under Development": []})
        has_any = False
        for _, r in sub.iterrows():
            haz = str(r.get("_hazard","")).strip()
            org = str(r.get("_org","")).strip()
            status = str(r.get("status","")).strip()
            bits = [b for b in [haz, org] if b]
            if not bits: 
                continue
            label = " — ".join(bits)
            if status in details[iso] and label not in details[iso][status]:
                details[iso][status].append(label)
                has_any = True
        flags[iso] = has_any
    return flags, details

AA_FLAGS_ISO, AA_DETAILS_ISO = build_aa_maps()

# ------------- Core build -------------
def build_view(shock, sheet=None, top_n=30,
               use_exclude_fw=True, use_cirv_med=False,
               use_hnrp=False, use_food_insec=False, use_other_aa=False):
    xl = pd.ExcelFile(DATA_PATH)
    if sheet is None: sheet = xl.sheet_names[0]
    df = pd.read_excel(DATA_PATH, sheet_name=sheet)
    df = _clean_cols(df)

    country_col, cirv_col = _detect_base_cols(df)
    cols = _detect_shock_cols(df, shock)
    if not country_col or not cols["amt"]:
        raise ValueError("Could not detect required columns in the Excel.")

    # normalize nums
    for key in ["amt","ufe_amt","rr_amt"]:
        c = cols.get(key)
        if c and c in df.columns:
            df[c] = df[c].map(_to_num)
    for key in ["cnt","ufe_cnt","rr_cnt"]:
        c = cols.get(key)
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if cirv_col and cirv_col in df.columns:
        df[cirv_col] = df[cirv_col].map(_to_num)

    sel = df[df[cols["amt"]].fillna(0) > 0].copy()
    if "Country" not in sel.columns and country_col != "Country":
        sel = sel.rename(columns={country_col: "Country"})
    sel["Country"] = sel["Country"].astype(str).str.strip()
    if cols["tier"] and cols["tier"] in sel.columns:
        tnorm = sel[cols["tier"]].astype(str).str.extract(r'(Tier\s*\d)', expand=False)
        sel["_TierNorm"] = tnorm.fillna(sel[cols["tier"]].astype(str))
        sel = sel[sel["_TierNorm"].str.contains(r'(?i)Tier\s*1|Tier\s*2', na=False)]
    else:
        sel["_TierNorm"] = np.nan

    sel["ISO3"] = sel["Country"].map(_name_to_iso3)
    sel["IPC3plus"] = sel["ISO3"].map(IPC3_BY_ISO3)

    # Funding plan / HNRP
    funding_plan_col = _detect_funding_plan_col(sel)
    if funding_plan_col is None:
        funding_plan_col = _col_for([r'plan', r'appeal', r'(hnrp)'], list(sel.columns))
    if funding_plan_col and funding_plan_col in sel.columns:
        sel[funding_plan_col] = sel[funding_plan_col].astype(str).replace({"nan":"", "NaN":"", "None":""})
        sel[funding_plan_col] = sel[funding_plan_col].apply(lambda s: "" if str(s).strip() in {"0","0.0","0.00"} else s)
        sel["HNRP"] = sel[funding_plan_col].str.contains(r'\bHNRP\b', case=False, na=False)
        sel["HasFundingPlan"] = sel[funding_plan_col].str.strip().ne("")
    else:
        sel["HNRP"] = False
        sel["HasFundingPlan"] = False

    # Other AA flag via ISO3
    sel["HasOtherAA"] = sel["ISO3"].map(lambda iso: bool(AA_FLAGS_ISO.get(iso or "", False)))

    # ranking base (apply criteria)
    ds = sel.copy()
    fw_col = cols["fw"]; amt_col = cols["amt"]
    if use_exclude_fw and fw_col and fw_col in ds.columns:
        bad = ds[fw_col].astype(str).str.strip().str.lower()
        ds = ds[~bad.isin({"active","dormant","under development","under-development","under_development"})]
    if use_cirv_med and cirv_col and cirv_col in ds.columns:
        top10 = sel.sort_values(by=amt_col, ascending=False).head(10)
        cirv_median_top10 = float(top10[cirv_col].median()) if not top10.empty else np.nan
        if pd.notna(cirv_median_top10):
            ds = ds[ds[cirv_col] > cirv_median_top10]
    if use_hnrp: ds = ds[ds["HNRP"] == True]
    if use_food_insec: ds = ds[(~ds["IPC3plus"].isna()) & (ds["IPC3plus"] > 0)]
    if use_other_aa: ds = ds[ds["HasOtherAA"] == True]

    ranked = ds.sort_values(by=amt_col, ascending=False).copy()
    tier1 = ranked["Country"].tolist()[:3]
    tier2 = ranked["Country"].tolist()[3:6]

    return sel, cols, cirv_col, amt_col, funding_plan_col, tier1, tier2

def pct01(v): 
    if pd.isna(v) or float(v)<=0: return ""
    return f"{int(round(float(v)*100))}%"

def cur(v): 
    if pd.isna(v): return ""
    return f"{int(round(float(v))):,}"

def cnt(v):
    v = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    return "" if pd.isna(v) else str(int(round(float(v))))

def render_country_details(row, cols, cirv_col, amt_col, funding_plan_col):
    country = str(row["Country"])
    iso3 = row.get("ISO3","")
    fw_col = cols.get("fw")
    fw_v = row.get(fw_col, "")
    fw_str = "" if str(fw_v).strip().lower() in {"inactive","0","0.0","0.00"} else str(fw_v or "")
    cirv = row.get(cirv_col, np.nan)
    ipc  = row.get("IPC3plus", np.nan)
    rr_amt = row.get(cols.get("rr_amt"), np.nan)
    rr_cnt = row.get(cols.get("rr_cnt"), np.nan)
    ufe_amt= row.get(cols.get("ufe_amt"), np.nan)
    ufe_cnt= row.get(cols.get("ufe_cnt"), np.nan)
    amt    = row.get(amt_col, np.nan)
    fplan  = row.get(funding_plan_col, "") if funding_plan_col else ""
    by_iso = AA_DETAILS_ISO.get(iso3, {"Active": [], "Under Development": []})
    act_list = by_iso.get("Active", [])
    dev_list = by_iso.get("Under Development", [])
    def ul(lst):
        if not lst: return "<span style='color:#666;'>None</span>"
        return "<ul style='margin:2px 0 0 18px; padding:0;'>" + "".join([f"<li>{x}</li>" for x in lst]) + "</ul>"
    html = f"""
    <div style="border:1px solid #e5e7eb;border-radius:10px;padding:10px;margin:10px 0;background:#fff;">
      <div style="font-weight:800;font-size:16px;margin-bottom:6px;">{country} — Details</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px 18px;font-size:13px;">
        <div><b>Funding Plan</b></div><div>{fplan if fplan else ''}</div>
        <div><b>Other AA (in 2024)</b></div><div>{ul(act_list)}</div>
        <div><b>Other AA (Under Development)</b></div><div>{ul(dev_list)}</div>
        <div><b>Framework Status</b></div><div>{fw_str}</div>
        <div><b>CIRV</b></div><div>{'' if pd.isna(cirv) else int(round(float(cirv)))}</div>
        <div><b>IPC 3+ (%)</b></div><div>{pct01(ipc)}</div>
        <div><b>RR Allocations ($)</b></div><div>{cur(rr_amt)}</div>
        <div><b>RR Count (#)</b></div><div>{cnt(rr_cnt)}</div>
        <div><b>UFE Allocations ($)</b></div><div>{cur(ufe_amt)}</div>
        <div><b>UFE Count (#)</b></div><div>{cnt(ufe_cnt)}</div>
        <div><b>UFE+RR Amount ($)</b></div><div>{cur(amt)}</div>
      </div>
    </div>
    """
    return html

# ---------------- UI ----------------
st.set_page_config(page_title="Recommended 2026 Priorities", layout="wide")
st.title("Recommended 2026 Priorities")

# Sidebar controls
shock = st.sidebar.selectbox("Shock", ["cholera","floods","drought","storms"], index=0, format_func=str.capitalize)
st.sidebar.markdown("**Dashboard filters**")
excl_fw = st.sidebar.checkbox("Exclude Active/Dormant/Under Dev", value=True)
cirv_med = st.sidebar.checkbox("CIRV > median (Top-10 by amount)", value=False)
req_hnrp = st.sidebar.checkbox("Require HNRP status", value=False)
req_ipc  = st.sidebar.checkbox("Require food insecurity (IPC 3+ > 0)", value=False)
req_aa   = st.sidebar.checkbox("Require other AA frameworks", value=False)

# Load & compute
try:
    sel, cols, cirv_col, amt_col, funding_plan_col, tier1, tier2 = build_view(
        shock, use_exclude_fw=excl_fw, use_cirv_med=cirv_med,
        use_hnrp=req_hnrp, use_food_insec=req_ipc, use_other_aa=req_aa
    )
except Exception as e:
    st.error(f"Failed to load or parse the Excel file. {e}")
    st.stop()

# Card grid renderer
def yesno(b): return "Yes" if bool(b) else "No"

def render_cards(df_part, label):
    st.subheader(label)
    if df_part.empty:
        st.info("No countries meet the selected criteria.")
        return
    cols_grid = st.columns(3) if len(df_part)>=3 else st.columns(len(df_part))
    for i, (_, row) in enumerate(df_part.iterrows()):
        with cols_grid[i % len(cols_grid)]:
            fw_col = cols.get("fw")
            fw_v = row.get(fw_col, "")
            fw_str = "" if str(fw_v).strip().lower() in {"inactive","0","0.0","0.00"} else str(fw_v or "")
            has_fplan = bool(row.get("HasFundingPlan", False))
            has_other = bool(row.get("HasOtherAA", False))
            cirv_text = "" if pd.isna(row.get(cirv_col)) else str(int(round(float(row[cirv_col]))))
            ipc_text = pct01(row.get("IPC3plus"))

            st.markdown(f"### {row['Country']}")
            st.markdown(
                f"**CIRV:** {cirv_text}  \n"
                f"**IPC 3+ (%):** {ipc_text}  \n"
                f"**Funding Plan?** {yesno(has_fplan)}  \n"
                f"**Other AA?** {yesno(has_other)}  \n"
                f"**{shock.capitalize()} Framework:** {fw_str}"
            )

# Build Tier dataframes
ranked = sel.sort_values(by=cols["amt"], ascending=False)
tier1_df = ranked[ranked["Country"].isin(tier1)].set_index("Country").loc[tier1].reset_index()
tier2_df = ranked[ranked["Country"].isin(tier2)].set_index("Country").loc[tier2].reset_index()

# Render Tier cards
render_cards(tier1_df, "Tier 1")
render_cards(tier2_df, "Tier 2")

# Details selector (Tier 1 + Tier 2 list)
eligible = tier1 + tier2
st.markdown("### Country Details")
choice = st.selectbox("Select a Tier 1/2 country", eligible, index=0 if eligible else None)
if choice:
    row = ranked[ranked["Country"]==choice].iloc[0]
    st.markdown(render_country_details(row, cols, cirv_col, cols["amt"], funding_plan_col), unsafe_allow_html=True)

# Expandable: top table
with st.expander("Show Details Table"):
    # pick rendered columns (simple)
    view_cols = ["Country"]
    if "_TierNorm" in sel.columns: view_cols += ["_TierNorm"]
    if cirv_col: view_cols += [cirv_col]
    view_cols += ["IPC3plus"]
    if funding_plan_col and funding_plan_col in sel.columns: view_cols += [funding_plan_col]
    if cols["fw"]: view_cols += [cols["fw"]]
    view_cols += [cols["amt"]]
    for extra in [cols.get("cnt"), cols.get("ufe_amt"), cols.get("ufe_cnt"), cols.get("rr_amt"), cols.get("rr_cnt")]:
        if extra: view_cols.append(extra)
    view = sel[view_cols].copy()
    # polish
    labels = {cirv_col:"CIRV", "IPC3plus":"IPC 3+ (%)", cols["amt"]:f"{shock.capitalize()} UFE+RR Amount ($)"}
    if "_TierNorm" in view.columns: labels["_TierNorm"]="Tier"
    if funding_plan_col in view.columns: labels[funding_plan_col]="Funding Plan"
    if cols["fw"]: labels[cols["fw"]]=f"{shock.capitalize()} Framework Status"
    view = view.rename(columns=labels)
    if "CIRV" in view.columns: view["CIRV"] = pd.to_numeric(view["CIRV"], errors="coerce").round(0).astype("Int64")
    if "IPC 3+ (%)" in view.columns: view["IPC 3+ (%)"] = view["IPC 3+ (%)"].apply(lambda v: "" if (pd.isna(v) or float(v)<=0) else f"{int(round(float(v)*100))}%")
    money_cols = [c for c in view.columns if c.endswith("Amount ($)")]
    for mc in money_cols:
        view[mc] = pd.to_numeric(view[mc], errors="coerce").apply(lambda v: "" if pd.isna(v) else f"{int(round(float(v))):,}")
    cnt_cols = [c for c in view.columns if c.endswith("Count (#)")]
    for cc in cnt_cols:
        view[cc] = pd.to_numeric(view[cc], errors="coerce").apply(lambda v: "" if pd.isna(v) else str(int(v)))
    st.dataframe(view.reset_index(drop=True), use_container_width=True)
