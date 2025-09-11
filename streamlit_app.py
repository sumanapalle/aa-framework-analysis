import re, io, requests
from pathlib import Path
import pandas as pd
import numpy as np
import pycountry
import streamlit as st

# ---------------- CONFIG ----------------
INPUT_PATH = "Yakubu Analysis Updated.xlsx"  # allocations workbook at repo root

# ---------------- Hard-coded IPC 3+ in DECIMALS (map by ISO-3) ----------------
IPC3_BY_ISO3 = {
    "AFG":0.27,"AGO":0.49,"BDI":0.15,"BEN":0.03,"BFA":0.07,"BGD":0.16,"CAF":0.34,"CIV":0.05,
    "CMR":0.00,"COD":0.22,"CPV":0.00,"DJI":0.17,"DOM":0.09,"ECU":0.14,"ETH":0.30,"GHA":0.07,
    "GIN":0.13,"GMB":0.00,"GNB":0.07,"GTM":0.17,"HND":0.18,"HTI":0.48,"KEN":0.11,"LBN":0.21,
    "LBR":0.08,"LSO":0.19,"MDG":0.15,"MLI":0.00,"MOZ":0.28,"MRT":0.07,"MWI":0.20,"NAM":0.38,
    "NER":0.00,"NGA":0.12,"PAK":0.22,"PSE":1.00,"SDN":0.45,"SEN":0.00,"SLE":0.11,"SLV":0.13,
    "SOM":0.17,"SSD":0.47,"SWZ":0.16,"TCD":0.13,"TGO":0.10,"TLS":0.27,"TZA":0.10,"UGA":0.17,
    "YEM":0.49,"ZAF":0.16,"ZMB":0.29,"ZWE":0.27
}

# ---------------- Helpers ----------------
def _clean_cols(df):
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join([str(x) for x in tup if str(x) != "nan"]).strip()
            for tup in df.columns.values
        ]
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]
    return df

def _to_num(x):
    if pd.isna(x): return np.nan
    s = str(x).replace(",", "")
    s = re.sub(r"[\$\u20AC\u00A3%]", "", s)
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
    country = _col_for([r"^country$", r"country\s*name", r"location", r"iso.*name"], cols)
    cirv = _col_for([r"\bcirv\b", r"index.*risk", r"vulnerab"], cols)
    return country, cirv

def _detect_shock_cols(df, shock):
    cols = list(df.columns)
    amt = _col_for([fr"{shock}.*ufe\+?rr.*\$", fr"{shock}.*total.*\$", fr"{shock}.*alloc.*\$", fr"{shock}.*usd"], cols)
    cnt = _col_for([fr"{shock}.*(ufe\s*rr|ufe\+?rr).*(#|count|num|number)"], cols)
    ufe_amt = _col_for([fr"{shock}.*\bufe\b.*\$", fr"{shock}.*underfunded.*\$", fr"{shock}.*\bufe\b", fr"{shock}.*underfunded\s*emergency"], cols)
    rr_amt  = _col_for([fr"{shock}.*\brr\b.*\$", fr"{shock}.*rapid\s*response.*\$", fr"{shock}.*rapid\s*response"], cols)
    ufe_cnt = _col_for([fr"{shock}.*\bufe\b.*(#|count|num|number)"], cols)
    rr_cnt  = _col_for([fr"{shock}.*\brr\b.*(#|count|num|number)"], cols)
    fw = _col_for([fr"{shock}.*framework.*status", fr"{shock}.*aa\s*framework", fr"{shock}.*trigger.*status", fr"{shock}.*framework"], cols)
    tier = _col_for([fr"{shock}.*tier"], cols)
    return {"amt": amt, "cnt": cnt, "ufe_amt": ufe_amt, "rr_amt": rr_amt,
            "ufe_cnt": ufe_cnt, "rr_cnt": rr_cnt, "fw": fw, "tier": tier}

def _detect_funding_plan_col(df):
    cols = list(df.columns)
    patterns = [
        r"fund(ing)?\s*plan", r"(hnrp)\s*plan", r"flash\s*appeal",
        r"(response|humanitarian)\s*plan", r"\bappeal\b", r"\bhnrp\b",
    ]
    return _col_for(patterns, cols)

# ---------- Country name normalization & ISO-3 ----------
_ALIAS_TO_CANON = {
    "DR Congo": "Congo, The Democratic Republic of the",
    "Dem. Rep. Congo": "Congo, The Democratic Republic of the",
    "Democratic Republic of Congo": "Congo, The Democratic Republic of the",
    "Congo, Dem. Rep.": "Congo, The Democratic Republic of the",
    "Congo (Brazzaville)": "Congo",
    "Cote d'Ivoire": "Côte d'Ivoire",
    "Cote dIvoire": "Côte d'Ivoire",
    "Ivory Coast": "Côte d'Ivoire",
    "Swaziland": "Eswatini",
    "Burma": "Myanmar",
    "Czech Republic": "Czechia",
    "Macedonia": "North Macedonia",
    "Turkey": "Türkiye",
    "Turkiye": "Türkiye",
    "Laos": "Lao People's Democratic Republic",
    "Lao PDR": "Lao People's Democratic Republic",
    "Cape Verde": "Cabo Verde",
    "Palestine": "Palestine, State of",
    "Syria": "Syrian Arab Republic",
    "Moldova": "Moldova, Republic of",
    "Russia": "Russian Federation",
}
def _name_alias(nm: str) -> str:
    nm = (nm or "").strip()
    return _ALIAS_TO_CANON.get(nm, nm)

def _name_to_iso3(nm: str) -> str:
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

# -------- Anticipation Hub frameworks (Active & Under Development) --------
def _extract_src_from_office_online_url(office_online_url):
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(office_online_url); q = parse_qs(parsed.query)
    return q.get("src", [None])[0]

@st.cache_data(show_spinner=False, ttl=60*60)
def load_aa_frameworks():
    """
    Returns rows keyed by ISO3 with explicit hazard & org, so details panel can render
    'hazard — coordinating org' for statuses: Active (2024) and Under Development.
    Columns: _iso3, status, _hazard, _org
    """
    dev_url = "https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fwww.anticipation-hub.org%2FDocuments%2FReports%2FOverview-report-2024%2FTable_A3._Frameworks_under_development_in_2024_FINAL_TABLE.xlsx&wdOrigin=BROWSELINK"
    act_url = "https://view.officeapps.live.com/op/view.aspx?src=https%3A%2F%2Fwww.anticipation-hub.org%2FDocuments%2FReports%2FOverview-report-2024%2FTable_A2._Active_frameworks_in_2024_FINAL_TABLE.xlsx&wdOrigin=BROWSELINK"
    src_dev = _extract_src_from_office_online_url(dev_url)
    src_act = _extract_src_from_office_online_url(act_url)
    if not (src_dev and src_act):
        return pd.DataFrame(columns=["_iso3","status","_hazard","_org"])

    def _fetch_xlsx(url):
        try:
            r = requests.get(url, timeout=60)
            r.raise_for_status()
            return pd.read_excel(io.BytesIO(r.content), engine="openpyxl")
        except Exception:
            return pd.DataFrame()

    df_dev = _fetch_xlsx(src_dev)
    df_act = _fetch_xlsx(src_act)
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
        df_dev.columns = clean_cols(df_dev.columns)
        df_dev["status"] = "Under Development"
    if not df_act.empty:
        df_act.columns = clean_cols(df_act.columns)
        df_act["status"] = "Active"

    df_all = pd.concat([df_dev, df_act], ignore_index=True, sort=False)
    cols = list(df_all.columns)

    # country column -> ISO3
    ctry_col = _col_for([r"^country$", r"country.*name", r"country"], cols)
    df_all["_country_tmp"] = df_all[ctry_col].astype(str).str.strip() if ctry_col else ""
    df_all["_iso3"] = df_all["_country_tmp"].map(lambda x: _name_to_iso3(_name_alias(x)))

    # hazard-like columns
    hazard_candidates = [
        r"\bhazard(s)?\b", r"\bshock(s)?\b", r"\btrigger(s)?\b", r"\bevent\b",
        r"\bhazard_?type\b", r"\btype\b", r"\bperil\b", r"\brisk\b",
        r"\bhazard_?category\b", r"\bshock_?type\b", r"\bhazard_?name\b"
    ]
    haz_cols=[]
    for pat in hazard_candidates:
        c = _col_for([pat], cols)
        if c and c not in haz_cols:
            haz_cols.append(c)

    # coordinating org columns
    org_candidates = [
        r"coordinat(ing|ion|or)", r"lead(_)?(agency|org|organization)",
        r"\borg(anization)?\b", r"\bagency\b", r"partner", r"institution",
        r"ministry", r"authority"
    ]
    org_cols=[]
    for pat in org_candidates:
        c = _col_for([pat], cols)
        if c and c not in org_cols:
            org_cols.append(c)

    # broad hazard keywords for fallback scan
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
    HAZ_RE = re.compile(r"\b(" + "|".join(map(re.escape, HAZ_KW)) + r")\b", flags=re.I)

    def _tidy(s):
        s = (str(s) if not pd.isna(s) else "").strip()
        s = re.sub(r"\s+", " ", s)
        return s

    def build_hazard(row):
        vals = [_tidy(row.get(c, "")) for c in haz_cols]
        vals = [v for v in vals if v]
        if vals:
            main = vals[0]
            extra = next((v for v in vals[1:] if v.lower() != main.lower()), "")
            return f"{main} / {extra}" if extra else main
        found=[]
        for v in row.values:
            s=_tidy(v)
            if not s: continue
            for m in HAZ_RE.findall(s):
                token=m.strip()
                if token and token.lower() not in [x.lower() for x in found]:
                    found.append(token)
        if found:
            return f"{found[0]} / {found[1]}" if len(found)>=2 else found[0]
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

    out = df_all[["_iso3","status"]].copy()
    out["_hazard"] = df_all.apply(build_hazard, axis=1)
    out["_org"]    = df_all.apply(build_org, axis=1)
    out = out[(out["_iso3"].notna()) & ((out["_hazard"].str.strip()!="") | (out["_org"].str.strip()!=""))]
    return out

AA_RAW = load_aa_frameworks()

# Build:
# - AA_FLAGS_ISO: ISO3 -> bool(has any)
# - AA_DETAILS_ISO: ISO3 -> {"Active":[ "hazard — org", ...], "Under Development":[...]}
AA_FLAGS_ISO = {}
AA_DETAILS_ISO = {}
if not AA_RAW.empty:
    tmp = AA_RAW.copy()
    tmp["_iso3"] = tmp["_iso3"].astype(str)
    for iso, sub in tmp.groupby("_iso3"):
        if iso not in AA_DETAILS_ISO:
            AA_DETAILS_ISO[iso] = {"Active": [], "Under Development": []}
        has_any = False
        for _, r in sub.iterrows():
            haz = str(r.get("_hazard","")).strip()
            org = str(r.get("_org","")).strip()
            status = str(r.get("status","")).strip()
            bits = [b for b in [haz, org] if b]
            if not bits:
                continue
            label = " — ".join(bits)  # <-- hazard — org
            if status in AA_DETAILS_ISO[iso]:
                if label not in AA_DETAILS_ISO[iso][status]:
                    AA_DETAILS_ISO[iso][status].append(label)
                has_any = True
        AA_FLAGS_ISO[iso] = has_any

# ---------- Load Excel ----------
@st.cache_data(show_spinner=False)
def _load_sheet(sheet_name=None):
    path = Path(INPUT_PATH)
    if not path.exists():
        raise FileNotFoundError(f"Excel file not found at: {INPUT_PATH}")
    xl = pd.ExcelFile(path)
    if sheet_name is None:
        sheet_name = xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet_name)
    return _clean_cols(df)

def _normalize_fields(df, cols_detected, cirv_col):
    df = df.copy()
    for key in ["amt", "ufe_amt", "rr_amt"]:
        col = cols_detected.get(key)
        if col and col in df.columns:
            df[col] = df[col].map(_to_num)
    for key in ["cnt", "ufe_cnt", "rr_cnt"]:
        col = cols_detected.get(key)
        if col and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if cirv_col and cirv_col in df.columns:
        df[cirv_col] = df[cirv_col].map(_to_num)
    return df

# ----- Details HTML -----
def render_country_details(row, fw_col, cirv_col, amt_col, funding_plan_col, cols):
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
    country= str(row["Country"])
    iso3   = _name_to_iso3(country) or ""

    def pct(v):  return "" if (pd.isna(v) or float(v)<=0) else f"{int(round(float(v)*100))}%"
    def cur(v):  return "" if pd.isna(v) else f"{int(round(float(v))):,}"
    def cnt(v):
        v = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
        return "" if pd.isna(v) else str(int(round(float(v))))

    by_iso = AA_DETAILS_ISO.get(iso3, {"Active": [], "Under Development": []})
    act_list = by_iso.get("Active", [])
    dev_list = by_iso.get("Under Development", [])
    def _fmt_list(lst):
        if not lst: return "<span style='color:#666;'>None</span>"
        items = "".join([f"<li>{str(x)}</li>" for x in lst])
        return f"<ul style='margin:0 0 0 16px;padding:0;'>{items}</ul>"

    return f"""
    <div style="border:1px solid #e5e7eb;border-radius:10px;padding:10px;margin:10px 0;background:#fff;">
      <div style="font-weight:800;font-size:16px;margin-bottom:6px;">{country} — Details</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px 18px;font-size:13px;">
        <div><b>Funding Plan</b></div><div>{fplan if fplan else ''}</div>
        <div><b>Other AA (in 2024)</b></div><div>{_fmt_list(act_list)}</div>
        <div><b>Other AA (Under Development)</b></div><div>{_fmt_list(dev_list)}</div>
        <div><b>Framework Status</b></div><div>{fw_str}</div>
        <div><b>CIRV</b></div><div>{'' if pd.isna(cirv) else int(round(float(cirv)))}</div>
        <div><b>IPC 3+ (%)</b></div><div>{pct(ipc)}</div>
        <div><b>RR Allocations ($)</b></div><div>{cur(rr_amt)}</div>
        <div><b>RR Count (#)</b></div><div>{cnt(rr_cnt)}</div>
        <div><b>UFE Allocations ($)</b></div><div>{cur(ufe_amt)}</div>
        <div><b>UFE Count (#)</b></div><div>{cnt(ufe_cnt)}</div>
        <div><b>UFE+RR Amount ($)</b></div><div>{cur(amt)}</div>
      </div>
    </div>
    """

# ---------------- Core builder ----------------
def build_view_and_dashboard(shock, sheet=None, top_n=30,
                             use_exclude_fw=True, use_cirv_med=False,
                             use_hnrp=False, use_food_insec=False, use_other_aa=False,
                             manual_remove_tier1=None, manual_add_countries=None):
    shock = shock.lower()
    assert shock in ("cholera","floods","drought","storms")
    df = _load_sheet(sheet)
    country_col, cirv_col = _detect_base_cols(df)
    if not country_col:
        raise ValueError("Could not detect a country column.")
    cols = _detect_shock_cols(df, shock)
    if not cols["amt"]:
        raise ValueError(f"Could not detect a '{shock}' UFE+RR amount column.")
    df = _normalize_fields(df, cols, cirv_col)

    # Only countries with non-zero allocations for this shock
    sel = df[df[cols["amt"]].fillna(0) > 0].copy()

    # Keep Tier 1 & 2 in table
    if cols["tier"] and cols["tier"] in sel.columns:
        tnorm = sel[cols["tier"]].astype(str).str.extract(r"(Tier\s*\d)", expand=False)
        sel["_TierNorm"] = tnorm.fillna(sel[cols["tier"]].astype(str))
        sel = sel[sel["_TierNorm"]].copy()
        sel = sel[sel["_TierNorm"].str.contains(r"(?i)Tier\s*1|Tier\s*2", na=False)]
    else:
        sel["_TierNorm"] = np.nan

    # Country, ISO3, IPC
    if "Country" not in sel.columns and country_col != "Country":
        sel = sel.rename(columns={country_col: "Country"})
    sel["Country"] = sel["Country"].astype(str).str.strip()
    sel["ISO3"] = sel["Country"].map(_name_to_iso3)
    sel["IPC3plus"] = sel["ISO3"].map(IPC3_BY_ISO3)  # decimal (0–1)

    # Funding Plan + HNRP + HasFundingPlan
    funding_plan_col = _detect_funding_plan_col(sel)
    if funding_plan_col is None:
        funding_plan_col = _col_for([r"plan", r"appeal", r"(hnrp)"], list(sel.columns))
    if funding_plan_col and funding_plan_col in sel.columns:
        sel[funding_plan_col] = sel[funding_plan_col].astype(str).replace({"nan":"", "NaN":"", "None":""})
        sel[funding_plan_col] = sel[funding_plan_col].apply(lambda s: "" if str(s).strip() in {"0","0.0","0.00"} else s)
        sel["HNRP"] = sel[funding_plan_col].str.contains(r"\bHNRP\b", case=False, na=False)
        sel["HasFundingPlan"] = sel[funding_plan_col].str.strip().ne("")
    else:
        sel["HNRP"] = False
        sel["HasFundingPlan"] = False

    # Other AA frameworks flags via ISO3
    sel["HasOtherAA"] = sel["ISO3"].map(lambda iso: bool(AA_FLAGS_ISO.get(iso or "", False)))

    fw_col  = cols["fw"]
    amt_col = cols["amt"]

    # ----------- Build table (also used for ranges & add dropdown) -----------
    labels = {"Country": "Country"}
    disp_cols = ["Country"]
    if "_TierNorm" in sel.columns and not sel["_TierNorm"].isna().all():
        disp_cols.append("_TierNorm"); labels["_TierNorm"] = "Tier"
    if cirv_col:
        disp_cols.append(cirv_col); labels[cirv_col] = "CIRV"
    disp_cols.append("IPC3plus"); labels["IPC3plus"] = "IPC 3+ (%)"
    if funding_plan_col and funding_plan_col in sel.columns:
        disp_cols.append(funding_plan_col); labels[funding_plan_col] = "Funding Plan"
    if fw_col:
        disp_cols.append(fw_col); labels[fw_col] = f"{shock.capitalize()} Framework Status"
    disp_cols.append(amt_col); labels[amt_col] = f"{shock.capitalize()} UFE+RR Amount ($)"
    if cols["cnt"]:
        disp_cols.append(cols["cnt"]); labels[cols["cnt"]] = f"{shock.capitalize()} UFE+RR Count (#)"
    else:
        sel[f"__{shock}_cnt__"] = np.nan
        disp_cols.append(f"__{shock}_cnt__"); labels[f"__{shock}_cnt__"] = f"{shock.capitalize()} UFE+RR Count (#)"
    # UFE + RR breakdowns
    for k, lab in [("ufe_amt","UFE Allocations ($)"),("ufe_cnt","UFE Count (#)"),
                   ("rr_amt","RR Allocations ($)"),("rr_cnt","RR Count (#)")]:
        c = cols.get(k)
        if c and (k.endswith("amt") and c!=amt_col):
            disp_cols.append(c); labels[c] = f"{shock.capitalize()} {lab}"
        elif c and k.endswith("cnt"):
            disp_cols.append(c); labels[c] = f"{shock.capitalize()} {lab}"
        else:
            tmp = f"__{shock}_{k}__"
            sel[tmp] = np.nan
            disp_cols.append(tmp); labels[tmp] = f"{shock.capitalize()} {lab}"

    view = sel[disp_cols].rename(columns=labels).copy()

    # -------- Apply dashboard criteria to build ranked base --------
    ds = sel.copy()
    if use_exclude_fw and fw_col and fw_col in ds.columns:
        bad = ds[fw_col].astype(str).str.strip().str.lower()
        ds = ds[~bad.isin({"active","dormant","under development","under-development","under_development"})]
    if use_cirv_med and cirv_col and cirv_col in ds.columns:
        top10 = sel.sort_values(by=amt_col, ascending=False).head(10)
        cirv_median_top10 = float(top10[cirv_col].median()) if not top10.empty else np.nan
        if pd.notna(cirv_median_top10):
            ds = ds[ds[cirv_col] > cirv_median_top10]
    if use_hnrp:
        ds = ds[ds["HNRP"] == True]
    if use_food_insec:
        ds = ds[(~ds["IPC3plus"].isna()) & (ds["IPC3plus"] > 0)]
    if use_other_aa:
        ds = ds[ds["HasOtherAA"] == True]

    ranked = ds.sort_values(by=amt_col, ascending=False).copy()

    # ---------- Manual overrides ----------
    r_countries = ranked["Country"].tolist()
    tier1 = r_countries[:3]
    tier2 = r_countries[3:6]
    remaining = r_countries[6:]

    # 1) Remove (from Tier 1 only)
    remove_set = set(manual_remove_tier1) if manual_remove_tier1 else set()
    if remove_set:
        tier1 = [c for c in tier1 if c not in remove_set]
        while len(tier1) < 3 and tier2:
            tier1.append(tier2.pop(0))
        while len(tier1) < 3 and remaining:
            tier1.append(remaining.pop(0))
        while len(tier2) < 3 and remaining:
            tier2.append(remaining.pop(0))

    # 2) MULTI-add to Tier 1 last slot with cascading
    add_list = [c for c in (manual_add_countries or []) if isinstance(c, str) and c.strip()]
    table_order = view["Country"].tolist()
    add_list = [c for c in table_order if c in set(add_list)]

    def apply_add(one):
        nonlocal tier1, tier2, remaining
        if one in tier1:
            if len(tier1)==0 or tier1[-1]==one: return
            prev_last = tier1[-1] if len(tier1)>=1 else None
            tier1 = [c for c in tier1 if c != one]
            bump = prev_last if prev_last and prev_last != one else None
            tier1.append(one)
        elif one in tier2:
            tier2.remove(one)
            prev_last = tier1[-1] if len(tier1)>=1 else None
            if len(tier1) < 3:
                tier1.append(one); bump=None
            else:
                bump = prev_last; tier1[-1] = one
        else:
            if one in remaining:
                remaining.remove(one)
            prev_last = tier1[-1] if len(tier1)>=1 else None
            if len(tier1) < 3:
                tier1.append(one); bump=None
            else:
                bump = prev_last; tier1[-1] = one
        if 'bump' in locals() and bump:
            tier2.insert(0, bump)
        while len(tier2) > 3:
            overflow = tier2.pop()
            remaining.insert(0, overflow)
        while len(tier2) < 3 and remaining:
            tier2.append(remaining.pop(0))

    for to_add in add_list:
        apply_add(to_add)

    # Build tier DataFrames for rendering cards
    def take_rows(names):
        if not names: return ranked.iloc[0:0].copy()
        return ranked[ranked["Country"].isin(names)].set_index("Country").loc[names].reset_index()

    tier1_df = take_rows(tier1)
    tier2_df = take_rows(tier2)

    # Prepare table country list (no blanks)
    table_countries = sorted([c for c in view["Country"].unique().tolist() if str(c).strip()])

    # ---- Table formatting (simple) ----
    sort_cols = [c for c in [f"{shock.capitalize()} UFE+RR Amount ($)", "CIRV"] if c in view.columns]
    if sort_cols:
        view = view.sort_values(by=sort_cols, ascending=[False]*len(sort_cols))
    view = view.head(top_n).reset_index(drop=True)
    view.insert(0, "#", range(1, len(view)+1))
    if "CIRV" in view.columns:
        view["CIRV"] = pd.to_numeric(view["CIRV"], errors="coerce").round(0).astype("Int64")
    fw_label = f"{shock.capitalize()} Framework Status"
    if fw_label in view.columns:
        s = view[fw_label].astype(str)
        inactive = s.str.strip().str.lower().eq("inactive")
        zeros = s.str.strip().isin(["0","0.0","0.00"])
        view[fw_label] = s.mask(inactive | zeros, "").replace("nan","")
    total_label   = f"{shock.capitalize()} UFE+RR Amount ($)"
    ufe_amt_label = f"{shock.capitalize()} UFE Allocations ($)"
    rr_amt_label  = f"{shock.capitalize()} RR Allocations ($)"
    def _fmt_curr_col(s, blank_zero=False):
        s = pd.to_numeric(s, errors="coerce")
        out=[]
        for v in s:
            if pd.isna(v): out.append("")
            else:
                if blank_zero and abs(float(v))<0.5: out.append("")
                else: out.append(f"{int(round(float(v))):,}")
        return out
    if total_label in view.columns:
        view[total_label]   = _fmt_curr_col(view[total_label], blank_zero=False)
    if ufe_amt_label in view.columns:
        view[ufe_amt_label] = _fmt_curr_col(view[ufe_amt_label], blank_zero=True)
    if rr_amt_label in view.columns:
        view[rr_amt_label]  = _fmt_curr_col(view[rr_amt_label], blank_zero=False)
    for c in [f"{shock.capitalize()} UFE+RR Count (#)",
              f"{shock.capitalize()} UFE Count (#)",
              f"{shock.capitalize()} RR Count (#)"]:
        if c in view.columns:
            view[c] = pd.to_numeric(view[c], errors="coerce").round(0).astype("Int64").astype(str).replace("<NA>","")
    if "IPC 3+ (%)" in view.columns:
        def pct01(v):
            try:
                v=float(v)
                return "" if v<=0 or np.isnan(v) else f"{int(round(v*100))}%"
            except: return ""
        view["IPC 3+ (%)"] = [pct01(v) for v in view["IPC 3+ (%)"]]

    table_html_simple = view.to_html(index=False)

    return table_html_simple, tier1_df, tier2_df, table_countries, sel.copy(), cols, cirv_col, amt_col, funding_plan_col

# ---------------- STREAMLIT UI ----------------
st.set_page_config(page_title="Recommended 2026 Priorities", layout="wide")
st.title("Recommended 2026 Priorities")

# sidebar filters
shock = st.sidebar.selectbox("Shock", ["cholera","floods","drought","storms"], format_func=str.capitalize, index=0)
st.sidebar.markdown("**Filters (dashboard only)**")
use_exclude_fw = st.sidebar.checkbox("Exclude Active/Dormant/Under Dev", value=True)
use_cirv_med   = st.sidebar.checkbox("CIRV > median (Top-10 by amount)", value=False)
use_hnrp       = st.sidebar.checkbox("Require HNRP status", value=False)
use_food_insec = st.sidebar.checkbox("Require food insecurity (IPC 3+ > 0)", value=False)
use_other_aa   = st.sidebar.checkbox("Require other AA frameworks", value=False)

# session state
if "applied_remove" not in st.session_state: st.session_state.applied_remove = set()
if "applied_adds"   not in st.session_state: st.session_state.applied_adds   = []
if "selected_country" not in st.session_state: st.session_state.selected_country = None

# Build data
try:
    table_html, tier1_df, tier2_df, table_countries, rows_df, cols, cirv_col, amt_col, fplan_col = build_view_and_dashboard(
        shock,
        use_exclude_fw = use_exclude_fw,
        use_cirv_med   = use_cirv_med,
        use_hnrp       = use_hnrp,
        use_food_insec = use_food_insec,
        use_other_aa   = use_other_aa,
        manual_remove_tier1 = sorted(list(st.session_state.applied_remove)) if st.session_state.applied_remove else None,
        manual_add_countries = list(st.session_state.applied_adds) if st.session_state.applied_adds else None,
    )
except Exception as e:
    st.error(str(e))
    st.stop()

# Manual controls (true to original: remove multi, add multi, Save applies)
with st.expander("Manual Tier 1 Edits", expanded=True):
    colA, colB, colC = st.columns([1,1,1])
    with colA:
        rem_choices = tier1_df["Country"].tolist() if not tier1_df.empty else []
        to_remove = st.multiselect("Remove from Tier 1", rem_choices, default=[])
    with colB:
        to_add = st.multiselect("Add to Tier 1 (multi)", options=table_countries, default=[], help="Adds occupy the last Tier 1 slot; prior last cascades to Tier 2.")
    with colC:
        if st.button("Save Changes", type="primary"):
            st.session_state.applied_remove = set(to_remove)
            st.session_state.applied_adds   = list(to_add)
            st.rerun()
        if st.button("Reset Dashboard"):
            st.session_state.applied_remove = set()
            st.session_state.applied_adds   = []
            st.session_state.selected_country = None
            st.rerun()

# ---- Card rendering (with clickable country titles) ----
def yesno(v): return "Yes" if v else "No"

def card_grid(df_part, label):
    st.subheader(label)
    if df_part.empty:
        st.info("No countries meet the selected criteria.")
        return
    rows = [df_part.iloc[i:i+3] for i in range(0, len(df_part), 3)]  # 3 per row
    for chunk in rows:
        cols_row = st.columns(min(3, len(chunk)))
        for col, (_, row) in zip(cols_row, chunk.iterrows()):
            country = str(row["Country"])
            has_fplan = bool(row.get("HasFundingPlan", False))
            has_other = bool(row.get("HasOtherAA", False))
            fw_v = row.get(cols.get("fw"), "")
            fw_str = ""
            if isinstance(fw_v, str):
                fw_str = "" if fw_v.strip().lower() in {"inactive","0","0.0","0.00"} else fw_v
            with col:
                # Country title is a button -> opens details panel
                if st.button(country, key=f"country_{label}_{country}"):
                    st.session_state.selected_country = country
                col1, col2 = st.columns(2)
                with col1:
                    if cirv_col and not pd.isna(row.get(cirv_col)):
                        st.metric("CIRV", f"{int(round(float(row[cirv_col])))}")
                    else:
                        st.metric("CIRV", "—")
                    ufe_amt = row.get(cols.get("ufe_amt"), np.nan)
                    st.metric("UFE Allocations ($)", f"{int(round(float(ufe_amt))):,}" if not pd.isna(ufe_amt) else "—")
                    st.metric("Funding Plan?", yesno(has_fplan))
                with col2:
                    ipc = row.get("IPC3plus")
                    st.metric("IPC 3+ (%)", f"{int(round(float(ipc)*100))}%" if (pd.notna(ipc) and float(ipc)>0) else "—")
                    rr_amt = row.get(cols.get("rr_amt"), np.nan)
                    st.metric("RR Allocations ($)", f"{int(round(float(rr_amt))):,}" if not pd.isna(rr_amt) else "—")
                    st.metric("Other AA?", yesno(has_other))
                st.caption(f"{shock.capitalize()} Framework: {fw_str if fw_str else '—'}")

card_grid(tier1_df, "Tier 1")
card_grid(tier2_df, "Tier 2")

# ---- Details panel (just like notebook; shows hazard — coordinating org) ----
st.subheader("Country Details")
if st.session_state.selected_country:
    c = st.session_state.selected_country
    if c in set(rows_df["Country"]):
        r = rows_df[rows_df["Country"]==c].iloc[0]
        st.markdown(render_country_details(r, cols.get("fw"), cirv_col, amt_col, fplan_col, cols), unsafe_allow_html=True)
    else:
        st.info(f"No details available for **{c}**.")
else:
    st.caption("Click a country title on a Tier card to see details.")

# ---- Table (Funding Plan + IPC 3+ (%) included) ----
st.subheader("Details Table")
st.markdown(
    """
    <style>
      table { font-size: 13px; }
      th, td { padding: 6px 8px; }
    </style>
    """,
    unsafe_allow_html=True
)
st.markdown(table_html, unsafe_allow_html=True)
