#!/usr/bin/env python3
import argparse, os, sys, math, json, re, time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path

import pandas as pd
import requests
from openpyxl import load_workbook
from openpyxl.styles.fills import PatternFill
from roles import roles_dict

# ========= Config & constants =========

ARTIFACTS = Path("artifacts"); ARTIFACTS.mkdir(exist_ok=True)
TIMEOUT = 30

# Track property map (API expects array[int])
TRACK_PROP_MAP = {
    "NONE": 1,
    "REMIX OR DERIVATIVE": 2,
    "SAMPLES OR STOCK": 3,
    "MIX OR COMPILATION": 4,
    "ALTERNATE VERSION": 5,
    "SPECIAL GENRE": 6,
    "NON MUSICAL CONTENT": 7,
    "INCLUDES AI": 8,
}

# Build a name->id role map from roles.py (case-insensitive)
ROLE_FALLBACK = { (str(v).strip().lower()): k for k, v in roles_dict.items() }

# Fallback language & genre maps (used if API lookup fails)
LANGUAGE_FALLBACK = {
    1:"English",2:"Hebrew",3:"French",4:"Afrikaans",5:"Arabic",6:"Bulgarian",8:"Catalan",9:"Croatian",
    10:"Czech",11:"Danish",12:"Dutch",13:"Estonian",14:"Finnish",15:"German",16:"Greek",17:"Hindi",
    18:"Hungarian",19:"Icelandic",20:"Indonesian",21:"Italian",22:"Japanese",23:"Kazakh",24:"Korean",
    25:"Lao",26:"Latvian",27:"Lithuanian",28:"Malay",29:"Norwegian",30:"Polish",31:"Portuguese",
    32:"Romanian",34:"Russian",35:"Slovak",36:"Slovenian",37:"Spanish",38:"Swedish",39:"Tagalog",
    40:"Tamil",41:"Telugu",42:"Thai",43:"Turkish",44:"Ukrainian",45:"Urdu",46:"Vietnamese",47:"Zulu",
    48:"Instrumental",49:"Chinese Simplified",50:"Chinese Traditional",52:"Cantonese",53:"Bengali",
    54:"Haitian",55:"Irish",56:"Latin",57:"Persian",58:"Punjabi",59:"Sanskrit",60:"Spanish (Latin America)",
    61:"Amharic",62:"Oromo",63:"Tigrinya",66:"Abkhazian",67:"Afar",68:"Akan",69:"Albanian",70:"Aragonese",
    71:"Armenian",72:"Assamese",73:"Avaric",74:"Avestan",75:"Aymara",76:"Azerbaijani",77:"Bambara",78:"Bashkir",
    79:"Basque",80:"Belarusian",81:"Bihari languages",82:"Bislama",83:"Bosnian",84:"Breton",85:"Burmese",
    86:"Chamorro",87:"Chechen",88:"Chichewa",89:"Chuvash",90:"Cornish",91:"Corsican",92:"Cree",93:"Divehi",
    94:"Dzongkha",95:"Esperanto",96:"Ewe",97:"Faroese",98:"Fijian",99:"Fulah",100:"Galician",101:"Georgian",
    102:"Guarani",103:"Gujarati",104:"Hausa",105:"Herero",106:"Hiri Motu",107:"Interlingua",108:"Interlingue",
    109:"Igbo",110:"Inupiaq",111:"Ido",112:"Inuktitut",113:"Javanese",114:"Kalaallisut",115:"Kannada",116:"Kanuri",
    117:"Kashmiri",118:"Central Khmer",119:"Kikuyu",120:"Kinyarwanda",121:"Kirghiz",122:"Komi",123:"Kongo",
    124:"Kurdish",125:"Kuanyama",126:"Luxembourgish",127:"Ganda",128:"Limburgan",129:"Lingala",130:"Luba-Katanga",
    131:"Manx",132:"Macedonian",133:"Malagasy",134:"Malayalam",135:"Maltese",136:"Maori",137:"Marathi",
    138:"Marshallese",139:"Mongolian",140:"Nauru",141:"Navajo",142:"North Ndebele",143:"Nepali",144:"Ndonga",
    145:"Norwegian Bokmål",146:"Norwegian Nynorsk",147:"Sichuan Yi",148:"South Ndebele",149:"Occitan",150:"Ojibwa",
    151:"Church Slavic",152:"Oromo",153:"Oriya",154:"Ossetian",155:"Pali",156:"Pashto",157:"Quechua",158:"Romansh",
    159:"Rundi",160:"Sardinian",161:"Sindhi",162:"Northern Sami",163:"Samoan",164:"Sango",165:"Serbian",
    166:"Gaelic",167:"Shona",168:"Sinhala",169:"Somali",170:"Southern Sotho",171:"Sundanese",172:"Swahili",
    173:"Swati",174:"Tajik",175:"Tibetan",176:"Turkmen",177:"Tswana",178:"Tonga",179:"Tsonga",180:"Tatar",
    181:"Twi",182:"Tahitian",183:"Uighur",184:"Uzbek",185:"Venda",186:"Volapük",187:"Walloon",188:"Welsh",
    189:"Wolof",190:"Western Frisian",191:"Xhosa",192:"Yiddish",193:"Yoruba",194:"Zhuang",195:"Bhojpuri",
    196:"Haryanvi",197:"Konkani",198:"Rajasthani",199:"Bhojpuri",200:"Haryanvi",201:"Konkani",202:"Rajasthani",
}
# inverted name->id
LANG_NAME_TO_ID_FALLBACK = {v.lower(): k for k,v in LANGUAGE_FALLBACK.items()}

MUSICSTYLE_FALLBACK = {
    10:"Pop",11:"Rock",12:"Electronic",13:"Reggae",14:"Singer/Songwriter",15:"World",16:"Dance",
    17:"Salsa y Tropical",18:"Latin",19:"New Age",20:"Holiday",21:"Arabic",22:"Jazz",23:"Children's Music",
    24:"R&B/Soul",25:"Alternative",26:"Anime",28:"Blues",29:"Brazilian",30:"Chinese",31:"Christian & Gospel",
    32:"Classical",33:"Comedy",34:"Country",35:"Folk",37:"Fitness & Workout",38:"French Pop",39:"German Folk",
    40:"German Pop",41:"Hip Hop/Rap",43:"Indian",45:"J-Pop",46:"K-Pop",47:"Karaoke",48:"Korean",49:"Opera",
    52:"Soundtrack",53:"Vocal",54:"Disney",55:"Easy Listening",56:"Inspirational",57:"Instrumental",
    58:"Marching Bands",59:"Spoken Word",60:"College Rock",61:"Goth Rock",62:"Grunge",63:"Indie Rock",
    64:"New Wave",65:"Punk",
    # (trimmed for brevity; you can paste the full list here if you prefer strict offline fallback)
}
MUSICSTYLE_NAME_TO_ID_FALLBACK = {v.lower(): k for k,v in MUSICSTYLE_FALLBACK.items()}

# ========= Helpers =========

class Progress:
    """Lightweight step tracker for transparent progress & debugging.
    Usage:
        progress = Progress()
        with progress.step("Read sheets") as s:
            # ... work ...
            s.info(sheets=9)
        progress.write_log()
    """
    def __init__(self):
        self.records: List[Dict[str, Any]] = []

    @contextmanager
    def step(self, name: str):
        start = time.time()
        rec: Dict[str, Any] = {"name": name, "status": "running", "start_ts": start}
        print(f"[STEP] → {name}")
        class StepCtx:
            def __init__(self, rec: Dict[str, Any]):
                self._rec = rec
            def info(self, **kwargs):
                self._rec.setdefault("meta", {}).update(kwargs)
                if kwargs:
                    kv = ", ".join(f"{k}={v}" for k,v in kwargs.items())
                    print(f"[INFO] {name}: {kv}")
        ctx = StepCtx(rec)
        try:
            yield ctx
            rec["status"] = "ok"
        except Exception as e:
            rec["status"] = "error"
            rec["error"] = str(e)
            raise
        finally:
            rec["duration_sec"] = round(time.time() - start, 3)
            self.records.append(rec)
            print(f"[STEP] ✓ {name} → {rec['status']} in {rec['duration_sec']}s")

    def write_log(self, path: Path = ARTIFACTS / "run_log.json"):
        try:
            path.write_text(json.dumps(self.records, indent=2))
            print(f"[LOG] Wrote step log to {path.resolve()}")
        except Exception as e:
            print(f"[WARN] Failed writing run log: {e}")

def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        print(f"[FATAL] Missing env var: {name}")
        sys.exit(2)
    return v

def http(session: requests.Session, method: str, url: str, token: str, json_body=None, params=None, headers=None) -> requests.Response:
    h = {"Authorization": f"Bearer {token}"}
    if headers: h.update(headers)
    resp = session.request(method, url, json=json_body, params=params, headers=h, timeout=TIMEOUT)
    return resp

def fetch_all_labels(session: requests.Session, base_url: str, token: str, headers: Dict[str,str]) -> Dict[str, Dict[str,Any]]:
    out: Dict[str, Dict[str,Any]] = {}
    page = 1; page_size = 100
    while True:
        url = f"{base_url}/content/label/all"
        resp = http(session, "GET", url, token, params={"pageNumber": page, "pageSize": page_size}, headers=headers)
        if not resp.ok:
            break
        data = resp.json() or {}
        items = data.get("items", []) or []
        for it in items:
            name = (it.get("name") or "").strip()
            if name:
                out[name.lower()] = it
        total = data.get("totalItemsCount", 0)
        if page * page_size >= total or not items:
            break
        page += 1
    return out

def find_artist_id(session: requests.Session, base_url: str, token: str, enterpriseId: int, name: str, headers: Dict[str,str]) -> Optional[int]:
    if not name:
        return None
    url = f"{base_url}/api/enterprises/{enterpriseId}/artists"
    resp = http(session, "GET", url, token, params={"name": name, "pageSize": 1}, headers=headers)
    if not resp.ok:
        return None
    data = resp.json() or {}
    items = data.get("items", []) or []
    if not items:
        return None
    for it in items:
        if (it.get("name") or "").strip().lower() == name.strip().lower():
            return it.get("artistId")
    return None

def create_or_reuse_artists(session: requests.Session, base_url: str, token: str, headers: Dict[str,str], enterpriseId: int, artists_payload: List[Dict[str,Any]], http_errors: List[Dict[str,Any]]):
    name_to_id: Dict[str,int] = {}
    created = 0; reused = 0; failed = 0
    # First attempt to resolve existing by name, then create missing
    for it in artists_payload:
        name = (it.get("name") or "").strip()
        if not name:
            continue
        existing = find_artist_id(session, base_url, token, enterpriseId, name, headers)
        if existing:
            name_to_id[name.lower()] = int(existing)
            reused += 1
            continue
        endpoint = f"{base_url}/artists"
        resp = http(session, "POST", endpoint, token, json_body=it, headers=headers)
        if not resp.ok:
            failed += 1
            http_errors.append({
                "when": "create_artist",
                "endpoint": endpoint,
                "status": resp.status_code,
                "request": it,
                "response": (resp.text or "")[:1500]
            })
        else:
            try:
                aid = int((resp.json() or {}).get("artistId"))
                name_to_id[name.lower()] = aid
                created += 1
            except Exception:
                created += 1
    return name_to_id, created, reused, failed

def create_or_reuse_labels(session: requests.Session, base_url: str, token: str, headers: Dict[str,str], labels_payload: List[Dict[str,Any]], http_errors: List[Dict[str,Any]]):
    existing = fetch_all_labels(session, base_url, token, headers)
    name_to_id: Dict[str,int] = {}
    created = 0; reused = 0; failed = 0
    for it in labels_payload:
        name = (it.get("name") or "").strip()
        if not name:
            continue
        key = name.lower()
        if key in existing:
            name_to_id[key] = int(existing[key].get("labelId"))
            reused += 1
            continue
        endpoint = f"{base_url}/content/label/save"
        resp = http(session, "POST", endpoint, token, json_body=it, headers=headers)
        if not resp.ok:
            failed += 1
            http_errors.append({
                "when": "create_label",
                "endpoint": endpoint,
                "status": resp.status_code,
                "request": it,
                "response": (resp.text or "")[:1500]
            })
        else:
            try:
                lid = int((resp.json() or {}).get("labelId"))
                name_to_id[key] = lid
                created += 1
            except Exception:
                created += 1
    return name_to_id, created, reused, failed

def yes_no(prompt: str) -> bool:
    while True:
        ans = input(f"{prompt} [y/n]: ").strip().lower()
        if ans in ("y","yes"): return True
        if ans in ("n","no"): return False

def is_nan(x): 
    return x is None or (isinstance(x, float) and math.isnan(x)) or (isinstance(x, str) and x.strip()=="")

def norm_bool(x) -> Optional[bool]:
    if x is None: return None
    s = str(x).strip().lower()
    if s in ("1","true","yes","y"): return True
    if s in ("0","false","no","n"): return False
    return None

def norm_int(x) -> Optional[int]:
    if is_nan(x): return None
    try: return int(float(str(x).strip()))
    except: return None

def norm_float(x) -> Optional[float]:
    if is_nan(x): return None
    try: return float(str(x).strip())
    except: return None

def norm_str(x) -> Optional[str]:
    if is_nan(x): return None
    s = str(x)
    # normalize non-breaking spaces and collapse runs of whitespace
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    s = s.strip()
    return s if s != "" else None

def parse_header_and_requirements(xlsx_path: str, sheet_name: str) -> Tuple[List[str], Dict[str,bool], int]:
    """Return (headers, required_map, data_start_row_index) using row 3 as headers and row 4 for notes.
       required_map[col] = True if REQUIRED FIELD (based on text or fill heuristics)."""
    wb = load_workbook(xlsx_path, data_only=True)
    ws = wb[sheet_name]
    header_row = 3
    req_row = 4
    data_start = 5
    headers: List[str] = []
    col_index_map: Dict[int, str] = {}
    for c in range(1, ws.max_column+1):
        val = ws.cell(row=header_row, column=c).value
        head = "" if val is None else str(val).strip()
        headers.append(head)
        col_index_map[c] = head

    required: Dict[str, bool] = {}
    for c in range(1, ws.max_column+1):
        head = col_index_map[c]
        if not head:
            continue
        cell = ws.cell(row=req_row, column=c)
        txt = str(cell.value).strip().upper() if cell.value else ""
        fill: PatternFill = cell.fill
        is_required = False
        if "= REQUIRED FIELD" in txt:
            is_required = True
        elif "= OPTIONAL FIELD" in txt:
            is_required = False
        else:
            fg = getattr(fill, "fgColor", None)
            rgb = getattr(fg, "rgb", None) if fg else None
            if rgb and (rgb.startswith("FFFF00") or rgb.endswith("FF00")):
                is_required = True
        required[head] = is_required

    # stash meta inside req map for logging later
    required["_header_row_value"] = header_row
    required["_data_start_value"] = data_start
    return headers, required, data_start

def df_from_sheet(xlsx_path: str, sheet_name: str) -> Tuple[pd.DataFrame, Dict[str,bool]]:
    headers, req_map, data_start = parse_header_and_requirements(xlsx_path, sheet_name)
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, header=None)
    # build rename map
    rename = {}
    for idx, h in enumerate(headers):
        if h:
            rename[idx] = h
    # Force data to start at row 5 per template (rows 1-4 are fixed header/subheaders)
    effective_start = 5
    df = df.iloc[effective_start-1:, :]  # pandas 1-index vs 0-index care
    df = df.rename(columns=rename)
    # keep only known headers
    keep_cols = [c for c in df.columns if isinstance(c, str) and c in req_map]
    df = df[keep_cols]
    # drop fully empty rows
    df = df.dropna(how="all")
    # stash effective start for logging
    req_map["_effective_data_start"] = effective_start
    return df.reset_index(drop=True), req_map

def require_columns(df: pd.DataFrame, req_map: Dict[str,bool]) -> List[Tuple[int,str]]:
    errs = []
    required_cols = [c for c,req in req_map.items() if req and c in df.columns]
    for i,row in df.iterrows():
        for col in required_cols:
            v = row.get(col, None)
            if is_nan(v):
                errs.append((i+2, f"Missing required '{col}'"))
    return errs

def parse_year_holder(year, holder) -> Optional[str]:
    y = norm_int(year); h = norm_str(holder)
    if y and h: return f"{y} {h}"
    return None

def resolve_language_id(name: Optional[str], session: requests.Session, base_url: str, token: str) -> Optional[int]:
    if not name: return None
    try:
        resp = http(session, "GET", f"{base_url}/common/lookup/languages", token)
        if resp.ok:
            items = resp.json()
            for it in items:
                if it.get("name","").strip().lower() == name.strip().lower():
                    return int(it.get("languageId"))
    except Exception:
        pass
    return LANG_NAME_TO_ID_FALLBACK.get(name.strip().lower())

def resolve_musicstyle_id(name: Optional[str], session: requests.Session, base_url: str, token: str) -> Optional[int]:
    if not name: return None
    try:
        resp = http(session, "GET", f"{base_url}/common/lookup/musicstyles", token)
        if resp.ok:
            items = resp.json()
            for it in items:
                if it.get("name","").strip().lower() == name.strip().lower():
                    return int(it.get("musicStyleId"))
    except Exception:
        pass
    return MUSICSTYLE_NAME_TO_ID_FALLBACK.get(name.strip().lower())

def ingest_image_by_url(url: str, session: requests.Session, base_url: str, token: str) -> Optional[Dict[str,Any]]:
    if not url: return None
    # If Revelator has image pull-by-URL, use it; else, just store the URL in dry-run.
    # Placeholder: assuming upload by URL-like endpoint isn’t public; we keep URL in dry-run and return mock structure.
    return {"fileId": None, "filename": os.path.basename(url), "sourceUrl": url}

def ingest_audio_by_url(url: str, filetype: str, session: requests.Session, base_url: str, token: str, live: bool) -> Optional[Dict[str,Any]]:
    if not url: return None
    fmt = (filetype or "").strip().upper()
    fileFormat = {"WAV":1, "FLAC":2, "MP3":3}.get(fmt)
    # If a pull-external audio endpoint exists, call it here. Otherwise, dry-run structure with sourceUrl for diagnostics:
    return {"audioId": None, "audioFilename": os.path.basename(url), "fileFormat": fileFormat, "sourceUrl": url}

def extract_spotify_artist_id(val: Optional[str]) -> Optional[str]:
    """Return the canonical Spotify artist ID from a URI or URL.
    Examples:
      'spotify:artist:1Gnh4...' -> '1Gnh4...'
      'https://open.spotify.com/artist/1Gnh4...?si=...' -> '1Gnh4...'
      '1Gnh4...' -> '1Gnh4...'
    """
    if not val:
        return None
    s = str(val).strip()
    if s.startswith("spotify:"):
        parts = s.split(":")
        return parts[-1] or None
    low = s.lower()
    if "open.spotify.com/artist/" in low:
        try:
            tail = s.split("/artist/")[1]
            tail = tail.split("?")[0]
            tail = tail.split("/")[0]
            return tail or None
        except Exception:
            return None
    return s or None

def normalize_audio_url(url: Optional[str]) -> Optional[str]:
    """Normalize known share URLs to direct-download when possible (e.g., Dropbox dl=1)."""
    s = norm_str(url)
    if not s:
        return None
    try:
        low = s.lower()
        # Dropbox: ensure dl=1 for direct download
        if "dropbox.com/" in low:
            if "?" in s:
                base, qs = s.split("?", 1)
                # preserve existing params but force dl=1
                params = []
                seen_dl = False
                for part in qs.split("&"):
                    if part.startswith("dl="):
                        params.append("dl=1"); seen_dl = True
                    else:
                        params.append(part)
                if not seen_dl:
                    params.append("dl=1")
                s = base + "?" + "&".join(params)
            else:
                s = s + "?dl=1"
        return s
    except Exception:
        return url

def map_track_properties(row: Dict[str,Any]) -> Optional[List[int]]:
    # Normalize incoming row keys: collapse whitespace/newlines, uppercase
    def norm_key(k: str) -> str:
        return COLSPACE_RE.sub(" ", str(k or "").strip()).upper()

    row_norm = {norm_key(k): v for k, v in row.items()}

    labels = [
        "REMIX OR DERIVATIVE","SAMPLES OR STOCK","MIX OR COMPILATION","ALTERNATE VERSION",
        "SPECIAL GENRE","NON MUSICAL CONTENT","INCLUDES AI","NONE APPLY","NONE"
    ]
    set_ids: List[int] = []
    any_true = False
    for lab in labels:
        v = row_norm.get(norm_key(lab))
        v = norm_bool(v)
        if v:
            any_true = True
            key = lab.upper()
            if key in ("NONE APPLY", "NONE"):
                set_ids = [1]  # exclusive
                break
            # add mapped (TRACK_PROP_MAP keys are already normalized)
            set_ids.append(TRACK_PROP_MAP[key if key != "NONE APPLY" else "NONE"])
    if not any_true:
        return None
    # dedupe & sort
    return sorted(set(set_ids))

# Column name normalization (case + whitespace resilient)
COLSPACE_RE = re.compile(r"[\s_]+")
def norm_colkey(s: str) -> str:
    return COLSPACE_RE.sub(" ", (s or "").strip()).lower()

# Build a map of normalized column names to real names
def make_colmap(df: pd.DataFrame) -> Dict[str,str]:
    return {norm_colkey(c): c for c in df.columns if isinstance(c, str)}

def has_col(df: pd.DataFrame, *names: str) -> bool:
    cmap = make_colmap(df)
    return any(norm_colkey(n) in cmap for n in names)

def get_val(row: pd.Series, cmap: Dict[str,str], *names: str):
    for n in names:
        col = cmap.get(norm_colkey(n))
        if col is not None:
            return row.get(col)
    return None

# Resolve a column by trying exact normalized name then token-based partial match
TOK_RE = re.compile(r"[a-z0-9]+")
def _tokens(s: str) -> List[str]:
    return TOK_RE.findall(norm_colkey(s))

def resolve_colkey(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    if df is None or df.empty:
        return None
    cmap = make_colmap(df)
    # Exact first
    for n in candidates:
        col = cmap.get(norm_colkey(n))
        if col is not None:
            return col
    # Token containment fallback
    cand_tokens = [(n, set(_tokens(n))) for n in candidates]
    best: Tuple[float, Optional[str]] = (0.0, None)
    for norm_name, real in cmap.items():
        real_tokens = set(_tokens(real))
        for name, toks in cand_tokens:
            if not toks:
                continue
            # score: fraction of target tokens present in real col
            inter = len(toks & real_tokens)
            score = inter / len(toks)
            if score == 1.0:  # all tokens present
                # prefer the candidate with most tokens matched (tie by real length)
                weight = (len(toks), len(real_tokens))
                # encode into a float while preserving ordering; simpler keep best by score then token count
                if score > best[0] or (score == best[0] and len(toks) > 0):
                    best = (score, real)
    return best[1]

def first_nonempty(df: pd.DataFrame, col: Optional[str]) -> Optional[str]:
    if not col or col not in df.columns:
        return None
    for v in df[col].tolist():
        nv = norm_str(v)
        if nv is not None:
            return nv
    return None

def resolve_and_peek(df: pd.DataFrame, *candidates: str) -> Tuple[Optional[str], Optional[str]]:
    col = resolve_colkey(df, *candidates)
    return col, first_nonempty(df, col)

# ========= Main pipeline =========

def main():
    parser = argparse.ArgumentParser(description="Catalog spreadsheet parser (dry-run first).")
    parser.add_argument("xlsx", help="Path to the XLSX")
    parser.add_argument("--base-url", default=os.getenv("REVELATOR_BASE_URL", "https://api.revelator.com"))
    parser.add_argument("--token", default=os.getenv("REVELATOR_TOKEN", ""))
    parser.add_argument("--live", action="store_true", help="Execute HTTP calls (otherwise dry-run)")
    parser.add_argument("--role-map", default="roles.json", help="JSON file with RoleName->roleId mapping (optional).")
    args = parser.parse_args()

    token = args.token or getenv_required("REVELATOR_TOKEN")
    base_url = args.base_url.rstrip("/")

    # ---- Enterprise/Tenant prompt & validation
    print("Before we start, please provide target account identifiers.")
    ent = input("EnterpriseId: ").strip()
    ten = input("TenantId: ").strip()
    if not ent.isdigit() or not ten.isdigit():
        print("[FATAL] EnterpriseId and TenantId must be integers.")
        sys.exit(2)
    enterpriseId = int(ent); tenantId = int(ten)

    with requests.Session() as session:
        progress = Progress()

        # Validate enterprise
        with progress.step("Validate enterprise") as s:
            r = http(session, "GET", f"{base_url}/enterprise/clients/{enterpriseId}", token)
            if not r.ok:
                print(f"[FATAL] Enterprise check failed ({r.status_code}): {r.text[:500]}")
                sys.exit(2)
            ent_info = r.json()
            name = ent_info.get("name","?")
            s.info(enterpriseId=enterpriseId, tenantId=tenantId, enterpriseName=name)
            print(f"Resolved EnterpriseId={enterpriseId} → name='{name}'")
            if not yes_no("Proceed ingesting catalog for this enterprise?"):
                print("Aborting as requested.")
                sys.exit(0)

        # Load role map
        with progress.step("Load role map") as s:
            # ROLE_FALLBACK is name->id from roles.py
            role_map = ROLE_FALLBACK.copy()
            if Path(args.role_map).exists():
                try:
                    file_map = json.loads(Path(args.role_map).read_text())
                    if isinstance(file_map, dict):
                        # Accept both name->id and id->name; normalize to name->id
                        normalized: Dict[str,int] = {}
                        for k,v in file_map.items():
                            if isinstance(k, str) and isinstance(v, int):
                                nm = (norm_str(k) or "").lower()
                                if nm: normalized[nm] = v
                            elif isinstance(k, (int, float)) and isinstance(v, str):
                                nm = (norm_str(v) or "").lower()
                                if nm: normalized[nm] = int(k)
                        role_map.update(normalized)
                except Exception:
                    print("[WARN] Could not parse roles.json, using fallback map only.")
            s.info(roles=len(role_map))

        # ===== Read sheets
        xlsx_path = args.xlsx

        s1 = "1) Artists list"
        s2 = "2) Labels list"
        s3 = "3) Release_Label"
        s4 = "4) Release_Artist(s)"
        s5 = "5) Release_Track"
        s6 = "6) Track_Artist(s)"
        s7 = "7) Comp ContributorPublisher li"
        s8 = "8) Track_Composition(s)"
        s9 = "9) Audio_Properties"

        with progress.step("Read sheets") as s:
            try:
                df_art, req_art = df_from_sheet(xlsx_path, s1)
                df_lab, req_lab = df_from_sheet(xlsx_path, s2)
                df_rel, req_rel = df_from_sheet(xlsx_path, s3)
                df_relart, req_relart = df_from_sheet(xlsx_path, s4)
                df_reltrk, req_reltrk = df_from_sheet(xlsx_path, s5)
                df_trkart, req_trkart = df_from_sheet(xlsx_path, s6)
                df_comp_masters, req_comp_masters = df_from_sheet(xlsx_path, s7)
                df_trkcomp, req_trkcomp = df_from_sheet(xlsx_path, s8)
                df_props, req_props = df_from_sheet(xlsx_path, s9)
            except KeyError as e:
                print(f"[FATAL] Sheet not found: {e}")
                raise
            # capture header/data start from req maps
            def meta(req):
                return {
                    "header_row": req.get("_header_row_value"),
                    "data_start": req.get("_data_start_value"),
                    "effective_start": 5,
                }
            # Column peeks: show the first data point found under key columns
            art_name_col, art_name_first = resolve_and_peek(df_art, "Artist Name", "ARTIST NAME", "Artist")
            lab_name_col, lab_name_first = resolve_and_peek(df_lab, "Label Name", "LABEL NAME", "Label")
            upc_rel_col, upc_rel_first = resolve_and_peek(df_rel, "UPC / EAN / JAN")
            upc_reltrk_col, upc_reltrk_first = resolve_and_peek(df_reltrk, "UPC / EAN / JAN")
            isrc_reltrk_col, isrc_reltrk_first = resolve_and_peek(df_reltrk, "ISRC/vISRC")
            isrc_trkart_col, isrc_trkart_first = resolve_and_peek(df_trkart, "ISRC/vISRC")
            artist_trkart_col, artist_trkart_first = resolve_and_peek(df_trkart, "ARTIST")
            role_trkart_col, role_trkart_first = resolve_and_peek(df_trkart, "ARTIST ROLE")
            isrc_trkcomp_col, isrc_trkcomp_first = resolve_and_peek(df_trkcomp, "ISRC/vISRC")
            comp_trkcomp_col, comp_trkcomp_first = resolve_and_peek(df_trkcomp, "COMPOSITION CONTRIBUTOR")
            share_trkcomp_col, share_trkcomp_first = resolve_and_peek(df_trkcomp, "SHARE%")
            isrc_props_col, isrc_props_first = resolve_and_peek(df_props, "ISRC/vISRC")
            s.info(
                artists=len(df_art), labels=len(df_lab), releases=len(df_rel), rel_artists=len(df_relart), rel_tracks=len(df_reltrk), track_artists=len(df_trkart), comps=len(df_trkcomp), props=len(df_props),
                artists_meta=meta(req_art), labels_meta=meta(req_lab), releases_meta=meta(req_rel),
                peek={
                    "artists": {"col": art_name_col, "first": art_name_first},
                    "labels": {"col": lab_name_col, "first": lab_name_first},
                    "releases": {"upc_col": upc_rel_col, "first_upc": upc_rel_first},
                    "rel_tracks": {"upc_col": upc_reltrk_col, "first_upc": upc_reltrk_first, "isrc_col": isrc_reltrk_col, "first_isrc": isrc_reltrk_first},
                    "track_artists": {"isrc_col": isrc_trkart_col, "first_isrc": isrc_trkart_first, "artist_col": artist_trkart_col, "first_artist": artist_trkart_first, "role_col": role_trkart_col, "first_role": role_trkart_first},
                    "track_comps": {"isrc_col": isrc_trkcomp_col, "first_isrc": isrc_trkcomp_first, "comp_col": comp_trkcomp_col, "first_comp": comp_trkcomp_first, "share_col": share_trkcomp_col, "first_share": share_trkcomp_first},
                    "props": {"isrc_col": isrc_props_col, "first_isrc": isrc_props_first}
                }
            )

            # Dump headers snapshot for debugging
            headers_snapshot = {
                s1: {"raw": list(df_art.columns), "norm": [norm_colkey(c) for c in df_art.columns]},
                s2: {"raw": list(df_lab.columns), "norm": [norm_colkey(c) for c in df_lab.columns]},
                s3: {"raw": list(df_rel.columns), "norm": [norm_colkey(c) for c in df_rel.columns]},
                s4: {"raw": list(df_relart.columns), "norm": [norm_colkey(c) for c in df_relart.columns]},
                s5: {"raw": list(df_reltrk.columns), "norm": [norm_colkey(c) for c in df_reltrk.columns]},
                s6: {"raw": list(df_trkart.columns), "norm": [norm_colkey(c) for c in df_trkart.columns]},
                s7: {"raw": list(df_comp_masters.columns), "norm": [norm_colkey(c) for c in df_comp_masters.columns]},
                s8: {"raw": list(df_trkcomp.columns), "norm": [norm_colkey(c) for c in df_trkcomp.columns]},
                s9: {"raw": list(df_props.columns), "norm": [norm_colkey(c) for c in df_props.columns]},
            }
            (ARTIFACTS/"headers.json").write_text(json.dumps(headers_snapshot, indent=2, ensure_ascii=False))

        # ===== Preflight validations
        report: List[Dict[str,Any]] = []

        # Required-field checks (yellow)
        for name, df, req in [
            (s1, df_art, req_art),
            (s2, df_lab, req_lab),
            (s3, df_rel, req_rel),
            (s4, df_relart, req_relart),
            (s5, df_reltrk, req_reltrk),
            (s6, df_trkart, req_trkart),
            (s7, df_comp_masters, req_comp_masters),
            (s8, df_trkcomp, req_trkcomp),
            (s9, df_props, req_props),
        ]:
            errs = require_columns(df, req)
            for rownum, msg in errs:
                report.append({"sheet": name, "row": rownum, "error": msg})

        # Cross-tab keys & integrity
        # Releases must have UPC / EAN / JAN (join key), Tracks must have ISRC/vISRC
        def expect_col(df, name):
            return has_col(df, name)

        UPC_COL = "UPC / EAN / JAN"
        ISRC_COL = "ISRC/vISRC"

        if expect_col(df_rel, UPC_COL):
            cm_rel = make_colmap(df_rel)
            for i, rw in df_rel.iterrows():
                if is_nan(get_val(rw, cm_rel, UPC_COL)):
                    report.append({"sheet": s3, "row": i+2, "error": "Missing release key 'UPC / EAN / JAN'"})

        if expect_col(df_reltrk, UPC_COL) and expect_col(df_reltrk, ISRC_COL):
            cm_reltrk = make_colmap(df_reltrk)
            # also check track order is present
            for i, rw in df_reltrk.iterrows():
                if is_nan(get_val(rw, cm_reltrk, UPC_COL)):
                    report.append({"sheet": s5, "row": i+2, "error": "Release_Track missing UPC to join Release"})
                if is_nan(get_val(rw, cm_reltrk, ISRC_COL)):
                    report.append({"sheet": s5, "row": i+2, "error": "Release_Track missing ISRC/vISRC"})

        if expect_col(df_trkcomp, ISRC_COL) and has_col(df_trkcomp, "SHARE%"):
            # shares sum to 100 by (ISRC) or 1.0 when decimal representation is used
            cm_trkcomp = make_colmap(df_trkcomp)
            by_isrc: Dict[str, float] = {}
            for i, rw in df_trkcomp.iterrows():
                isrc = norm_str(get_val(rw, cm_trkcomp, ISRC_COL))
                share_s = norm_str(get_val(rw, cm_trkcomp, "SHARE%"))
                share = None
                try:
                    share = float(share_s) if share_s is not None else None
                except Exception:
                    pass
                if isrc and share is not None:
                    by_isrc[isrc] = by_isrc.get(isrc, 0.0) + share
            for isrc, total in by_isrc.items():
                # Accept either 100-based or decimal-based totals (1.0 == 100%)
                tol = 1e-3
                ok_100 = abs(total - 100.0) <= tol
                ok_1 = abs(total - 1.0) <= tol
                if not (ok_100 or ok_1):
                    report.append({"sheet": s8, "row": "-", "error": f"Composition shares for ISRC {isrc} sum to {total}, expected ~100 or ~1.0"})

        # Property conflicts
        if expect_col(df_props, ISRC_COL):
            cm_props = make_colmap(df_props)
            for i, rw in df_props.iterrows():
                arr = map_track_properties(rw.to_dict())
                if arr and 1 in arr and len(arr) > 1:
                    report.append({"sheet": s9, "row": i+2, "error": "Track properties: 'None' cannot be combined with other flags"})

        # Stop if any blocking issues
        with progress.step("Preflight validations") as s:
            s.info(issues=len(report))
            if report:
                out = ARTIFACTS / "preflight_report.json"
                out.write_text(json.dumps(report, indent=2))
                print(f"[BLOCKED] Preflight failed with {len(report)} issue(s). See {out.resolve()}")
                progress.write_log()
                sys.exit(1)

        # ===== Build master maps (Artists, Labels, Composers, Publishers)
        # Artists
        with progress.step("Build artists & labels & master entities") as s:
            cm_art = make_colmap(df_art)
            # Try robust resolution of the artist name column with several aliases
            art_name_col = resolve_colkey(
                df_art,
                "Artist Name", "ARTIST NAME", "Artist",
                "Artist Full Name", "ArtistName", "Name"
            )
            # Ultimate fallback: choose the first column whose header contains 'artist' and 'name' tokens
            if not art_name_col:
                for c in df_art.columns:
                    if isinstance(c, str):
                        key = norm_colkey(c)
                        if "artist" in key and ("name" in key or key.endswith("artist")):
                            art_name_col = c
                            break
            if not art_name_col:
                print("[WARN] Could not resolve 'Artist Name' column; artist list may be empty.")
            artists_payload = []
            artist_name_to_obj = {}
            dropped_art_missing_name = 0
            for i,rw in df_art.iterrows():
                # fallback: use resolved column if present
                name = norm_str(rw.get(art_name_col)) if art_name_col else norm_str(get_val(rw, cm_art, "Artist Name", "ARTIST NAME", "Artist"))
                if not name:
                    dropped_art_missing_name += 1
                    continue
                img_url = norm_str(get_val(rw, cm_art, "Artist Image url", "Artist Image URL"))
                apple = norm_str(get_val(rw, cm_art, "Apple ArtistId"))
                spotify = norm_str(get_val(rw, cm_art, "Spotify Artist URI"))
                spotify_id = extract_spotify_artist_id(spotify) if spotify else None
                meta = norm_str(get_val(rw, cm_art, "Meta ArtistId"))
                sc = norm_str(get_val(rw, cm_art, "SoundCloud ProfileId", "SoundCloud Profile ID"))
                ext = []
                if apple: ext.append({"distributorStoreId":1, "profileId":apple})
                if spotify_id: ext.append({"distributorStoreId":9, "profileId":spotify_id})
                if sc: ext.append({"distributorStoreId":68, "profileId":sc})
                if meta: ext.append({"distributorStoreId":309, "profileId":meta})
                img = ingest_image_by_url(img_url, session, base_url, token) if img_url else None
                payload = {"name": name}
                if ext: payload["artistExternalIds"] = ext
                if img: payload["image"] = {"fileId": img["fileId"], "filename": img["filename"]}
                artists_payload.append(payload)
                artist_name_to_obj[name.lower()] = payload

        # Labels
            cm_lab = make_colmap(df_lab)
            lab_name_col = resolve_colkey(df_lab, "Label Name", "LABEL NAME", "Label")
            labels_payload = []
            label_name_to_id = {}
            dropped_lab_missing_name = 0
            for i,rw in df_lab.iterrows():
                lname = norm_str(rw.get(lab_name_col)) if lab_name_col else norm_str(get_val(rw, cm_lab, "Label Name", "LABEL NAME", "Label"))
                if not lname:
                    dropped_lab_missing_name += 1
                    continue
                labels_payload.append({"name": lname})

        # Publishers & Composers
            cm_cm = make_colmap(df_comp_masters)
            pub_col = resolve_colkey(df_comp_masters, "Publisher Name", "PUBLISHER NAME", "Publisher")
            comp_col = resolve_colkey(df_comp_masters, "Composition Contributor", "COMPOSITION CONTRIBUTOR", "Contributor")
            publishers_payload = []
            composers_payload = []
            pub_names = set(); comp_names = set()
            if pub_col:
                for _, rw in df_comp_masters.iterrows():
                    pn = norm_str(rw.get(pub_col))
                    if pn and pn.lower() not in pub_names:
                        publishers_payload.append({"name": pn}); pub_names.add(pn.lower())
            if comp_col:
                for _, rw in df_comp_masters.iterrows():
                    cn = norm_str(rw.get(comp_col))
                    if cn and cn.lower() not in comp_names:
                        composers_payload.append({"name": cn}); comp_names.add(cn.lower())
            # Include small samples in the log for quick visibility
            art_samples = []
            if art_name_col:
                for i in range(min(3, len(df_art))):
                    art_samples.append(norm_str(df_art.iloc[i].get(art_name_col)))
            lab_samples = []
            if lab_name_col:
                for i in range(min(3, len(df_lab))):
                    lab_samples.append(norm_str(df_lab.iloc[i].get(lab_name_col)))
            pub_samples = []
            if pub_col:
                for i in range(min(3, len(df_comp_masters))):
                    pub_samples.append(norm_str(df_comp_masters.iloc[i].get(pub_col)))
            comp_samples = []
            if comp_col:
                for i in range(min(3, len(df_comp_masters))):
                    comp_samples.append(norm_str(df_comp_masters.iloc[i].get(comp_col)))
            s.info(
                artists=len(artists_payload), labels=len(labels_payload), publishers=len(publishers_payload), composers=len(composers_payload),
                artists_seen=len(df_art), labels_seen=len(df_lab), dropped_art_missing_name=dropped_art_missing_name, dropped_lab_missing_name=dropped_lab_missing_name,
                artist_name_col=art_name_col, label_name_col=lab_name_col, publisher_col=pub_col, composer_col=comp_col,
                artist_samples=art_samples, label_samples=lab_samples, publisher_samples=pub_samples, composer_samples=comp_samples
            )

        # ===== Releases & Tracks
        with progress.step("Build releases") as s:
            cm_rel = make_colmap(df_rel)
            releases_payload = []
            tracks_payload = []  # list of (release_key, payload)
            upc_dupes_logged = []

            for i,rw in df_rel.iterrows():
                upc = norm_str(get_val(rw, cm_rel, UPC_COL))
                title = norm_str(get_val(rw, cm_rel, "RELEASE TITLE"))
                version = norm_str(get_val(rw, cm_rel, "RELEASE VERSION"))
                title_lang = norm_str(get_val(rw, cm_rel, "TITLE LANGUAGE"))
                img_url = norm_str(get_val(rw, cm_rel, "COVER IMAGE URL", "COVER IMAGE url"))
                p_year = rw.get("(P) Copyright Year"); p_holder = rw.get("(P) Copyright Holder")
                c_year = rw.get("(C) Copyright Year"); c_holder = rw.get("(C) Copyright Holder")
                p_line = parse_year_holder(p_year, p_holder)
                c_line = parse_year_holder(c_year, c_holder)
                g1 = norm_str(get_val(rw, cm_rel, "GENRE 1")); g2 = norm_str(get_val(rw, cm_rel, "GENRE 2"))
                label_name = norm_str(get_val(rw, cm_rel, "LABEL", "Label Name", "LABEL NAME"))

                lang_id = resolve_language_id(title_lang, session, base_url, token)
                g1_id = resolve_musicstyle_id(g1, session, base_url, token)
                g2_id = resolve_musicstyle_id(g2, session, base_url, token)

                img = ingest_image_by_url(img_url, session, base_url, token) if img_url else None
                rel = {
                    "name": title, "version": version,
                    "previouslyReleased": bool(norm_str(get_val(rw, cm_rel, "ORIGINAL RELEASE DATE", "ORIGINAL\nRELEASE DATE"))),
                    "releaseDate": norm_str(get_val(rw, cm_rel, "ORIGINAL RELEASE DATE", "ORIGINAL\nRELEASE DATE")),
                }
                if upc: rel["upc"] = upc
                if p_line: rel["copyrightP"] = p_line
                if c_line: rel["copyrightC"] = c_line
                if lang_id: rel.setdefault("releaseLocals", []).append({"languageId": lang_id, "name": title})
                if g1_id: rel["primaryMusicStyleId"] = g1_id
                if g2_id: rel["secondaryMusicStyleId"] = g2_id
                if label_name:
                    rel["hasRecordLabel"] = True
                    rel["labelName"] = label_name
                # Only attach image if we have a valid fileId; otherwise, skip to avoid 400s on null GUID
                if img and img.get("fileId"):
                    rel["image"] = {"fileId": img["fileId"], "filename": img["filename"]}
                releases_payload.append(rel)
            sample_releases = []
            for i in range(min(3, len(releases_payload))):
                rr = releases_payload[i]
                sample_releases.append({k: rr.get(k) for k in ("name","version","upc","labelName")})
            s.info(releases=len(releases_payload), sample_releases=sample_releases)

        # Release contributors
        with progress.step("Parse release contributors") as s:
            cm_relart = make_colmap(df_relart)
            release_contribs_by_upc: Dict[str,List[Dict[str,Any]]] = {}
            release_primary_artist_by_upc: Dict[str,str] = {}
            if has_col(df_relart, UPC_COL) and has_col(df_relart, "ARTIST") and has_col(df_relart, "ARTIST ROLE"):
                for _,rw in df_relart.iterrows():
                    upc = norm_str(get_val(rw, cm_relart, UPC_COL))
                    artist = norm_str(get_val(rw, cm_relart, "ARTIST"))
                    role = norm_str(get_val(rw, cm_relart, "ARTIST ROLE"))
                    if not upc or not artist or not role:
                        continue
                    role_norm = (role or '').strip().lower()
                    # Special case: 'Main Primary Artist' sets release.artistName, not a contributor
                    if role_norm == "main primary artist":
                        release_primary_artist_by_upc.setdefault(upc, artist)
                        continue
                    rid = role_map.get(role_norm, None)
                    if rid is None:
                        print(f"[WARN] Unknown role '{role}' for release UPC {upc}")
                        continue
                    release_contribs_by_upc.setdefault(upc, []).append({
                        "artistName": artist, "roleId": rid
                    })
            total = sum(len(v) for v in release_contribs_by_upc.values())
            sample_rel_contribs = []
            for upc, arr in list(release_contribs_by_upc.items())[:2]:
                sample_rel_contribs.append({"upc": upc, "first": arr[0] if arr else None})
            sample_rel_primary = []
            for upc, name in list(release_primary_artist_by_upc.items())[:2]:
                sample_rel_primary.append({"upc": upc, "artistName": name})
            s.info(contributors=total, primaries=len(release_primary_artist_by_upc), sample=sample_rel_contribs, sample_primary=sample_rel_primary)

        # Tracks (by Release_Track)
        audio_url_map: Dict[str, Optional[str]] = {}
        with progress.step("Build tracks from Release_Track") as s:
            cm_reltrk = make_colmap(df_reltrk)
            audio_url_col = resolve_colkey(df_reltrk, "AUDIO FILE URL", "AUDIO URL", "AUDIO DOWNLOAD URL", "AUDIO FILE", "FILE URL", "AUDIO")
            audio_type_col = resolve_colkey(df_reltrk, "AUDIO TYPE", "FILE TYPE", "AUDIO FORMAT", "FORMAT")
            track_rows = []
            first_audio_url_seen = None
            isrc_to_track: Dict[str, Dict[str,Any]] = {}
            for _,rw in df_reltrk.iterrows():
                upc = norm_str(get_val(rw, cm_reltrk, UPC_COL)); isrc = norm_str(get_val(rw, cm_reltrk, ISRC_COL))
                if not upc or not isrc: continue
                t_title = norm_str(get_val(rw, cm_reltrk, "TRACK TITLE")); t_version = norm_str(get_val(rw, cm_reltrk, "TRACK VERSION"))
                lang = norm_str(get_val(rw, cm_reltrk, "LANGUAGE OF LYRICS", "LANGUAGE"))
                explicit = norm_bool(get_val(rw, cm_reltrk, "EXPLICIT"))
                ttype = norm_str(get_val(rw, cm_reltrk, "TYPE"))
                ttype_id = {"original":1,"cover":2,"public domain":3}.get((ttype or "").strip().lower())
                audio_url_raw = norm_str(rw.get(audio_url_col)) if audio_url_col else norm_str(get_val(rw, cm_reltrk, "AUDIO FILE URL"))
                audio_url = normalize_audio_url(audio_url_raw)
                audio_type = norm_str(rw.get(audio_type_col)) if audio_type_col else norm_str(get_val(rw, cm_reltrk, "AUDIO TYPE"))
                preview = norm_int(get_val(rw, cm_reltrk, "TRACK PREVIEW", "PREVIEW START"))
                trknum = norm_int(get_val(rw, cm_reltrk, "TRACK", "TRACK #", "TRACK NUMBER"))  # track number
                lang_id = resolve_language_id(lang, session, base_url, token)

                audio = ingest_audio_by_url(audio_url, audio_type, session, base_url, token, args.live) if audio_url else None
                if first_audio_url_seen is None and audio_url:
                    first_audio_url_seen = audio_url
                if isrc:
                    audio_url_map[isrc] = audio_url
                track = {
                    "name": t_title,
                    "version": t_version,
                    "languageId": lang_id,
                    "explicit": explicit,
                    "trackType": ttype_id,
                    "trackNumber": trknum,
                    "previewStartSeconds": preview,
                    "trackRecordingVersions": [{
                        "isrc": isrc,
                        # Only attach audioFiles objects when we have an uploaded audioId.
                        # Sending null GUIDs causes 400; external URLs are not accepted directly here.
                        "audioFiles": ([{"audioId": audio["audioId"], "audioFilename": audio["audioFilename"], "fileFormat": audio["fileFormat"]}] if (audio and audio.get("audioId")) else [])
                    }]
                }
                track_rows.append((upc, isrc, track))
                isrc_to_track[isrc] = track
            sample_tracks = []
            for i in range(min(3, len(track_rows))):
                upc,isrc,track = track_rows[i]
                sample_tracks.append({"upc": upc, "isrc": isrc, "name": track.get("name"), "trackNumber": track.get("trackNumber")})
            s.info(tracks=len(track_rows), sample_tracks=sample_tracks, first_audio_url=first_audio_url_seen, audio_url_col=audio_url_col, audio_type_col=audio_type_col)

        # Track contributors
        with progress.step("Parse track contributors") as s:
            cm_trkart = make_colmap(df_trkart)
            track_contribs_by_isrc: Dict[str,List[Dict[str,Any]]] = {}
            track_primary_artist_by_isrc: Dict[str,str] = {}
            if has_col(df_trkart, ISRC_COL) and has_col(df_trkart, "ARTIST") and has_col(df_trkart, "ARTIST ROLE"):
                for _,rw in df_trkart.iterrows():
                    isrc = norm_str(get_val(rw, cm_trkart, ISRC_COL))
                    artist = norm_str(get_val(rw, cm_trkart, "ARTIST"))
                    role = norm_str(get_val(rw, cm_trkart, "ARTIST ROLE"))
                    if not isrc or not artist or not role:
                        continue
                    role_norm = (role or '').strip().lower()
                    if role_norm == "main primary artist":
                        track_primary_artist_by_isrc.setdefault(isrc, artist)
                        continue
                    rid = role_map.get(role_norm, None)
                    if rid is None:
                        print(f"[WARN] Unknown role '{role}' for track ISRC {isrc}")
                        continue
                    track_contribs_by_isrc.setdefault(isrc, []).append({
                        "artistName": artist, "roleId": rid
                    })
            total = sum(len(v) for v in track_contribs_by_isrc.values())
            sample_trk_contribs = []
            for isrc, arr in list(track_contribs_by_isrc.items())[:2]:
                sample_trk_contribs.append({"isrc": isrc, "first": arr[0] if arr else None})
            sample_trk_primary = []
            for isrc, name in list(track_primary_artist_by_isrc.items())[:2]:
                sample_trk_primary.append({"isrc": isrc, "artistName": name})
            s.info(contributors=total, primaries=len(track_primary_artist_by_isrc), sample=sample_trk_contribs, sample_primary=sample_trk_primary)

        # Track compositions
        with progress.step("Parse track compositions") as s:
            cm_trkcomp = make_colmap(df_trkcomp)
            trk_comp_by_isrc: Dict[str,List[Dict[str,Any]]] = {}
            for _,rw in df_trkcomp.iterrows():
                isrc = norm_str(get_val(rw, cm_trkcomp, ISRC_COL)); comp = norm_str(get_val(rw, cm_trkcomp, "COMPOSITION CONTRIBUTOR"))
                role = norm_str(get_val(rw, cm_trkcomp, "ROLE")); share_s = norm_str(get_val(rw, cm_trkcomp, "SHARE%"))
                rights = norm_str(get_val(rw, cm_trkcomp, "PUBLISHING")); publisher = norm_str(get_val(rw, cm_trkcomp, "PUBLISHER"))
                if not isrc or not comp or not role or not share_s: continue
                # share remains string for API, but we validated numerically earlier
                rightsId = None
                if rights:
                    rs = rights.strip().lower()
                    if rs in ("copyright control","self-published","self published","1","yes (self)"): rightsId = 1
                    elif rs in ("published","2","yes (publisher)"): rightsId = 2
                    elif rs in ("public domain","3","no publisher"): rightsId = 3
                entry = {"composerName": comp, "roleName": role, "share": share_s}
                if rightsId: entry["rightsId"] = rightsId
                if rightsId == 2 and publisher:
                    entry["publisherName"] = publisher
                trk_comp_by_isrc.setdefault(isrc, []).append(entry)
            total = sum(len(v) for v in trk_comp_by_isrc.values())
            sample_comps = []
            for isrc, arr in list(trk_comp_by_isrc.items())[:2]:
                sample_comps.append({"isrc": isrc, "first": arr[0] if arr else None})
            s.info(compositions=total, sample=sample_comps)

        # Track properties
        with progress.step("Parse track properties") as s:
            cm_props = make_colmap(df_props)
            props_by_isrc: Dict[str,List[int]] = {}
            for _, rw in df_props.iterrows():
                isrc = norm_str(get_val(rw, cm_props, ISRC_COL))
                if not isrc: 
                    continue
                arr = map_track_properties(rw.to_dict())
                if arr:
                    props_by_isrc[isrc] = arr
            sample_props = []
            for isrc, arr in list(props_by_isrc.items())[:2]:
                sample_props.append({"isrc": isrc, "props": arr})
            s.info(with_properties=len(props_by_isrc), sample=sample_props)

        # Attach contributors/compositions/properties
        with progress.step("Attach track contributors/compositions/properties") as s:
            for idx,(upc,isrc,track) in enumerate(track_rows):
                # Contributors
                if isrc in track_contribs_by_isrc:
                    applied = []
                    for c in track_contribs_by_isrc[isrc]:
                        applied.append({"roleId": c["roleId"], "artist": {"name": c["artistName"]}})
                    if applied:
                        track["contributors"] = applied
                # Primary artistName from Track_Artist(s)
                if 'artistName' not in track and isrc in track_primary_artist_by_isrc:
                    track['artistName'] = track_primary_artist_by_isrc[isrc]
                # Compositions
                if isrc in trk_comp_by_isrc:
                    comp_out = []
                    for cc in trk_comp_by_isrc[isrc]:
                        item = {
                            "share": str(cc["share"]),
                            "roleName": cc["roleName"],
                            "composer": {"name": cc["composerName"]}
                        }
                        if "rightsId" in cc: item["rightsId"] = cc["rightsId"]
                        if "publisherName" in cc: item["publisher"] = {"name": cc["publisherName"]}
                        comp_out.append(item)
                    if comp_out:
                        track["composerContentsDTO"] = comp_out
                # Properties
                if isrc in props_by_isrc:
                    track["trackProperties"] = props_by_isrc[isrc]
                tracks_payload.append((upc, track))
            s.info(tracks=len(tracks_payload))

        # Attach release contributors
        with progress.step("Attach release contributors") as s:
            for rel in releases_payload:
                upc = rel.get("upc")
                if not upc: continue
                # Apply primary artistName from Release_Artist(s)
                if 'artistName' not in rel and upc in release_primary_artist_by_upc:
                    rel['artistName'] = release_primary_artist_by_upc[upc]
                if upc in release_contribs_by_upc:
                    applied = []
                    for c in release_contribs_by_upc[upc]:
                        applied.append({"roleId": c["roleId"], "artist": {"name": c["artistName"]}})
                    if applied:
                        rel["contributors"] = applied
            s.info(releases=len(releases_payload))

        # ===== Emit dry-run artifacts
        with progress.step("Write dry-run artifacts") as s:
            (ARTIFACTS/"artists.json").write_text(json.dumps(artists_payload, indent=2, ensure_ascii=False))
            (ARTIFACTS/"labels.json").write_text(json.dumps(labels_payload, indent=2, ensure_ascii=False))
            (ARTIFACTS/"publishers.json").write_text(json.dumps(publishers_payload, indent=2, ensure_ascii=False))
            (ARTIFACTS/"composers.json").write_text(json.dumps(composers_payload, indent=2, ensure_ascii=False))
            (ARTIFACTS/"releases.json").write_text(json.dumps(releases_payload, indent=2, ensure_ascii=False))
            (ARTIFACTS/"tracks.json").write_text(json.dumps([{"upc": u, **t} for u,t in tracks_payload], indent=2, ensure_ascii=False))
            # Aid troubleshooting: dump resolved audio URLs per ISRC
            try:
                if 'audio_url_map' in globals() or 'audio_url_map' in locals():
                    (ARTIFACTS/"audio_urls.json").write_text(json.dumps(audio_url_map, indent=2, ensure_ascii=False))
            except Exception:
                pass
            s.info(artists=len(artists_payload), labels=len(labels_payload), publishers=len(publishers_payload), composers=len(composers_payload), releases=len(releases_payload), tracks=len(tracks_payload))
            print(f"[OK] Dry-run artifacts written under {ARTIFACTS.resolve()}")

        if not args.live:
            progress.write_log()
            print("Dry-run complete. Re-run with --live to execute API calls.")
            return

        # ===== Live execution (upserts + creation)
        headers_common = {
            "X-EnterpriseId": str(enterpriseId),
            "X-TenantId": str(tenantId),
        }

        http_errors: List[Dict[str, Any]] = []
        def create_simple_list(items, url_path):
            created = 0; failed = 0
            for it in items:
                endpoint = f"{base_url}{url_path}"
                resp = http(session, "POST", endpoint, token, json_body=it, headers=headers_common)
                if not resp.ok:
                    failed += 1
                    err = {
                        "when": "create_simple_list",
                        "path": url_path,
                        "endpoint": endpoint,
                        "status": resp.status_code,
                        "request": it,
                        "response": (resp.text or "")[:1000]
                    }
                    http_errors.append(err)
                    print(f"[WARN] POST {url_path} failed {resp.status_code}: {resp.text[:300]}")
                else:
                    created += 1
            return created, failed

        # Upsert masters and create others
        with progress.step("Upsert masters (artists/labels) and create publishers/composers") as s:
            label_map, l_created, l_reused, l_failed = create_or_reuse_labels(session, base_url, token, headers_common, labels_payload, http_errors)
            artist_map, a_created, a_reused, a_failed = create_or_reuse_artists(session, base_url, token, headers_common, enterpriseId, artists_payload, http_errors)
            p_ok, p_fail = create_simple_list(publishers_payload, "/content/publisher/save")
            c_ok, c_fail = create_simple_list(composers_payload, "/content/composer/save")
            s.info(labels_created=l_created, labels_reused=l_reused, labels_failed=l_failed,
                   artists_created=a_created, artists_reused=a_reused, artists_failed=a_failed,
                   publishers_ok=p_ok, publishers_fail=p_fail, composers_ok=c_ok, composers_fail=c_fail)

        # Inject known IDs into release/track payloads before creation
        with progress.step("Wire labelId/artistId into payloads") as s:
            # Labels on releases
            try:
                for rel in releases_payload:
                    lname = (rel.get("labelName") or "").strip()
                    if lname:
                        lid = None
                        try:
                            lid = label_map.get(lname.lower())
                        except Exception:
                            lid = None
                        if lid:
                            rel["labelId"] = int(lid)
                            rel["hasRecordLabel"] = True
                            # Keep labelName for readability; API should prefer labelId
                # Contributors on releases
                for rel in releases_payload:
                    contribs = rel.get("contributors") or []
                    for c in contribs:
                        art = c.get("artist") or {}
                        nm = (art.get("name") or "").strip()
                        if nm:
                            aid = None
                            try:
                                aid = artist_map.get(nm.lower())
                            except Exception:
                                aid = None
                            if aid:
                                c["artist"] = {"artistId": int(aid)}
                # Contributors on tracks
                for i, (upc, track) in enumerate(tracks_payload):
                    contribs = track.get("contributors") or []
                    for c in contribs:
                        art = c.get("artist") or {}
                        nm = (art.get("name") or "").strip()
                        if nm:
                            aid = None
                            try:
                                aid = artist_map.get(nm.lower())
                            except Exception:
                                aid = None
                            if aid:
                                c["artist"] = {"artistId": int(aid)}
            finally:
                # Summaries
                counted_rel_label_ids = sum(1 for rel in releases_payload if rel.get("labelId"))
                counted_rel_contrib_ids = sum(1 for rel in releases_payload for c in (rel.get("contributors") or []) if isinstance(c.get("artist"), dict) and "artistId" in c.get("artist", {}))
                counted_trk_contrib_ids = sum(1 for _, track in tracks_payload for c in (track.get("contributors") or []) if isinstance(c.get("artist"), dict) and "artistId" in c.get("artist", {}))
                s.info(release_label_ids=counted_rel_label_ids, release_contrib_ids=counted_rel_contrib_ids, track_contrib_ids=counted_trk_contrib_ids)

        # Block if any track lacks a valid uploaded audioId
        with progress.step("Validate required media (audio)") as s:
            missing = []
            for upc, track in tracks_payload:
                trk_recs = track.get("trackRecordingVersions") or []
                isrc = trk_recs[0].get("isrc") if trk_recs else None
                afs = (trk_recs[0].get("audioFiles") if trk_recs else None) or []
                has_audio_id = any(isinstance(af, dict) and af.get("audioId") for af in afs)
                if not has_audio_id:
                    missing.append({
                        "upc": upc,
                        "isrc": isrc,
                        "audioUrl": audio_url_map.get(isrc) if 'audio_url_map' in locals() or 'audio_url_map' in globals() else None
                    })
            s.info(missing=len(missing))
            if missing:
                out = ARTIFACTS/"missing_audio_ids.json"
                out.write_text(json.dumps(missing, indent=2, ensure_ascii=False))
                print(f"[BLOCKED] Missing audioId for {len(missing)} track(s). Upload audio to storage and retry. See {out.resolve()}")
                progress.write_log()
                sys.exit(1)

        # Releases (with UPC duplicate handling)
        with progress.step("Create releases") as s:
            upc_to_release_id: Dict[str,str] = {}
            rel_created = 0; rel_failed = 0
            for rel in releases_payload:
                body = dict(rel)  # copy
                url = f"{base_url}/content/release/save"
                resp = http(session, "POST", url, token, json_body=body, headers=headers_common)
                if not resp.ok:
                    txt = (resp.text or "").lower()
                    # If duplicate UPC error → retry without upc and log
                    if "upc" in txt and ("exist" in txt or "duplicate" in txt or resp.status_code in (400,409)):
                        upc_val = body.pop("upc", None)
                        upc_dupes_logged.append(upc_val)
                        print(f"[INFO] UPC '{upc_val}' appears to exist; retrying without UPC as requested.")
                        resp = http(session, "POST", url, token, json_body=body, headers=headers_common)
                if not resp.ok:
                    rel_failed += 1
                    http_errors.append({
                        "when": "create_release",
                        "status": resp.status_code,
                        "endpoint": url,
                        "request": body,
                        "response": (resp.text or "")[:1500]
                    })
                    print(f"[ERROR] Release create failed {resp.status_code}: {resp.text[:300]}")
                else:
                    rel_created += 1
                    rid = resp.json().get("releaseId")
                    if rel.get("upc"): upc_to_release_id[rel["upc"]] = rid
            s.info(created=rel_created, failed=rel_failed)

    # Tracks per release
    with progress.step("Create tracks") as s:
            t_created = 0; t_failed = 0
            for upc, track in tracks_payload:
                # If we know releaseId, include association if API needs it; otherwise the endpoint may infer.
                t_url = f"{base_url}/content/track/save"
                t_resp = http(session, "POST", t_url, token, json_body=track, headers=headers_common)
                if not t_resp.ok:
                    t_failed += 1
                    http_errors.append({
                        "when": "create_track",
                        "status": t_resp.status_code,
                        "endpoint": t_url,
                        "request": track,
                        "response": (t_resp.text or "")[:1500]
                    })
                    print(f"[ERROR] Track create failed {t_resp.status_code}: {t_resp.text[:300]}")
                else:
                    t_created += 1
            s.info(created=t_created, failed=t_failed)

        if upc_dupes_logged:
            (ARTIFACTS/"upc_skipped_for_duplicates.json").write_text(json.dumps(upc_dupes_logged, indent=2))
            print(f"[INFO] UPCs skipped (already existed): {len(upc_dupes_logged)} → logged to upc_skipped_for_duplicates.json")

        if http_errors:
            (ARTIFACTS/"http_errors.json").write_text(json.dumps(http_errors, indent=2, ensure_ascii=False))
            print(f"[LOG] Wrote HTTP error details to {(ARTIFACTS/ 'http_errors.json').resolve()}")

        progress.write_log()
        print("[DONE] Live execution finished.")

if __name__ == "__main__":
    main()
