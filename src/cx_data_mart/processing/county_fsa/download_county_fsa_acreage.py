#!/usr/bin/env python3
"""Download the LATEST USDA FSA Crop Acreage Data ZIP for a given crop year from:
https://www.fsa.usda.gov/tools/informational/freedom-information-act-foia/electronic-reading-room/frequently-requested/crop-acreage-data

Examples:
  python download_fsa_acreage_by_year.py --year 2025
  python download_fsa_acreage_by_year.py --year 2024 -o ./data
Key features:
- Robust HTTP session with retries & exponential backoff.
- Parses the FOIA page, scopes links to the requested crop year section,
  and picks the newest "as of" date for that year.
- Handles ZIP links that DO NOT have a ".zip" extension (e.g., .../documents/xxxxzip).
- NEW: Resolves '/documents/<id>' landing pages that show "Download" to the actual ZIP URL.
- Validates candidate links via HEAD/GET (Content-Type/Disposition).
- Uses a descriptive filename:
    usda_fsa_crop_acreage_by_crop_county_{YEAR}_asof_{YYYY-MM-DD}.zip
Requirements:
  pip install requests beautifulsoup4
"""

import argparse
import os
import re
import sys
import time
from datetime import UTC, datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

FSA_PAGE = "https://www.fsa.usda.gov/tools/informational/freedom-information-act-foia/electronic-reading-room/frequently-requested/crop-acreage-data"

# Regex for "as of Month Day, Year"
AS_OF_PAT = re.compile(r"\bas of\s+([A-Za-z]+\.?\s+\d{1,2},\s+\d{4})", re.IGNORECASE)
# Fallback for any "Month Day, Year"
DATE_PAT = re.compile(r"([A-Za-z]+\.?\s+\d{1,2},\s+\d{4})")
# Month normalization for abbreviated months
MONTH_FIX = {
    "Sept": "September",
    "Sep.": "September",
    "Sep": "September",
    "Aug.": "August",
    "Oct.": "October",
    "Nov.": "November",
    "Dec.": "December",
    "Jan.": "January",
    "Feb.": "February",
    "Mar.": "March",
    "Apr.": "April",
    "Jun.": "June",
    "Jul.": "July",
}

# Accept zip-like endings (with and without dot)
ZIP_ENDING_PAT = re.compile(r"\.zip$|(?<!\.)zip$", re.IGNORECASE)
# USDA document pages often live here; may be landing pages
DOCS_PATH_PAT = re.compile(r"/documents/", re.IGNORECASE)

# Detect a crop-year at the start of the link text, e.g. "2024 acreage data ..."
CROPYEAR_PREFIX_PAT = re.compile(r"^\s*(20\d{2})\s+acreage\s+data\b", re.IGNORECASE)


# ---------------------------
# HTTP session & helpers
# ---------------------------
def get_session():
    session = requests.Session()
    retry = Retry(
        total=8,
        connect=5,
        read=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
    )
    return session


def http_get(session, url, timeout=(20, 180), stream=False):
    return session.get(url, timeout=timeout, stream=stream)


def http_head(session, url, timeout=(10, 40), allow_redirects=True):
    return session.head(url, timeout=timeout, allow_redirects=allow_redirects)


# ---------------------------
# Parsing helpers
# ---------------------------
def normalize_months(date_text: str) -> str:
    tokens = date_text.split()
    if not tokens:
        return date_text
    first = tokens[0]
    fixed = MONTH_FIX.get(first, first)
    tokens[0] = fixed
    return " ".join(tokens)


def parse_date_from_text(text: str):
    if not text:
        return None
    m = AS_OF_PAT.search(text)
    if not m:
        m = DATE_PAT.search(text)
    if not m:
        return None
    date_str = normalize_months(m.group(1))
    for fmt in ("%B %d, %Y",):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    try:
        date_str = date_str.replace("  ", " ").replace(" ,", ",")
        return datetime.strptime(date_str, "%B %d, %Y")
    except Exception:
        return None


def get_text_with_context(a_tag):
    texts = []
    if a_tag:
        t = a_tag.get_text(" ", strip=True)
        if t:
            texts.append(t)
        parent = a_tag.find_parent(["li", "p", "div"])
        if parent:
            pt = parent.get_text(" ", strip=True)
            if pt:
                texts.append(pt)
        prev = a_tag.find_previous(string=True)
        if prev:
            texts.append(str(prev).strip())
    unique = []
    seen = set()
    for x in texts:
        if x and x not in seen:
            unique.append(x)
            seen.add(x)
    return " | ".join(unique)


def extract_year_hint(text: str):
    m = re.search(r"\b(20\d{2})\b", text or "")
    return m.group(1) if m else None


def crop_year_from_link_text(link_text: str) -> str | None:
    m = CROPYEAR_PREFIX_PAT.search(link_text or "")
    return m.group(1) if m else None


def nearest_crop_year_heading(a_tag):
    for header in a_tag.find_all_previous(["h2", "h3", "h4"], limit=8):
        ht = header.get_text(" ", strip=True)
        if not ht:
            continue
        if "crop year" in ht.lower():
            m = re.search(r"\b(20\d{2})\b", ht)
            if m:
                return m.group(1)
    return None


def clean_filename_from_url(url: str) -> str:
    name = os.path.basename(urlparse(url).path) or "fsa_acreage_data.zip"
    if not name.lower().endswith(".zip"):
        if name.lower().endswith("zip"):
            name = f"{name}.zip"
        else:
            name += ".zip"
    return name


def is_zip_like_href(href: str) -> bool:
    # Accept typical .zip links, links ending with 'zip' (no dot), and
    # document links that might be binary assets or landing pages.
    if ZIP_ENDING_PAT.search(href):
        return True
    if DOCS_PATH_PAT.search(href):
        return True
    return False


# ---------------------------
# URL validation & resolution
# ---------------------------
def validate_zip_headers_like_zip(resp, url: str) -> bool:
    """Return True if response headers suggest a ZIP download."""
    ct = (
        resp.headers.get("Content-Type") or resp.headers.get("content-type") or ""
    ).lower()
    cd = (
        resp.headers.get("Content-Disposition")
        or resp.headers.get("content-disposition")
        or ""
    )
    is_zip_ct = (
        ("zip" in ct)
        or ("application/octet-stream" in ct)
        or ("application/x-zip" in ct)
    )
    has_zip_name = ".zip" in cd.lower()
    ends_zip = url.lower().endswith(".zip") or url.lower().endswith("zip")
    return bool(is_zip_ct or has_zip_name or ends_zip)


def resolve_document_download_url(session, doc_url: str) -> str | None:
    """Some '/documents/<id>' URLs are HTML landing pages that show 'Your file is ready. Download'.
    This function loads the page, parses HTML, and looks for actual download anchors.
    Returns the resolved absolute URL to the ZIP, or None if not found.
    """
    try:
        r = http_get(session, doc_url, timeout=(20, 180), stream=False)
    except Exception:
        return None

    # If it's already a binary, just use it.
    if r.ok and validate_zip_headers_like_zip(r, doc_url):
        return doc_url

    if not r.ok:
        return None

    # Must be HTML landing page; parse anchors.
    soup = BeautifulSoup(r.text, "html.parser")
    candidate_hrefs = []

    # Heuristics: look for a visible 'Download' link first.
    for a in soup.find_all("a", href=True):
        text = a.get_text(" ", strip=True).lower()
        href = a["href"].strip()
        if not href:
            continue
        href_l = href.lower()

        if (
            ("download" in text)
            or ("download" in href_l)
            or href_l.endswith(".zip")
            or href_l.endswith("zip")
        ):
            candidate_hrefs.append(urljoin(doc_url, href))

    # De-dup while preserving order
    seen = set()
    candidates = []
    for u in candidate_hrefs:
        if u not in seen:
            candidates.append(u)
            seen.add(u)

    # Validate candidates via HEAD/GET
    for cand in candidates:
        try:
            h = http_head(session, cand, timeout=(10, 40), allow_redirects=True)
            # Some servers don't like HEAD; fall back to GET stream quickly
            if (
                h.status_code >= 400
                or not h.ok
                or not validate_zip_headers_like_zip(h, cand)
            ):
                g = http_get(session, cand, timeout=(20, 40), stream=True)
                ok = g.ok and validate_zip_headers_like_zip(g, cand)
                g.close()
                if ok:
                    return cand
            else:
                return cand
        except Exception:
            continue
    return None


def validate_or_resolve_zip_url(session, absolute_url: str) -> str | None:
    """Returns a confirmed ZIP download URL:
    - If absolute_url is already a ZIP (by headers), return it.
    - If it's an HTML landing page under /documents/, resolve to its 'Download' URL.
    - Otherwise, return None.
    """
    # First, try HEAD/GET to see if it's directly a ZIP.
    try:
        h = http_head(session, absolute_url, timeout=(10, 40), allow_redirects=True)
        if (
            h.status_code < 400
            and h.ok
            and validate_zip_headers_like_zip(h, absolute_url)
        ):
            return absolute_url
        # Fallback to GET (some servers don’t expose headers fully on HEAD)
        g = http_get(session, absolute_url, timeout=(20, 40), stream=True)
        ok = g.ok and validate_zip_headers_like_zip(g, absolute_url)
        g.close()
        if ok:
            return absolute_url
    except Exception:
        pass

    # If it’s under /documents/, try to resolve a 'Download' link inside.
    if DOCS_PATH_PAT.search(absolute_url):
        resolved = resolve_document_download_url(session, absolute_url)
        return resolved

    return None


# ---------------------------
# Core logic
# ---------------------------
def fetch_index_html(session):
    try:
        resp = http_get(session, FSA_PAGE, timeout=(20, 180), stream=False)
        resp.raise_for_status()
    except requests.exceptions.ReadTimeout:
        time.sleep(5)
        resp = http_get(session, FSA_PAGE, timeout=(20, 240), stream=False)
        resp.raise_for_status()
    return resp.text


def collect_year_zip_links(session, soup: BeautifulSoup, target_year: str):
    """Return all ZIP link records that belong to the target crop year section.
    Prefer crop year derived from the link text ('YYYY acreage data ...'),
    then from the nearest 'YYYY Crop Year' heading, then a generic year hint.
    Validate links, and resolve landing pages to the actual ZIP URL when necessary.
    """
    results = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not is_zip_like_href(href):
            continue

        context_text = get_text_with_context(a)
        link_text = a.get_text(" ", strip=True)
        link_text_year = crop_year_from_link_text(link_text)
        section_year = nearest_crop_year_heading(a)
        year_hint = extract_year_hint(context_text)

        link_year = link_text_year or section_year or year_hint
        if link_year != target_year:
            continue

        absolute_url = urljoin(FSA_PAGE, href)

        # Validate/resolve to a true ZIP URL (handles /documents/<id> landing pages)
        zip_url = validate_or_resolve_zip_url(session, absolute_url)
        if not zip_url:
            continue

        asof_dt = parse_date_from_text(context_text)

        results.append(
            {
                "year": link_year,
                "url": zip_url,  # use the final resolved ZIP URL
                "asof": asof_dt,
                "text": link_text,
                "context": context_text,
            }
        )
    return results


def choose_latest_for_year(zip_items):
    if not zip_items:
        return None
    dated = [z for z in zip_items if z["asof"] is not None]
    if dated:
        dated.sort(key=lambda z: z["asof"])
        return dated[-1]
    if len(zip_items) == 1:
        return zip_items[0]
    return zip_items[0]


def descriptive_filename(year: str, as_of_date: datetime | None) -> str:
    prefix = "usda_fsa_crop_acreage_by_crop_county"
    if as_of_date:
        return f"{prefix}_{year}_asof_{as_of_date.strftime('%Y-%m-%d')}.zip"
    return f"{prefix}_{year}.zip"


def stream_download(session, url: str, out_path: str):
    with http_get(session, url, timeout=(20, 300), stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", "0")) or None
        chunk = 1024 * 1024
        downloaded = 0
        with open(out_path, "wb") as f:
            for part in r.iter_content(chunk_size=chunk):
                if part:
                    f.write(part)
                    downloaded += len(part)
                    if total:
                        done = int(50 * downloaded / total)
                        sys.stdout.write(
                            "\r[{}{}] {}/{} bytes".format(
                                "#" * done, "." * (50 - done), downloaded, total
                            )
                        )
                        sys.stdout.flush()
        if total:
            sys.stdout.write("\n")


# ---------------------------
# CLI
# ---------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Download the latest USDA FSA Crop Acreage Data ZIP for a given"
        " crop year."
    )
    p.add_argument(
        "--year",
        "-y",
        required=True,
        type=int,
        help="Crop year to download (e.g., 2025).",
    )
    p.add_argument(
        "-o",
        "--outdir",
        default=".",
        help="Output directory (default: current directory).",
    )
    return p.parse_args()


def main(target_year, outdir):
    os.makedirs(outdir, exist_ok=True)

    print("FSA Crop Acreage Data downloader")
    print(f"  Crop year:    {target_year}")
    print(f"  Index page:   {FSA_PAGE}")
    print(f"  Output dir:   {os.path.abspath(outdir)}")

    session = get_session()

    print("\nFetching index page ...")
    html = fetch_index_html(session)
    soup = BeautifulSoup(html, "html.parser")

    print(f"Scanning links for crop year {target_year} ...")
    zip_items = collect_year_zip_links(session, soup, target_year)

    if not zip_items:
        all_years = set()
        for a in soup.find_all(["h2", "h3", "h4"]):
            ht = a.get_text(" ", strip=True)
            if ht and "crop year" in ht.lower():
                m = re.search(r"\b(20\d{2})\b", ht)
                if m:
                    all_years.add(m.group(1))
        print(f"\nNo ZIP files found for crop year {target_year}.")
        if all_years:
            years_list = ", ".join(sorted(all_years))
            print(f"Available crop years on page: {years_list}")
        sys.exit(2)

    latest = choose_latest_for_year(zip_items)
    if not latest:
        print(f"\nCould not determine the latest file for crop year {target_year}.")
        sys.exit(3)

    url = latest["url"]
    asof_dt = latest["asof"]
    filename = descriptive_filename(target_year, asof_dt)
    out_path = os.path.join(outdir, filename)

    print("\nLatest file identified:")
    print(f"  Year:       {target_year}")
    print(f"  URL:        {url}")
    print(f"  Link text:  {latest['text']}")
    print(f"  As-of date: {asof_dt.strftime('%Y-%m-%d') if asof_dt else 'N/A'}")
    print(f"  Saving to:  {out_path}")

    stream_download(session, url, out_path)

    if os.path.getsize(out_path) == 0:
        print("Downloaded file is empty; removing.")
        os.remove(out_path)
        sys.exit(4)

    print("\nDownload complete.")
    print(f"File saved: {out_path}")


def generate_years_str_range(start_str: str, end_str: str) -> list[str]:
    """Generate a range of years between start_str and end_str."""
    start = int(start_str)
    end = int(end_str)
    return [str(year) for year in range(start, end + 1)]


if __name__ == "__main__":
    outdir = "county_fsa_downloads"
    current_year = str(datetime.now(UTC).year)
    target_years = generate_years_str_range(start_str="2007", end_str=current_year)
    for target_year in target_years:
        print(f"Downloading {target_year} ...")
        main(target_year, outdir)
