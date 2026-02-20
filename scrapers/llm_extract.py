"""
LLM-powered deal extraction using Claude Haiku.
Extracts structured deal data from article text with graceful fallback
when ANTHROPIC_API_KEY is not set.
"""

import os
import re
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

_client = None
_client_checked = False

from config import LLM_MODEL, LLM_MAX_TEXT_LENGTH
MODEL = LLM_MODEL

# Rate limiter: 4 requests per minute (with buffer under 5 RPM limit)
_rate_lock = threading.Lock()
_request_times: list = []
_RPM_LIMIT = 4


def _wait_for_rate_limit():
    """Block until we're under the RPM limit."""
    with _rate_lock:
        now = time.time()
        # Remove timestamps older than 60 seconds
        _request_times[:] = [t for t in _request_times if now - t < 60]
        if len(_request_times) >= _RPM_LIMIT:
            wait = 60 - (now - _request_times[0]) + 0.5
            if wait > 0:
                logger.debug("Rate limit: waiting %.1fs", wait)
                _rate_lock.release()
                time.sleep(wait)
                _rate_lock.acquire()
                now = time.time()
                _request_times[:] = [t for t in _request_times if now - t < 60]
        _request_times.append(time.time())


def _get_client():
    """Lazy-init Anthropic client. Returns None if API key not set."""
    global _client, _client_checked
    if _client_checked:
        return _client
    _client_checked = True
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        logger.info("ANTHROPIC_API_KEY not set — LLM extraction disabled")
        return None
    try:
        from anthropic import Anthropic
        _client = Anthropic(api_key=api_key)
        logger.info("Anthropic client initialized (model: %s)", MODEL)
        return _client
    except ImportError:
        logger.warning("anthropic package not installed — LLM extraction disabled")
        return None
    except Exception as e:
        logger.warning("Failed to init Anthropic client: %s", e)
        return None


EXTRACTION_PROMPT = """\
You are a financial data extraction assistant. Given a news article title and text about a startup funding round, extract structured deal information.

Return ONLY a JSON object with these fields (use null for unknown values):
{
  "company_name": "Clean company name only (e.g. 'Acme' not 'Acme, an AI startup')",
  "description": "One sentence describing what the company does",
  "amount": null or number in USD (e.g. 5000000 for $5M),
  "stage": "Pre-Seed" | "Seed" | "Series A" | "Series B" | "Series C+" | "Unknown",
  "investors": ["Investor Name 1", "Investor Name 2"],
  "lead_investor": "Lead investor name" or null,
  "sector": "Fintech" | "Health & Biotech" | "AI / Machine Learning" | "SaaS / Enterprise" | "Cybersecurity" | "Consumer / D2C" | "Web3 / Crypto" | "Real Estate / Proptech" | "Climate / Cleantech" | "Developer Tools" | "HR / Future of Work" | "Food & Agriculture" | "Marketplace" | "Legal Tech" | "Logistics / Supply Chain" | "Education / Edtech" | "Insurance / Insurtech" | "Media & Entertainment" | "Other",
  "is_nyc": true | false | null,
  "is_funding_deal": true | false
}

Rules:
- company_name should be JUST the company name, not a description
- is_funding_deal should be false for articles about layoffs, acquisitions, IPOs, market analysis, etc.
- amount should be a raw number in USD (5000000 not "5M")
- Only set is_nyc to true if there's clear evidence the company is NYC-based
- For investors, include both lead and participating investors
- IMPORTANT for stage: Try hard to determine the stage. Look for keywords like "seed", "Series A/B/C", "pre-seed", "angel", "growth", etc. If no explicit stage keyword exists but the amount is known, infer: <$500K = Pre-Seed, <$3M = Seed, <$20M = Series A, <$80M = Series B, >=$80M = Series C+. Only use "Unknown" as a last resort when neither stage keywords nor amount are available."""


def extract_deal_from_text(title: str, text: str) -> Optional[Dict]:
    """
    Send article to Claude Haiku and return structured JSON.
    Returns None if API key not set or extraction fails.
    Respects rate limits and retries on 429 errors.
    """
    client = _get_client()
    if not client:
        return None

    # Truncate text to avoid excessive token usage
    truncated = text[:LLM_MAX_TEXT_LENGTH] if len(text) > LLM_MAX_TEXT_LENGTH else text
    user_msg = f"Title: {title}\n\nArticle text:\n{truncated}"

    for attempt in range(3):
        _wait_for_rate_limit()
        try:
            response = client.messages.create(
                model=MODEL,
                max_tokens=500,
                messages=[
                    {"role": "user", "content": user_msg}
                ],
                system=EXTRACTION_PROMPT,
            )
            raw = response.content[0].text.strip()

            # Strip markdown code fences if present
            if raw.startswith("```"):
                raw = re.sub(r"^```(?:json)?\s*", "", raw)
                raw = re.sub(r"\s*```$", "", raw)

            result = json.loads(raw)
            return result

        except json.JSONDecodeError as e:
            logger.debug("LLM returned invalid JSON for '%s': %s", title[:60], e)
            return None
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "rate_limit" in err_str:
                wait = 15 * (attempt + 1)
                logger.debug("Rate limited on '%s', waiting %ds (attempt %d/3)",
                             title[:40], wait, attempt + 1)
                time.sleep(wait)
                continue
            logger.warning("LLM extraction failed for '%s': %s", title[:60], e)
            return None

    logger.warning("LLM extraction gave up after 3 rate-limit retries for '%s'", title[:60])
    return None


BATCH_PROMPT = """\
You are a financial data extraction assistant. Given an AlleyWatch daily funding report (which contains multiple deal announcements), extract ALL deals from the text.

Return ONLY a JSON array of objects, one per deal:
[
  {
    "company_name": "Clean company name",
    "description": "What the company does (one sentence)",
    "amount": null or number in USD,
    "stage": "Pre-Seed" | "Seed" | "Series A" | "Series B" | "Series C+" | "Unknown",
    "investors": ["Investor 1", "Investor 2"],
    "lead_investor": "Lead investor" or null,
    "sector": "Fintech" | "Health & Biotech" | "AI / Machine Learning" | "SaaS / Enterprise" | "Cybersecurity" | "Consumer / D2C" | "Web3 / Crypto" | "Real Estate / Proptech" | "Climate / Cleantech" | "Developer Tools" | "HR / Future of Work" | "Food & Agriculture" | "Marketplace" | "Legal Tech" | "Logistics / Supply Chain" | "Education / Edtech" | "Insurance / Insurtech" | "Media & Entertainment" | "Other"
  }
]

Rules:
- Extract EVERY deal mentioned in the report
- company_name should be JUST the company name
- amount should be a raw number in USD
- Include all investors mentioned for each deal
- IMPORTANT for stage: Try hard to determine the stage. Look for keywords like "seed", "Series A/B/C", "pre-seed", "angel", "growth", etc. If no explicit stage keyword exists but the amount is known, infer: <$500K = Pre-Seed, <$3M = Seed, <$20M = Series A, <$80M = Series B, >=$80M = Series C+. Only use "Unknown" as a last resort."""


def extract_deals_batch(articles: List[Dict], max_workers: int = 1) -> Dict[str, Optional[Dict]]:
    """
    Extract deals from multiple articles sequentially (respects RPM limits).
    articles: list of dicts with 'title' and 'text' keys.
    Returns: dict mapping title -> extraction result (or None).
    """
    client = _get_client()
    if not client:
        return {}

    results = {}
    for i, article in enumerate(articles):
        title = article.get("title", "")
        text = article.get("text", "")
        try:
            result = extract_deal_from_text(title, text)
            results[title] = result
        except Exception as e:
            logger.debug("Batch extraction failed for '%s': %s", title[:60], e)
            results[title] = None
        if (i + 1) % 50 == 0:
            logger.info("LLM batch progress: %d/%d articles", i + 1, len(articles))

    logger.info("LLM batch extraction: %d/%d succeeded",
                sum(1 for v in results.values() if v), len(articles))
    return results


def extract_alleywatch_deals(page_text: str) -> Optional[List[Dict]]:
    """
    Send an AlleyWatch daily report page to Haiku and extract all deals.
    Returns list of deal dicts or None.
    """
    client = _get_client()
    if not client:
        return None

    truncated = page_text[:12000] if len(page_text) > 12000 else page_text

    _wait_for_rate_limit()
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            messages=[
                {"role": "user", "content": f"AlleyWatch funding report:\n\n{truncated}"}
            ],
            system=BATCH_PROMPT,
        )
        raw = response.content[0].text.strip()

        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)

        deals = json.loads(raw)
        if isinstance(deals, list):
            return deals
        return None

    except json.JSONDecodeError as e:
        logger.debug("LLM returned invalid JSON for AlleyWatch page: %s", e)
        return None
    except Exception as e:
        logger.warning("LLM AlleyWatch extraction failed: %s", e)
        return None


# ── Company Name Validation & Cleaning ─────────────────────────

_VERB_PATTERNS = re.compile(
    r"\b(started|announced|reported|said|told|according|launched|"
    r"plans|planning|expected|looking|seeking|trying|working|"
    r"becomes|turned|moved|expanded|joined|hired|fired|laid off)\b",
    re.I
)

_HEADLINE_PATTERNS = re.compile(
    r"(^a\s|^an\s|exclusive:|breaking:|report:|"
    r"update:|analysis:|why\s|how\s|what\s|when\s|who\s|where\s|"
    r"top\s+news|roundup|exploring\s)",
    re.I
)

# Patterns that indicate a headline was used as a company name
_HEADLINE_PREFIX_RE = re.compile(
    r"^(?:(?:AI|Gen\s*AI|RPA|Legal|Digital|Enterprise|Defense\s*Tech|"
    r"E-mobility|Smart\s*ring|Indoor\s*farming|Photonic|Data|Sales|"
    r"Media|Events|Luggage|Betting|Flexible|Funding\s*Daily|"
    r"Virtual|Cloud|Insurance|Identity|CPG|Israeli|Brooklyn|"
    r"San\s*Francisco|Long\s*Island|SoHo|NYC|New\s*York|"
    r"Anthropic|On-Demand|Correction|WealthStack|Big\s*Data|"
    r"In-House|Patent|Contract|Subletting|Swiss|"
    r"AI-Powered|AI-Coding|Generative\s*AI|Embedded|"
    r"AI\s+Cloud|AI\s+Video|AI\s+digital|AI\s+field|AI\s+agent|AI\s+Client)"
    r"[\s\-]+(?:startup|company|firm|platform|app|founder|operations|research))",
    re.I
)


def validate_company_name(name: str) -> bool:
    """
    Return True if name looks like a valid company name.
    Rejects names >45 chars, containing verbs, headline patterns,
    or common headline-as-name prefixes.
    """
    if not name or len(name) > 45:
        return False
    if _VERB_PATTERNS.search(name):
        return False
    if _HEADLINE_PATTERNS.search(name):
        return False
    if _HEADLINE_PREFIX_RE.search(name):
        return False
    # Reject names containing "startup" (headline artifacts)
    if re.search(r"\bstartup\b", name, re.I):
        return False
    # Reject if it's mostly lowercase words (likely a sentence fragment)
    words = [w for w in name.split() if w]
    if len(words) > 3:
        lowercase_count = sum(1 for w in words if w[0].islower())
        if lowercase_count > len(words) * 0.6:
            return False
    return True


def clean_company_name(name: str) -> str:
    """
    Clean up a company name extracted from a headline.
    'Bedrock, an A.I. Start-Up for Construction,' -> 'Bedrock'
    'AI Startup Acme' -> 'Acme'
    """
    if not name:
        return name

    # Strip trailing comma and whitespace
    name = name.strip().rstrip(",").strip()

    # Remove "a/an ... startup/company" suffix
    name = re.sub(
        r",?\s+(?:a|an)\s+.+?(?:startup|start-up|company|firm|platform|maker|provider).*$",
        "", name, flags=re.I
    ).strip()

    # Remove leading qualifiers
    name = re.sub(
        r"^(?:AI|A\.I\.|fintech|healthtech|biotech|edtech|proptech|"
        r"cybersecurity|saas|crypto|web3)\s+(?:startup|start-up|company|firm)\s+",
        "", name, flags=re.I
    ).strip()

    # Remove "NYC-based" / "New York-based" prefix
    name = re.sub(
        r"^(?:nyc|new\s+york|ny|manhattan|brooklyn)[\-\s]+based\s+",
        "", name, flags=re.I
    ).strip()

    # Strip quotes
    name = name.strip("'\"''""\u201c\u201d ")

    return name
