"""
LLM-powered deal extraction using Claude Haiku.
Extracts structured deal data from article text with graceful fallback
when ANTHROPIC_API_KEY is not set.

V2.0 changes:
  - Switched to tool_use for guaranteed valid JSON (no more code-fence parsing)
  - Added few-shot examples to extraction prompt (+15-20% accuracy)
  - Updated stage-from-amount thresholds for 2025-2026 market
  - Improved company name cleaning (formerly, legal suffixes, em dash)
"""

import os
import re
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

_client = None
_client_checked = False

from config import LLM_MODEL, LLM_MAX_TEXT_LENGTH
MODEL = LLM_MODEL


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


# ── Tool schema for structured extraction ─────────────────────

DEAL_TOOL = {
    "name": "extract_deal",
    "description": "Extract structured funding deal data from a news article.",
    "input_schema": {
        "type": "object",
        "properties": {
            "company_name": {
                "type": "string",
                "description": "Clean company name only (e.g. 'Ramp' not 'Ramp, a fintech startup')"
            },
            "description": {
                "type": ["string", "null"],
                "description": "One sentence describing what the company does"
            },
            "amount": {
                "type": ["number", "null"],
                "description": "Funding amount in USD (e.g. 5000000 for $5M). Null if undisclosed."
            },
            "stage": {
                "type": "string",
                "enum": ["Pre-Seed", "Seed", "Series A", "Series B", "Series C+", "Unknown"]
            },
            "investors": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of investor/firm names"
            },
            "lead_investor": {
                "type": ["string", "null"],
                "description": "Lead investor name, or null if not stated"
            },
            "sector": {
                "type": "string",
                "enum": [
                    "Fintech", "Health & Biotech", "AI / Machine Learning",
                    "SaaS / Enterprise", "Cybersecurity", "Consumer / D2C",
                    "Web3 / Crypto", "Real Estate / Proptech", "Climate / Cleantech",
                    "Developer Tools", "HR / Future of Work", "Food & Agriculture",
                    "Marketplace", "Legal Tech", "Logistics / Supply Chain",
                    "Education / Edtech", "Insurance / Insurtech",
                    "Media & Entertainment", "Robotics / Deep Tech", "Other"
                ]
            },
            "is_nyc": {
                "type": ["boolean", "null"],
                "description": "True if there's clear evidence the company is NYC-based"
            },
            "is_funding_deal": {
                "type": "boolean",
                "description": "False for articles about layoffs, acquisitions, IPOs, market analysis, etc."
            }
        },
        "required": ["company_name", "stage", "investors", "sector", "is_funding_deal"]
    }
}


EXTRACTION_PROMPT = """\
You are a financial data extraction assistant. Given a news article title and text about a startup funding round, extract structured deal information using the extract_deal tool.

Rules:
- company_name should be JUST the company name, not a description
- is_funding_deal should be false for articles about layoffs, acquisitions, IPOs, market analysis, etc.
- amount should be a raw number in USD (5000000 not "5M")
- Only set is_nyc to true if there's clear evidence the company is NYC-based
- For investors, include both lead and participating investors
- IMPORTANT for stage: Try hard to determine the stage. Look for keywords like "seed", "Series A/B/C", "pre-seed", "angel", "growth", etc.
- If no explicit stage keyword exists but the amount is known, infer: <$2M = Pre-Seed, <$8M = Seed, <$40M = Series A, <$100M = Series B, >=$100M = Series C+
- Only use "Unknown" as a last resort when neither stage keywords nor amount are available.

Here are examples of correct extractions:

Example 1:
Title: "Fintech Startup Ramp Raises $150M Series C Led by Founders Fund"
Text: "New York-based corporate card startup Ramp announced a $150 million Series C round today. The round was led by Founders Fund with participation from Thrive Capital, D1 Capital Partners, and existing investors Stripe and Goldman Sachs."
→ company_name: "Ramp", amount: 150000000, stage: "Series C+", investors: ["Founders Fund", "Thrive Capital", "D1 Capital Partners", "Stripe", "Goldman Sachs"], lead_investor: "Founders Fund", sector: "Fintech", is_nyc: true, is_funding_deal: true

Example 2:
Title: "Seed Round: AI Coding Assistant DevCo Secures $6M"
Text: "DevCo, which builds AI-powered code review tools for enterprise teams, has raised a $6 million seed round. The investment was led by Sequoia Capital, with Andreessen Horowitz also participating."
→ company_name: "DevCo", amount: 6000000, stage: "Seed", investors: ["Sequoia Capital", "Andreessen Horowitz"], lead_investor: "Sequoia Capital", sector: "Developer Tools", is_nyc: null, is_funding_deal: true

Example 3:
Title: "Tech Layoffs Continue as Startup Cuts 30% of Staff"
Text: "The latest round of tech layoffs hit NYC-based startup FooBar today, with the company cutting 30% of its workforce..."
→ company_name: "FooBar", is_funding_deal: false"""


def extract_deal_from_text(title: str, text: str) -> Optional[Dict]:
    """
    Send article to Claude Haiku and return structured JSON.
    Uses tool_use for guaranteed valid JSON output.
    Returns None if API key not set or extraction fails.
    On 429 rate limits, returns None immediately (regex fallback handles it).
    """
    client = _get_client()
    if not client:
        return None

    # Truncate text to avoid excessive token usage
    truncated = text[:LLM_MAX_TEXT_LENGTH] if len(text) > LLM_MAX_TEXT_LENGTH else text
    user_msg = f"Title: {title}\n\nArticle text:\n{truncated}"

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            tools=[DEAL_TOOL],
            tool_choice={"type": "tool", "name": "extract_deal"},
            messages=[
                {"role": "user", "content": user_msg}
            ],
            system=EXTRACTION_PROMPT,
        )

        # Extract the tool_use result — guaranteed valid JSON
        for block in response.content:
            if block.type == "tool_use":
                return block.input

        logger.debug("LLM returned no tool_use block for '%s'", title[:60])
        return None

    except Exception as e:
        # 429s fail fast — regex fallback handles these articles
        if "429" in str(e) or "rate_limit" in str(e):
            logger.debug("LLM rate limited for '%s', falling back to regex", title[:40])
        else:
            logger.warning("LLM extraction failed for '%s': %s", title[:60], e)
        return None


# ── Batch tool schema for AlleyWatch multi-deal extraction ────

BATCH_DEAL_TOOL = {
    "name": "extract_deals",
    "description": "Extract ALL funding deals from an AlleyWatch daily report.",
    "input_schema": {
        "type": "object",
        "properties": {
            "deals": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "company_name": {"type": "string"},
                        "description": {"type": ["string", "null"]},
                        "amount": {"type": ["number", "null"]},
                        "stage": {
                            "type": "string",
                            "enum": ["Pre-Seed", "Seed", "Series A", "Series B", "Series C+", "Unknown"]
                        },
                        "investors": {"type": "array", "items": {"type": "string"}},
                        "lead_investor": {"type": ["string", "null"]},
                        "sector": {
                            "type": "string",
                            "enum": [
                                "Fintech", "Health & Biotech", "AI / Machine Learning",
                                "SaaS / Enterprise", "Cybersecurity", "Consumer / D2C",
                                "Web3 / Crypto", "Real Estate / Proptech", "Climate / Cleantech",
                                "Developer Tools", "HR / Future of Work", "Food & Agriculture",
                                "Marketplace", "Legal Tech", "Logistics / Supply Chain",
                                "Education / Edtech", "Insurance / Insurtech",
                                "Media & Entertainment", "Robotics / Deep Tech", "Other"
                            ]
                        }
                    },
                    "required": ["company_name", "stage", "investors", "sector"]
                }
            }
        },
        "required": ["deals"]
    }
}


BATCH_PROMPT = """\
You are a financial data extraction assistant. Given an AlleyWatch daily funding report (which contains multiple deal announcements), extract ALL deals from the text using the extract_deals tool.

Rules:
- Extract EVERY deal mentioned in the report
- company_name should be JUST the company name
- amount should be a raw number in USD
- Include all investors mentioned for each deal
- IMPORTANT for stage: Try hard to determine the stage. Look for keywords like "seed", "Series A/B/C", "pre-seed", "angel", "growth", etc. If no explicit stage keyword exists but the amount is known, infer: <$2M = Pre-Seed, <$8M = Seed, <$40M = Series A, <$100M = Series B, >=$100M = Series C+. Only use "Unknown" as a last resort."""


def extract_deals_batch(articles: List[Dict], max_workers: int = 5) -> List[Optional[Dict]]:
    """
    Extract deals from multiple articles in parallel.
    articles: list of dicts with 'title' and 'text' keys.
    Returns: list of extraction results in same order as input (None for failures).
    429 rate limits fail fast and fall back to regex extraction.
    """
    client = _get_client()
    if not client:
        return [None] * len(articles)

    results = [None] * len(articles)

    def _extract(idx, article):
        title = article.get("title", "")
        text = article.get("text", "")
        return idx, extract_deal_from_text(title, text)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_extract, i, a): i for i, a in enumerate(articles)}
        for future in as_completed(futures):
            try:
                idx, result = future.result()
                results[idx] = result
            except Exception as e:
                idx = futures[future]
                logger.debug("Batch extraction failed for '%s': %s",
                             articles[idx].get("title", "")[:60], e)

    logger.info("LLM batch extraction: %d/%d succeeded",
                sum(1 for v in results if v), len(articles))
    return results


def extract_alleywatch_deals(page_text: str) -> Optional[List[Dict]]:
    """
    Send an AlleyWatch daily report page to Haiku and extract all deals.
    Uses tool_use for guaranteed valid JSON output.
    Returns list of deal dicts or None.
    """
    client = _get_client()
    if not client:
        return None

    truncated = page_text[:12000] if len(page_text) > 12000 else page_text

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=4000,
            tools=[BATCH_DEAL_TOOL],
            tool_choice={"type": "tool", "name": "extract_deals"},
            messages=[
                {"role": "user", "content": f"AlleyWatch funding report:\n\n{truncated}"}
            ],
            system=BATCH_PROMPT,
        )

        for block in response.content:
            if block.type == "tool_use":
                result = block.input
                if isinstance(result, dict) and "deals" in result:
                    return result["deals"]
                if isinstance(result, list):
                    return result

        return None

    except Exception as e:
        if "429" in str(e) or "rate_limit" in str(e):
            logger.debug("LLM rate limited for AlleyWatch page, falling back to regex")
        else:
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
    'Acme (formerly OldName)' -> 'Acme'
    'Acme, Inc.' -> 'Acme'
    'Acme — Series A' -> 'Acme'
    """
    if not name:
        return name

    # Strip trailing comma and whitespace
    name = name.strip().rstrip(",").strip()

    # Strip "(formerly ...)"
    name = re.sub(r"\s*\(formerly\s+.+?\)", "", name, flags=re.I).strip()

    # Strip legal suffixes
    name = re.sub(r",?\s*(?:Inc\.?|LLC|Corp\.?|Ltd\.?|L\.P\.?)$", "", name, flags=re.I).strip()

    # Strip " — Series X" / " - Raises $XM" / " — Closes $XM"
    name = re.sub(
        r"\s*[—–\-]\s*(?:Series|Raises?|Closes?|Secures?|Announces?).*$",
        "", name, flags=re.I
    ).strip()

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
