import json
import time
import logging
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
import trafilatura
from bs4 import BeautifulSoup

from seeds import SEED_URLS

# ── Config ───────────────────────────────────────────────────────
OUTPUT_FILE = Path(__file__).parent.parent.parent / "data" / "raw" / "crawler_output.jsonl"
CRAWL_DELAY = 1.5          # seconds between requests (polite crawling)
MAX_PAGES   = 120          # hard cap to keep runtime manageable
USER_AGENT  = "nba-kg-crawler/1.0 (academic project; respectful of robots.txt)"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Robots.txt checker ───────────────────────────────────────────
# Wikipedia's robots.txt explicitly allows /wiki/ pages for all crawlers.
# We verify this at runtime and fall back to allow-all if the file
# cannot be fetched (e.g. network timeout), so crawling is never
# silently blocked by a parser failure.
def build_robot_parser(base_url: str) -> RobotFileParser | None:
    robots_url = urljoin(base_url, "/robots.txt")
    try:
        resp = requests.get(robots_url, timeout=10,
                            headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        rp = RobotFileParser()
        rp.set_url(robots_url)
        rp.parse(resp.text.splitlines())
        log.info("robots.txt loaded from %s", robots_url)
        return rp
    except Exception as exc:
        log.warning("Could not load robots.txt (%s) — defaulting to allow-all", exc)
        return None


# ── Text extraction ───────────────────────────────────────────────
def extract_text(html: str, url: str) -> str | None:
    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
        no_fallback=False,
    )
    return text


# ── Link discovery (Wikipedia internal only) ─────────────────────
WIKI_LINK_RE = re.compile(r"^/wiki/[^:#]+$")

def discover_links(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if WIKI_LINK_RE.match(href):
            full = urljoin(base_url, href)
            links.append(full)
    return links


# ── Main crawler ──────────────────────────────────────────────────
def crawl(seed_urls: list[str], max_pages: int = MAX_PAGES) -> None:
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    robot_parser = build_robot_parser("https://en.wikipedia.org")

    visited   = set()
    queue     = list(seed_urls)
    collected = 0

    with open(OUTPUT_FILE, "w", encoding="utf-8") as fout:
        while queue and collected < max_pages:
            url = queue.pop(0)

            if url in visited:
                continue
            visited.add(url)

            # Respect robots.txt (skip check if parser unavailable)
            if robot_parser and not robot_parser.can_fetch(USER_AGENT, url):
                log.warning("Blocked by robots.txt: %s", url)
                continue

            try:
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
            except requests.RequestException as exc:
                log.error("Failed to fetch %s — %s", url, exc)
                continue

            html = resp.text
            text = extract_text(html, url)

            if not text or len(text.strip()) < 200:
                log.info("Skipped (too short): %s", url)
                time.sleep(CRAWL_DELAY)
                continue

            # Build page title from URL slug
            slug  = urlparse(url).path.split("/wiki/")[-1]
            title = slug.replace("_", " ")

            record = {
                "url":   url,
                "title": title,
                "text":  text.strip(),
            }
            fout.write(json.dumps(record, ensure_ascii=False) + "\n")
            collected += 1
            log.info("[%d/%d] Saved: %s", collected, max_pages, title)

            # Discover and enqueue new links from seed pages only
            if url in seed_urls and collected < max_pages:
                new_links = discover_links(html, url)
                for link in new_links[:8]:   # limit expansion per page
                    if link not in visited:
                        queue.append(link)

            time.sleep(CRAWL_DELAY)

    log.info("Crawl complete. %d pages saved to %s", collected, OUTPUT_FILE)


if __name__ == "__main__":
    crawl(SEED_URLS)
