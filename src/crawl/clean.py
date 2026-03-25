import json
import logging
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────
RAW_FILE     = Path(__file__).parent.parent.parent / "data" / "raw" / "crawler_output.jsonl"
CLEANED_FILE = Path(__file__).parent.parent.parent / "data" / "raw" / "crawler_cleaned.jsonl"

MIN_TEXT_LENGTH = 500   # characters

# Pages whose titles contain any of these strings are discarded
TITLE_BLOCKLIST = [
    "disambiguation",
    "baseball",
    "base ball",
    "football",
    "soccer",
    "hockey",
    "cricket",
    "rugby",
    "tennis",
    "golf",
]

# At least one of these must appear in the text (case-insensitive)
NBA_KEYWORDS = [
    "nba", "basketball", "player", "team", "season", "coach",
    "draft", "championship", "finals", "points", "rebounds",
    "assists", "playoffs", "roster", "league",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def is_relevant(record: dict) -> tuple[bool, str]:
    title = record.get("title", "").lower()
    text  = record.get("text",  "").lower()

    # Filter 1: blocked title keywords
    for word in TITLE_BLOCKLIST:
        if word in title:
            return False, f"blocked title keyword: '{word}'"

    # Filter 2: minimum text length
    if len(text) < MIN_TEXT_LENGTH:
        return False, f"text too short ({len(text)} chars)"

    # Filter 3: must contain at least one NBA keyword
    if not any(kw in text for kw in NBA_KEYWORDS):
        return False, "no NBA keywords found"

    return True, "ok"


def clean() -> None:
    if not RAW_FILE.exists():
        log.error("Raw file not found: %s", RAW_FILE)
        return

    total    = 0
    kept     = 0
    dropped  = 0

    with open(RAW_FILE, encoding="utf-8") as fin, \
         open(CLEANED_FILE, "w", encoding="utf-8") as fout:

        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            record = json.loads(line)
            ok, reason = is_relevant(record)

            if ok:
                fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                kept += 1
            else:
                log.info("Dropped [%s]: %s", reason, record.get("title", ""))
                dropped += 1

    log.info("Cleaning complete: %d kept, %d dropped (total %d)", kept, dropped, total)
    log.info("Cleaned file: %s", CLEANED_FILE)


if __name__ == "__main__":
    clean()
