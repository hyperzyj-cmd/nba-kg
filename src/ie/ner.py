import json
import csv
import logging
from pathlib import Path

import spacy

# ── Config ────────────────────────────────────────────────────────
CLEANED_FILE = Path(__file__).parent.parent.parent / "data" / "raw" / "crawler_cleaned.jsonl"
OUTPUT_CSV   = Path(__file__).parent.parent.parent / "data" / "processed" / "extracted_knowledge.csv"

# Entity types to keep
KEEP_TYPES = {"PERSON", "ORG", "GPE", "DATE", "EVENT", "WORK_OF_ART"}

# Minimum entity text length to avoid noise
MIN_ENTITY_LENGTH = 2

# Known ambiguous surface forms (documented for report)
AMBIGUOUS_FORMS = {
    "jordan":  "PERSON (Michael Jordan) vs GPE (country Jordan) vs GPE (Jordan, Utah)",
    "heat":    "ORG (Miami Heat) vs common noun (weather)",
    "magic":   "PERSON (Magic Johnson) vs ORG (Orlando Magic)",
    "nets":    "ORG (Brooklyn Nets) vs common noun (fishing nets)",
    "thunder": "ORG (Oklahoma City Thunder) vs common noun (weather)",
    "bulls":   "ORG (Chicago Bulls) vs common noun (animals)",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def load_model() -> spacy.Language:
    log.info("Loading spaCy model en_core_web_trf ...")
    nlp = spacy.load("en_core_web_trf")
    log.info("Model loaded.")
    return nlp


def extract_entities(nlp: spacy.Language) -> list[dict]:
    if not CLEANED_FILE.exists():
        log.error("Cleaned file not found: %s", CLEANED_FILE)
        return []

    records = []
    seen    = set()   # (entity_text_lower, label, url) dedup key

    with open(CLEANED_FILE, encoding="utf-8") as fin:
        pages = [json.loads(line) for line in fin if line.strip()]

    log.info("Processing %d pages ...", len(pages))

    for i, page in enumerate(pages):
        url   = page.get("url", "")
        title = page.get("title", "")
        text  = page.get("text", "")

        # Truncate to first 5000 chars to keep runtime reasonable
        doc = nlp(text[:5000])

        for ent in doc.ents:
            label = ent.label_
            if label not in KEEP_TYPES:
                continue

            surface = ent.text.strip()
            if len(surface) < MIN_ENTITY_LENGTH:
                continue

            # Context: sentence containing the entity
            context = ent.sent.text.strip().replace("\n", " ")[:200]

            # Ambiguity flag
            ambiguous = surface.lower() in AMBIGUOUS_FORMS
            ambiguity_note = AMBIGUOUS_FORMS.get(surface.lower(), "")

            dedup_key = (surface.lower(), label, url)
            if dedup_key in seen:
                continue
            seen.add(dedup_key)

            records.append({
                "entity":         surface,
                "type":           label,
                "source_url":     url,
                "source_title":   title,
                "context":        context,
                "ambiguous":      ambiguous,
                "ambiguity_note": ambiguity_note,
            })

        if (i + 1) % 10 == 0:
            log.info("  Processed %d / %d pages", i + 1, len(pages))

    return records


def save_csv(records: list[dict]) -> None:
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["entity", "type", "source_url", "source_title",
                  "context", "ambiguous", "ambiguity_note"]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    log.info("Saved %d entity records to %s", len(records), OUTPUT_CSV)


def print_summary(records: list[dict]) -> None:
    from collections import Counter
    type_counts = Counter(r["type"] for r in records)
    ambiguous   = [r for r in records if r["ambiguous"]]

    print("\n── Entity Type Summary ──────────────────────")
    for label, count in type_counts.most_common():
        print(f"  {label:<15} {count}")
    print(f"  {'TOTAL':<15} {len(records)}")

    print("\n── Ambiguous Entities Found ─────────────────")
    seen_forms = set()
    for r in ambiguous:
        form = r["entity"].lower()
        if form not in seen_forms:
            print(f"  '{r['entity']}' → {r['ambiguity_note']}")
            seen_forms.add(form)


if __name__ == "__main__":
    nlp     = load_model()
    records = extract_entities(nlp)
    save_csv(records)
    print_summary(records)
