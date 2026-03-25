import csv
import logging
import re
from pathlib import Path

from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal, URIRef
from rdflib.namespace import XSD

# ── Namespaces ────────────────────────────────────────────────────
NBA  = Namespace("http://nba-kg.org/ontology#")
RES  = Namespace("http://nba-kg.org/resource/")
PROV = Namespace("http://www.w3.org/ns/prov#")

# ── Paths ─────────────────────────────────────────────────────────
CSV_FILE = Path(__file__).parent.parent.parent / "data" / "processed" / "extracted_knowledge.csv"
OUT_FILE = Path(__file__).parent.parent.parent / "kg_artifacts" / "initial_kg.ttl"

# ── Known NBA teams (surface form → canonical label) ─────────────
NBA_TEAMS = {
    # Current teams
    "lakers", "los angeles lakers",
    "warriors", "golden state warriors",
    "celtics", "boston celtics",
    "bulls", "chicago bulls",
    "heat", "miami heat",
    "spurs", "san antonio spurs",
    "mavericks", "dallas mavericks",
    "nets", "brooklyn nets",
    "suns", "phoenix suns",
    "bucks", "milwaukee bucks",
    "nuggets", "denver nuggets",
    "76ers", "sixers", "philadelphia 76ers",
    "thunder", "oklahoma city thunder", "okc thunder",
    "cavaliers", "cavs", "cleveland cavaliers",
    "raptors", "toronto raptors",
    "knicks", "new york knicks",
    "clippers", "los angeles clippers",
    "grizzlies", "memphis grizzlies",
    "timberwolves", "minnesota timberwolves",
    "pelicans", "new orleans pelicans",
    "magic", "orlando magic",
    "hawks", "atlanta hawks",
    "hornets", "charlotte hornets",
    "pistons", "detroit pistons",
    "pacers", "indiana pacers",
    "wizards", "washington wizards",
    "jazz", "utah jazz",
    "trail blazers", "blazers", "portland trail blazers",
    "kings", "sacramento kings",
    "rockets", "houston rockets",
    # Historical / relocated teams
    "supersonics", "seattle supersonics",
    "new jersey nets",
    "new orleans hornets",
    "charlotte bobcats",
    "vancouver grizzlies",
    "washington bullets",
    "new jersey nets",
    "kansas city kings",
    "san diego clippers",
    "san diego rockets",
    "buffalo braves",
    "new orleans jazz",
    "new jersey americans",
    "chicago packers", "chicago zephyrs",
    "baltimore bullets", "capital bullets",
    "cincinnati royals",
    "fort wayne pistons",
    "minneapolis lakers",
    "rochester royals",
    "tri-cities blackhawks",
    "st. louis hawks",
    "philadelphia warriors",
    "san francisco warriors",
    "chicago bulls",
    "nba",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def slugify(text: str) -> str:
    text = text.strip().replace(" ", "_")
    text = re.sub(r"[^\w\-]", "", text)
    return text


def entity_uri(surface: str) -> URIRef:
    return RES[slugify(surface)]


def assign_type(surface: str, ner_type: str) -> URIRef | None:
    low = surface.lower()
    if ner_type == "PERSON":
        return NBA.Player
    if ner_type == "ORG":
        return NBA.Team if low in NBA_TEAMS else NBA.Organization
    if ner_type == "GPE":
        return NBA.City
    if ner_type == "EVENT":
        return NBA.Season
    if ner_type == "WORK_OF_ART":
        return NBA.Award
    return None


def build_kg() -> Graph:
    g = Graph()
    g.bind("nba",  NBA)
    g.bind("res",  RES)
    g.bind("owl",  OWL)
    g.bind("rdfs", RDFS)
    g.bind("prov", PROV)

    if not CSV_FILE.exists():
        log.error("CSV not found: %s", CSV_FILE)
        return g

    seen_entities: dict[URIRef, URIRef] = {}   # uri → rdf:type

    with open(CSV_FILE, encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        rows   = list(reader)

    log.info("Building KG from %d NER records ...", len(rows))

    for row in rows:
        surface  = row["entity"].strip()
        ner_type = row["type"].strip()
        source   = row["source_url"].strip()

        uri      = entity_uri(surface)
        rdf_type = assign_type(surface, ner_type)

        if rdf_type is None:
            continue

        # Add entity + type (only once per URI)
        if uri not in seen_entities:
            g.add((uri, RDF.type,    rdf_type))
            g.add((uri, RDFS.label,  Literal(surface, lang="en")))
            seen_entities[uri] = rdf_type

        # Provenance: entity mentioned in source document
        if source:
            source_uri = URIRef(source)
            g.add((uri, PROV.wasDerivedFrom, source_uri))

    # Add Organisation class (used for non-team ORGs)
    g.add((NBA.Organization, RDF.type,         OWL.Class))
    g.add((NBA.Organization, RDFS.subClassOf,   OWL.Thing))

    log.info("Initial KG: %d entities, %d triples", len(seen_entities), len(g))
    return g


def save(g: Graph) -> None:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    g.serialize(destination=str(OUT_FILE), format="turtle")
    log.info("Saved initial KG: %s", OUT_FILE)


if __name__ == "__main__":
    g = build_kg()
    save(g)
    print(f"\nInitial KG: {len(g)} triples")
