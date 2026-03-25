"""
KB Expansion via Wikidata SPARQL - batch mode.
Fetches all NBA players/teams/awards in bulk instead of entity-by-entity.
"""
import logging
import time
from pathlib import Path

from rdflib import Graph, URIRef, Literal as RDFLiteral
from SPARQLWrapper import SPARQLWrapper, JSON

# ── Paths ─────────────────────────────────────────────────────────
ROOT         = Path(__file__).parent.parent.parent
INITIAL_KG   = ROOT / "kg_artifacts" / "initial_kg.ttl"
ALIGN_FILE   = ROOT / "kg_artifacts" / "alignment.ttl"
EXPAND_OUT   = ROOT / "kg_artifacts" / "expanded.nt"

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
BATCH_SIZE        = 30    # entities per SPARQL VALUES block
BATCH_DELAY       = 2.5   # seconds between batch requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Fallback seed QIDs (used if live discovery fails) ─────────────
SEED_QIDS = [
    "Q36159","Q352230","Q214303","Q41421","Q134183","Q169452",
    "Q212993","Q193425","Q183700","Q1282327","Q16826663","Q38903117",
    "Q113183754","Q30884051","Q19675880","Q707460","Q351271","Q1075660",
    "Q1093497","Q189019","Q193569","Q185739","Q221222","Q201608",
    "Q213812","Q181163","Q215848","Q216400",
    "Q170323","Q157376","Q131172","Q131209","Q162990","Q157373",
    "Q157403","Q168131","Q157391","Q157395","Q157384","Q131227",
    "Q155223",
]


def run_query(sparql: SPARQLWrapper, query: str) -> list[dict]:
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    try:
        return sparql.query().convert()["results"]["bindings"]
    except Exception as exc:
        log.warning("Query failed: %s", exc)
        return []


def discover_nba_qids(sparql: SPARQLWrapper) -> list[str]:
    """Fetch QIDs for all NBA players, teams, and awards from Wikidata."""
    queries = {
        "players": """
            SELECT DISTINCT ?e WHERE {
              ?e wdt:P641 wd:Q5372 ;
                 wdt:P118 wd:Q155223 ;
                 wdt:P31  wd:Q5 .
            } LIMIT 600
        """,
        "teams": """
            SELECT DISTINCT ?e WHERE {
              ?e wdt:P118 wd:Q155223 .
              ?e wdt:P31/wdt:P279* wd:Q847017 .
            } LIMIT 40
        """,
        "awards": """
            SELECT DISTINCT ?e WHERE {
              ?e wdt:P361 wd:Q155223 .
              ?e wdt:P31/wdt:P279* wd:Q19020 .
            } LIMIT 40
        """,
    }

    qids: set[str] = set(SEED_QIDS)
    for label, q in queries.items():
        rows = run_query(sparql, q)
        found = [r["e"]["value"].split("/")[-1] for r in rows if r["e"]["value"].split("/")[-1].startswith("Q")]
        log.info("Discovered %d %s QIDs", len(found), label)
        qids.update(found)
        time.sleep(BATCH_DELAY)

    return list(qids)


def batch_fetch_triples(sparql: SPARQLWrapper, qids: list[str]) -> list[tuple]:
    """Fetch all wdt: triples for a batch of QIDs in one request."""
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
    SELECT ?s ?p ?o WHERE {{
      VALUES ?s {{ {values} }}
      ?s ?p ?o .
      FILTER(STRSTARTS(STR(?p), "http://www.wikidata.org/prop/direct/"))
    }}
    """
    rows = run_query(sparql, query)
    triples: list[tuple] = []
    for row in rows:
        s = URIRef(row["s"]["value"])
        p = URIRef(row["p"]["value"])
        o_val = row["o"]
        if o_val["type"] == "uri":
            o = URIRef(o_val["value"])
        else:
            try:
                lang     = o_val.get("xml:lang")
                datatype = o_val.get("datatype")
                if lang:
                    o = RDFLiteral(o_val["value"], lang=lang)
                elif datatype:
                    o = RDFLiteral(o_val["value"], datatype=URIRef(datatype))
                else:
                    o = RDFLiteral(o_val["value"])
            except (ValueError, OverflowError):
                continue
        triples.append((s, p, o))
    return triples


def expand() -> None:
    EXPAND_OUT.parent.mkdir(parents=True, exist_ok=True)

    sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql.addCustomHttpHeader("User-Agent", "nba-kg-expander/1.0 (academic project)")

    # ── Load existing graphs ──────────────────────────────────────
    g = Graph()
    g.parse(str(INITIAL_KG), format="turtle")
    log.info("Loaded initial KG: %d triples", len(g))
    g.parse(str(ALIGN_FILE), format="turtle")
    log.info("Loaded alignment: %d triples total", len(g))

    # ── Discover entities ─────────────────────────────────────────
    log.info("Discovering NBA entities from Wikidata ...")
    all_qids = discover_nba_qids(sparql)
    log.info("Total entities to expand: %d", len(all_qids))

    # ── Batch fetch ───────────────────────────────────────────────
    total_batches = (len(all_qids) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(all_qids), BATCH_SIZE):
        batch  = all_qids[i: i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        triples = batch_fetch_triples(sparql, batch)
        for t in triples:
            g.add(t)
        log.info("Batch %d/%d: +%d triples (total %d)",
                 batch_num, total_batches, len(triples), len(g))
        time.sleep(BATCH_DELAY)

    log.info("Expansion complete: %d triples", len(g))

    # ── Save ──────────────────────────────────────────────────────
    g.serialize(destination=str(EXPAND_OUT), format="nt")
    log.info("Saved: %s  (%d triples)", EXPAND_OUT, len(g))


if __name__ == "__main__":
    expand()
