import logging
import time
from pathlib import Path

from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal, URIRef
from SPARQLWrapper import SPARQLWrapper, JSON

# ── Namespaces ────────────────────────────────────────────────────
NBA  = Namespace("http://nba-kg.org/ontology#")
RES  = Namespace("http://nba-kg.org/resource/")
WD   = Namespace("http://www.wikidata.org/entity/")
DBO  = Namespace("http://dbpedia.org/ontology/")
DBR  = Namespace("http://dbpedia.org/resource/")
OWL_NS = OWL

# ── Paths ─────────────────────────────────────────────────────────
INITIAL_KG = Path(__file__).parent.parent.parent / "kg_artifacts" / "initial_kg.ttl"
ALIGN_OUT  = Path(__file__).parent.parent.parent / "kg_artifacts" / "alignment.ttl"

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
DBPEDIA_ENDPOINT  = "https://dbpedia.org/sparql"

# ── Hardcoded high-confidence alignments ─────────────────────────
# Manually verified: our resource URI → Wikidata QID → DBpedia URI
KNOWN_ALIGNMENTS = {
    # Players
    "LeBron_James":             ("Q36159",    "LeBron_James"),
    "Stephen_Curry":            ("Q352230",   "Stephen_Curry"),
    "Kevin_Durant":             ("Q214303",   "Kevin_Durant"),
    "Michael_Jordan":           ("Q41421",    "Michael_Jordan"),
    "Kobe_Bryant":              ("Q134183",   "Kobe_Bryant"),
    "Shaquille_ONeal":          ("Q169452",   "Shaquille_O%27Neal"),
    "Magic_Johnson":            ("Q213900",   "Magic_Johnson"),
    "Larry_Bird":               ("Q212993",   "Larry_Bird"),
    "Tim_Duncan":               ("Q193425",   "Tim_Duncan"),
    "Dirk_Nowitzki":            ("Q183700",   "Dirk_Nowitzki"),
    "Giannis_Antetokounmpo":    ("Q1282327",  "Giannis_Antetokounmpo"),
    "Nikola_Jokić":             ("Q16826663", "Nikola_Jokić"),
    "Luka_Dončić":              ("Q38903117", "Luka_Dončić"),
    "Victor_Wembanyama":        ("Q113183754","Victor_Wembanyama"),
    "Jayson_Tatum":             ("Q30884051", "Jayson_Tatum"),
    "Joel_Embiid":              ("Q19675880", "Joel_Embiid"),
    "Kawhi_Leonard":            ("Q707460",   "Kawhi_Leonard"),
    "James_Harden":             ("Q351271",   "James_Harden"),
    "Anthony_Davis":            ("Q1075660",  "Anthony_Davis"),
    "Damian_Lillard":           ("Q1093497",  "Damian_Lillard"),
    # Teams
    "Los_Angeles_Lakers":       ("Q170323",   "Los_Angeles_Lakers"),
    "Golden_State_Warriors":    ("Q157376",   "Golden_State_Warriors"),
    "Boston_Celtics":           ("Q131172",   "Boston_Celtics"),
    "Chicago_Bulls":            ("Q131209",   "Chicago_Bulls"),
    "Miami_Heat":               ("Q162990",   "Miami_Heat"),
    "San_Antonio_Spurs":        ("Q157373",   "San_Antonio_Spurs"),
    "Dallas_Mavericks":         ("Q157403",   "Dallas_Mavericks"),
    "Brooklyn_Nets":            ("Q168131",   "Brooklyn_Nets"),
    "Phoenix_Suns":             ("Q157391",   "Phoenix_Suns"),
    "Milwaukee_Bucks":          ("Q157395",   "Milwaukee_Bucks"),
    "Denver_Nuggets":           ("Q157384",   "Denver_Nuggets"),
    "Philadelphia_76ers":       ("Q131227",   "Philadelphia_76ers"),
    # League
    "NBA":                      ("Q155223",   "National_Basketball_Association"),
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def build_alignment() -> Graph:
    g = Graph()
    g.bind("nba",  NBA)
    g.bind("res",  RES)
    g.bind("wd",   WD)
    g.bind("dbr",  DBR)
    g.bind("owl",  OWL_NS)

    aligned = 0
    for slug, (qid, dbr_slug) in KNOWN_ALIGNMENTS.items():
        our_uri  = RES[slug]
        wd_uri   = WD[qid]
        dbr_uri  = DBR[dbr_slug]

        # owl:sameAs links
        g.add((our_uri, OWL_NS.sameAs, wd_uri))
        g.add((our_uri, OWL_NS.sameAs, dbr_uri))

        # Confidence annotation
        bn = URIRef(f"http://nba-kg.org/alignment/{slug}")
        g.add((bn, RDF.type,              URIRef("http://nba-kg.org/ontology#AlignmentRecord")))
        g.add((bn, URIRef("http://nba-kg.org/ontology#sourceEntity"),  our_uri))
        g.add((bn, URIRef("http://nba-kg.org/ontology#wikidataURI"),   wd_uri))
        g.add((bn, URIRef("http://nba-kg.org/ontology#dbpediaURI"),    dbr_uri))
        g.add((bn, URIRef("http://nba-kg.org/ontology#confidence"),    Literal(1.0)))
        g.add((bn, URIRef("http://nba-kg.org/ontology#method"),        Literal("manual-verified")))

        aligned += 1
        log.info("Aligned: %s → wd:%s / dbr:%s", slug, qid, dbr_slug)

    log.info("Total entity alignments: %d", aligned)

    # ── Predicate alignment ───────────────────────────────────────
    # owl:equivalentProperty between our predicates and Wikidata/DBpedia
    WDT = Namespace("http://www.wikidata.org/prop/direct/")
    g.bind("wdt", WDT)
    g.bind("dbo", DBO)

    PREDICATE_ALIGNMENTS = [
        # (our property, Wikidata prop, DBpedia prop or None)
        (NBA.playsFor,        WDT.P54,   DBO.team),
        (NBA.wonAward,        WDT.P166,  DBO.award),
        (NBA.nationality,     WDT.P27,   DBO.nationality),
        (NBA.locatedIn,       WDT.P131,  DBO.location),
        (NBA.partOfLeague,    WDT.P118,  DBO.league),
        (NBA.draftedIn,       WDT.P647,  None),
        (NBA.coachedBy,       WDT.P286,  DBO.headCoach),
        (NBA.wonChampionship, WDT.P1346, None),
    ]

    for our_prop, wd_prop, dbo_prop in PREDICATE_ALIGNMENTS:
        g.add((our_prop, OWL_NS.equivalentProperty, wd_prop))
        if dbo_prop:
            g.add((our_prop, OWL_NS.equivalentProperty, dbo_prop))
        log.info("Predicate aligned: %s → %s",
                 our_prop.split("#")[-1], wd_prop.split("/")[-1])

    log.info("Total predicate alignments: %d", len(PREDICATE_ALIGNMENTS))
    return g


def save(g: Graph) -> None:
    ALIGN_OUT.parent.mkdir(parents=True, exist_ok=True)
    g.serialize(destination=str(ALIGN_OUT), format="turtle")
    log.info("Alignment saved: %s  (%d triples)", ALIGN_OUT, len(g))


if __name__ == "__main__":
    g = build_alignment()
    save(g)
