from pathlib import Path
from rdflib import Graph, Namespace, RDF, RDFS, OWL, XSD, Literal

# ── Namespaces ────────────────────────────────────────────────────
NBA  = Namespace("http://nba-kg.org/ontology#")
OUT  = Path(__file__).parent.parent.parent / "kg_artifacts" / "ontology.ttl"


def build_ontology() -> Graph:
    g = Graph()
    g.bind("nba",  NBA)
    g.bind("owl",  OWL)
    g.bind("rdfs", RDFS)
    g.bind("xsd",  XSD)

    # ── Classes ───────────────────────────────────────────────────
    classes = {
        NBA.Player:       "An NBA player",
        NBA.Team:         "An NBA franchise",
        NBA.League:       "A basketball league",
        NBA.Season:       "An NBA season or playoff series",
        NBA.Award:        "An individual or team award",
        NBA.City:         "A city that hosts an NBA team",
        NBA.Country:      "A country of origin or nationality",
        NBA.Coach:        "A head coach or assistant coach",
        NBA.Draft:        "An NBA draft event",
        NBA.Conference:   "An NBA conference (Eastern / Western)",
        NBA.Division:     "An NBA division within a conference",
    }
    for cls, comment in classes.items():
        g.add((cls, RDF.type,        OWL.Class))
        g.add((cls, RDFS.comment,    Literal(comment)))

    # ── Subclass relations ────────────────────────────────────────
    g.add((NBA.Player, RDFS.subClassOf, NBA.Person))
    g.add((NBA.Coach,  RDFS.subClassOf, NBA.Person))
    g.add((NBA.Person, RDF.type,        OWL.Class))

    # ── Object properties ─────────────────────────────────────────
    obj_props = {
        NBA.playsFor:        (NBA.Player,  NBA.Team,       "Player currently or formerly on a roster"),
        NBA.wonChampionship: (NBA.Player,  NBA.Season,     "Player won NBA championship in that season"),
        NBA.wonAward:        (NBA.Player,  NBA.Award,      "Player received an award"),
        NBA.locatedIn:       (NBA.Team,    NBA.City,       "Team's home city"),
        NBA.partOfLeague:    (NBA.Team,    NBA.League,     "Team competes in this league"),
        NBA.partOfConference:(NBA.Team,    NBA.Conference, "Team belongs to this conference"),
        NBA.partOfDivision:  (NBA.Team,    NBA.Division,   "Team belongs to this division"),
        NBA.coachedBy:       (NBA.Team,    NBA.Coach,      "Team's head coach"),
        NBA.draftedIn:       (NBA.Player,  NBA.Draft,      "Player was selected in this draft"),
        NBA.nationality:     (NBA.Player,  NBA.Country,    "Player's nationality"),
        NBA.teammateOf:      (NBA.Player,  NBA.Player,     "Two players on the same roster"),
    }
    for prop, (domain, range_, comment) in obj_props.items():
        g.add((prop, RDF.type,        OWL.ObjectProperty))
        g.add((prop, RDFS.domain,     domain))
        g.add((prop, RDFS.range,      range_))
        g.add((prop, RDFS.comment,    Literal(comment)))

    # ── Datatype properties ───────────────────────────────────────
    data_props = {
        NBA.birthDate:    (NBA.Player, XSD.date,    "Player's birth date"),
        NBA.birthPlace:   (NBA.Player, XSD.string,  "Player's birth place"),
        NBA.height:       (NBA.Player, XSD.float,   "Player height in metres"),
        NBA.draftYear:    (NBA.Player, XSD.integer, "Year the player was drafted"),
        NBA.draftPick:    (NBA.Player, XSD.integer, "Overall pick number in the draft"),
        NBA.position:     (NBA.Player, XSD.string,  "Playing position"),
        NBA.jerseyNumber: (NBA.Player, XSD.integer, "Jersey number"),
        NBA.founded:      (NBA.Team,   XSD.integer, "Year the franchise was founded"),
        NBA.arena:        (NBA.Team,   XSD.string,  "Name of home arena"),
    }
    for prop, (domain, range_, comment) in data_props.items():
        g.add((prop, RDF.type,        OWL.DatatypeProperty))
        g.add((prop, RDFS.domain,     domain))
        g.add((prop, RDFS.range,      range_))
        g.add((prop, RDFS.comment,    Literal(comment)))

    # ── Symmetric property ────────────────────────────────────────
    g.add((NBA.teammateOf, RDF.type, OWL.SymmetricProperty))

    return g


def save(g: Graph) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    g.serialize(destination=str(OUT), format="turtle")
    print(f"Ontology saved: {OUT}  ({len(g)} triples)")


if __name__ == "__main__":
    g = build_ontology()
    save(g)
