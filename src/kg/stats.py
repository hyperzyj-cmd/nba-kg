from pathlib import Path
from collections import Counter
from rdflib import Graph, RDF, OWL, URIRef

KG_FILES = {
    "ontology":   Path(__file__).parent.parent.parent / "kg_artifacts" / "ontology.ttl",
    "initial_kg": Path(__file__).parent.parent.parent / "kg_artifacts" / "initial_kg.ttl",
    "alignment":  Path(__file__).parent.parent.parent / "kg_artifacts" / "alignment.ttl",
    "expanded":   Path(__file__).parent.parent.parent / "kg_artifacts" / "expanded.nt",
}


def load(path: Path, fmt: str) -> Graph:
    g = Graph()
    if path.exists():
        g.parse(str(path), format=fmt)
    return g


def stats_for(name: str, g: Graph) -> None:
    subjects   = set(s for s, _, _ in g if isinstance(s, URIRef))
    predicates = set(p for _, p, _ in g)
    objects    = set(o for _, _, o in g if isinstance(o, URIRef))
    entities   = subjects | objects

    type_counts = Counter(str(o) for _, p, o in g if str(p) == str(RDF.type))

    print(f"\n── {name.upper()} {'─' * (40 - len(name))}")
    print(f"  Triples     : {len(g):>8,}")
    print(f"  Entities    : {len(entities):>8,}")
    print(f"  Predicates  : {len(predicates):>8,}")
    if type_counts:
        print("  Top types:")
        for t, c in type_counts.most_common(8):
            short = t.split("/")[-1].split("#")[-1]
            print(f"    {short:<30} {c}")


def main() -> None:
    print("=" * 50)
    print("  NBA-KG  —  Knowledge Base Statistics")
    print("=" * 50)

    fmts = {
        "ontology":   "turtle",
        "initial_kg": "turtle",
        "alignment":  "turtle",
        "expanded":   "nt",
    }

    total_triples = 0
    for name, path in KG_FILES.items():
        if not path.exists():
            print(f"\n[MISSING] {name}: {path}")
            continue
        g = load(path, fmts[name])
        stats_for(name, g)
        total_triples += len(g)

    print(f"\n{'=' * 50}")
    print(f"  COMBINED TOTAL: {total_triples:,} triples")
    print(f"{'=' * 50}\n")


if __name__ == "__main__":
    main()
