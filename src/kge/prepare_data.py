"""
Prepare KGE training data from expanded.nt.
Extracts entity-entity triples, shortens URIs, splits 80/10/10.
"""
import random
import csv
import json
from pathlib import Path
from rdflib import Graph, URIRef

ROOT      = Path(__file__).parent.parent.parent
EXPANDED  = ROOT / "kg_artifacts" / "expanded.nt"
OUT_DIR   = ROOT / "data" / "kge"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SEED = 42
random.seed(SEED)


def shorten(uri: str) -> str:
    """Convert full URI to short readable ID."""
    if "wikidata.org/entity/" in uri:
        return uri.split("/entity/")[-1]
    if "wikidata.org/prop/direct/" in uri:
        return uri.split("/direct/")[-1]
    if "nba-kg.org/resource/" in uri:
        return uri.split("/resource/")[-1]
    if "nba-kg.org/ontology#" in uri:
        return uri.split("#")[-1]
    if "dbpedia.org/resource/" in uri:
        return "dbr:" + uri.split("/resource/")[-1]
    if "dbpedia.org/ontology/" in uri:
        return "dbo:" + uri.split("/ontology/")[-1]
    # fallback: last path/fragment segment
    return uri.rstrip("/").split("/")[-1].split("#")[-1]


def main():
    print(f"Loading {EXPANDED.name} ...")
    g = Graph()
    g.parse(str(EXPANDED), format="nt")
    print(f"  Total triples: {len(g)}")

    # Keep only entity-entity triples (no literals)
    triples = [
        (shorten(str(s)), shorten(str(p)), shorten(str(o)))
        for s, p, o in g
        if isinstance(s, URIRef) and isinstance(p, URIRef) and isinstance(o, URIRef)
    ]
    print(f"  Entity-entity triples: {len(triples)}")

    # Remove self-loops and duplicates
    triples = list({t for t in triples if t[0] != t[2]})
    print(f"  After dedup/self-loop removal: {len(triples)}")

    random.shuffle(triples)

    n      = len(triples)
    n_test = int(n * 0.1)
    n_val  = int(n * 0.1)

    test_set  = triples[:n_test]
    valid_set = triples[n_test: n_test + n_val]
    train_set = triples[n_test + n_val:]

    print(f"  Train: {len(train_set)}  Valid: {len(valid_set)}  Test: {len(test_set)}")

    def write_tsv(path, rows):
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerows(rows)

    write_tsv(OUT_DIR / "train.tsv", train_set)
    write_tsv(OUT_DIR / "valid.tsv", valid_set)
    write_tsv(OUT_DIR / "test.tsv",  test_set)

    # Save stats
    stats = {
        "total": len(triples),
        "train": len(train_set),
        "valid": len(valid_set),
        "test":  len(test_set),
        "entities":  len({t[0] for t in triples} | {t[2] for t in triples}),
        "relations": len({t[1] for t in triples}),
    }
    (OUT_DIR / "stats.json").write_text(json.dumps(stats, indent=2))

    print("\nSaved:")
    for k, v in stats.items():
        print(f"  {k:<12}: {v}")
    print(f"\nOutput: {OUT_DIR}")


if __name__ == "__main__":
    main()
