"""
Evaluate trained KGE models (TransE, DistMult) on the NBA knowledge graph.
- Reads metrics directly from each model's saved results.json (no retraining).
- Generates t-SNE visualisations with entities coloured by ontology class.
- Shows nearest neighbours for selected NBA entities.
"""
import io
import sys
import json
import numpy as np
import matplotlib

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from pathlib import Path

import torch
from rdflib import Graph, RDF, URIRef
from sklearn.manifold import TSNE
from sklearn.preprocessing import normalize
from adjustText import adjust_text
from pykeen.triples import TriplesFactory

ROOT    = Path(__file__).parent.parent.parent
RES_DIR = Path(__file__).parent / "results"
FIG_DIR = Path(__file__).parent / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

HIGHLIGHT = [
    "LeBron_James", "Stephen_Curry", "Nikola_Jokic",
    "Kevin_Durant", "Giannis_Antetokounmpo",
    "Los_Angeles_Lakers", "Golden_State_Warriors",
    "Boston_Celtics", "Denver_Nuggets",
]

TOP_K = 5

# Colour palette per ontology class
CLASS_COLORS = {
    "Player":  "#e74c3c",
    "Team":    "#3498db",
    "Award":   "#f39c12",
    "Season":  "#2ecc71",
    "City":    "#9b59b6",
    "League":  "#1abc9c",
    "Other":   "#bdc3c7",
}

ONTOLOGY_NS  = "http://nba-kg.org/ontology#"
ENTITY_NS    = "http://nba-kg.org/resource/"


# ── entity class loading ───────────────────────────────────────────────────────

def shorten(uri: str) -> str:
    if "wikidata.org/entity/" in uri:
        return uri.split("/entity/")[-1]
    if "wikidata.org/prop/direct/" in uri:
        return uri.split("/direct/")[-1]
    if "#" in uri:
        return uri.split("#")[-1]
    return uri.split("/")[-1]


def load_entity_classes(kg_path: Path) -> dict:
    """Return mapping  short_entity_name -> class_label.

    Builds from initial_kg.ttl (local resource names) and then extends with
    Wikidata QIDs using alignment.ttl so that the KGE model entities (Wikidata
    QIDs) also get colour-coded correctly.
    """
    if not kg_path.exists():
        return {}

    NBA_NS = "http://nba-kg.org/ontology#"
    SOURCE_PRED = URIRef(NBA_NS + "sourceEntity")
    WD_PRED     = URIRef(NBA_NS + "wikidataURI")

    g = Graph()
    g.parse(str(kg_path), format="turtle")

    # local resource name → class label
    mapping = {}
    for s, _, o in g.triples((None, RDF.type, None)):
        if not isinstance(s, URIRef) or not isinstance(o, URIRef):
            continue
        cls = str(o).replace(ONTOLOGY_NS, "")
        if cls not in CLASS_COLORS:
            continue
        mapping[shorten(str(s))] = cls

    # Try to load alignment.ttl for local → Wikidata QID bridge
    align_path = kg_path.parent / "alignment.ttl"
    if align_path.exists():
        ag = Graph()
        ag.parse(str(align_path), format="turtle")
        for rec in ag.subjects(RDF.type, URIRef(NBA_NS + "AlignmentRecord")):
            src_list = list(ag.objects(rec, SOURCE_PRED))
            wd_list  = list(ag.objects(rec, WD_PRED))
            if not src_list or not wd_list:
                continue
            local_name = shorten(str(src_list[0]))
            qid        = shorten(str(wd_list[0]))
            if local_name in mapping:
                mapping[qid] = mapping[local_name]

    print(f"Loaded {len(mapping)} entity-class mappings from KB.")
    return mapping


# ── metric reading ─────────────────────────────────────────────────────────────

def read_metrics_from_dir(run_dir: Path) -> dict:
    rjson = run_dir / "results.json"
    if not rjson.exists():
        return {"MRR": None, "Hits@1": None, "Hits@3": None, "Hits@10": None}

    data = json.loads(rjson.read_text(encoding="utf-8"))

    def dig(d, *keys):
        for k in keys:
            if not isinstance(d, dict):
                return None
            d = d.get(k)
        return d

    base = dig(data, "metrics", "both", "realistic")
    if base is None:
        base = dig(data, "metrics")
    if base is None:
        base = {}

    def get(name, *aliases):
        for key in (name, *aliases):
            v = base.get(key)
            if v is not None:
                return round(float(v), 4)
        return None

    return {
        "MRR":     get("inverse_harmonic_mean_rank"),
        "Hits@1":  get("hits_at_1", "hits@1"),
        "Hits@3":  get("hits_at_3", "hits@3"),
        "Hits@10": get("hits_at_10", "hits@10"),
    }


# ── model loading ──────────────────────────────────────────────────────────────

def load_model_and_factory(run_dir: Path):
    model_path  = run_dir / "trained_model.pkl"
    factory_dir = run_dir / "training_triples"

    if not model_path.exists():
        raise FileNotFoundError(f"trained_model.pkl not found in {run_dir.name}")
    if not factory_dir.exists():
        raise FileNotFoundError(f"training_triples/ not found in {run_dir.name}")

    model = torch.load(str(model_path), map_location="cpu", weights_only=False)
    model.eval()
    tf = TriplesFactory.from_path_binary(str(factory_dir))
    return model, tf


def get_entity_embeddings(model) -> np.ndarray:
    with torch.no_grad():
        emb = model.entity_representations[0](indices=None)
    return emb.detach().cpu().numpy()


# ── nearest neighbours ─────────────────────────────────────────────────────────

def nearest_neighbours(query: str, entity_to_id: dict,
                       emb: np.ndarray, k: int = TOP_K):
    if query not in entity_to_id:
        return []
    id_to_entity = {v: e for e, v in entity_to_id.items()}
    qid    = entity_to_id[query]
    normed = normalize(emb)
    scores = normed @ normed[qid]
    scores[qid] = -1.0
    top_ids = np.argsort(scores)[::-1][:k]
    return [(id_to_entity[i], round(float(scores[i]), 4)) for i in top_ids]


# ── t-SNE ──────────────────────────────────────────────────────────────────────

def tsne_plot(emb: np.ndarray, entity_to_id: dict,
              entity_classes: dict, title: str, out_path: Path) -> None:
    n   = min(2000, len(emb))
    rng = np.random.default_rng(42)

    # Always include HIGHLIGHT entities so they appear in the plot
    pinned = [entity_to_id[name] for name in HIGHLIGHT if name in entity_to_id]
    remaining = [i for i in range(len(emb)) if i not in pinned]
    fill = rng.choice(remaining, size=max(0, n - len(pinned)), replace=False).tolist()
    idx = np.array(pinned + fill)
    sub_emb = emb[idx]

    tsne = TSNE(n_components=2, perplexity=30, random_state=42,
                max_iter=1000, learning_rate="auto", init="pca")
    xy = tsne.fit_transform(sub_emb)

    id_to_entity = {v: e for e, v in entity_to_id.items()}
    idx_list     = idx.tolist()

    # Assign colour per sampled entity
    colors = []
    for pos, global_id in enumerate(idx_list):
        name = id_to_entity.get(global_id, "")
        cls  = entity_classes.get(name, "Other")
        colors.append(CLASS_COLORS.get(cls, CLASS_COLORS["Other"]))

    fig, ax = plt.subplots(figsize=(11, 8))
    ax.scatter(xy[:, 0], xy[:, 1], c=colors, s=5, alpha=0.45, linewidths=0)

    # Highlight entities with labels
    texts = []
    for name in HIGHLIGHT:
        if name not in entity_to_id:
            continue
        eid = entity_to_id[name]
        if eid not in idx_list:
            continue
        pos = idx_list.index(eid)
        ax.scatter(xy[pos, 0], xy[pos, 1], s=80, zorder=6,
                   color="black", edgecolors="white", linewidths=0.8)
        texts.append(ax.text(xy[pos, 0], xy[pos, 1],
                              name.replace("_", " "),
                              fontsize=7.5, fontweight="bold", color="black",
                              clip_on=True))

    # Reposition labels to reduce overlap, but keep them inside the axes
    if texts:
        x_min, x_max = ax.get_xlim()
        y_min, y_max = ax.get_ylim()
        adjust_text(texts, ax=ax,
                    arrowprops=dict(arrowstyle="-", color="gray", lw=0.6),
                    expand=(1.2, 1.3),
                    force_text=(0.3, 0.3))
        # Clamp any labels that drifted outside the axes
        for t in texts:
            tx, ty = t.get_position()
            t.set_position((
                max(x_min, min(x_max, tx)),
                max(y_min, min(y_max, ty)),
            ))

    # Legend
    present_classes = {entity_classes.get(id_to_entity.get(i, ""), "Other")
                       for i in idx_list}
    legend_handles = [
        Line2D([0], [0], marker="o", color="w",
               markerfacecolor=CLASS_COLORS[c], markersize=7, label=c)
        for c in CLASS_COLORS if c in present_classes
    ]
    ax.legend(handles=legend_handles, loc="lower right",
              fontsize=8, framealpha=0.8)

    ax.set_title(title, fontsize=13)
    ax.axis("off")
    plt.tight_layout()
    plt.savefig(str(out_path), dpi=150)
    plt.close()
    print(f"  Saved: {out_path.name}")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    experiments = [
        ("TransE",   "small"),
        ("TransE",   "full"),
        ("DistMult", "small"),
        ("DistMult", "full"),
    ]

    TRIPLES_COUNT = {"small": 4323, "full": 21617}

    # Load ontology class mapping from the local KB
    kg_path = ROOT / "kg_artifacts" / "initial_kg.ttl"
    entity_classes = load_entity_classes(kg_path)
    print(f"Loaded {len(entity_classes)} entity-class mappings from KB.")

    # 1. Metrics table
    rows = []
    for model_name, size_label in experiments:
        run_dir = RES_DIR / f"{model_name}_{size_label}"
        if not run_dir.exists():
            continue
        m = read_metrics_from_dir(run_dir)
        rows.append({
            "model":   model_name,
            "size":    size_label,
            "triples": TRIPLES_COUNT.get(size_label, "-"),
            **m,
        })

    if not rows:
        print("[ERROR] No result directories found. Run train.py first.")
        return

    print(f"\n{'='*80}")
    print("  KGE EVALUATION — METRICS TABLE")
    print(f"{'='*80}")
    print(f"{'Model':<12}{'Size':<8}{'Triples':<10}{'MRR':<10}"
          f"{'Hits@1':<10}{'Hits@3':<10}{'Hits@10'}")
    print("-" * 80)
    for r in rows:
        print(f"{r['model']:<12}{r['size']:<8}{str(r['triples']):<10}"
              f"{str(r['MRR']):<10}{str(r['Hits@1']):<10}"
              f"{str(r['Hits@3']):<10}{str(r['Hits@10'])}")
    print(f"{'='*80}\n")

    # 2. Per-model: t-SNE + nearest neighbours
    for model_name, size_label in experiments:
        run_dir = RES_DIR / f"{model_name}_{size_label}"
        if not run_dir.exists():
            print(f"[skip] {model_name}_{size_label} — directory not found")
            continue

        print(f"=== {model_name} ({size_label}) ===")
        try:
            model, tf = load_model_and_factory(run_dir)
        except FileNotFoundError as exc:
            print(f"  [skip] {exc}")
            continue

        emb          = get_entity_embeddings(model)
        entity_to_id = tf.entity_to_id
        print(f"  Entities: {len(entity_to_id)}   Dim: {emb.shape[1]}")

        # t-SNE with class colouring
        out_png = FIG_DIR / f"tsne_{model_name}_{size_label}.png"
        title   = f"t-SNE  {model_name} ({size_label})  dim={emb.shape[1]}"
        print("  Running t-SNE ...")
        tsne_plot(emb, entity_to_id, entity_classes, title, out_png)

        # Nearest neighbours
        print(f"  Nearest neighbours (cosine, top {TOP_K}):")
        any_found = False
        for entity in HIGHLIGHT:
            nbrs = nearest_neighbours(entity, entity_to_id, emb)
            if nbrs:
                any_found = True
                print(f"    [{entity.replace('_', ' ')}]")
                for nbr, score in nbrs:
                    print(f"      {nbr.replace('_', ' '):<38} {score:.4f}")
        if not any_found:
            print("    (highlight entities not in vocabulary)")
        print()

    print(f"Figures saved to: {FIG_DIR}")
    print("Done.")


if __name__ == "__main__":
    main()
