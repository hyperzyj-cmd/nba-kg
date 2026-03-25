"""
Train TransE and DistMult on the NBA knowledge graph.
Runs 4 experiments: 2 models x 2 data sizes.
"""
import json
import random
import time
from pathlib import Path

from pykeen.pipeline import pipeline
from pykeen.triples import TriplesFactory

ROOT     = Path(__file__).parent.parent.parent
DATA_DIR = ROOT / "data" / "kge"
RES_DIR  = ROOT / "src" / "kge" / "results"
RES_DIR.mkdir(parents=True, exist_ok=True)

SEED           = 42
EMBEDDING_DIM  = 64
NUM_EPOCHS     = 100
BATCH_SIZE     = 256


def subsample_train(train_path: Path, fraction: float, seed: int) -> list[list[str]]:
    """Return a random fraction of training triples."""
    rows = [line.strip().split("\t")
            for line in train_path.read_text(encoding="utf-8").splitlines()
            if line.strip()]
    random.seed(seed)
    random.shuffle(rows)
    return rows[:max(1, int(len(rows) * fraction))]


def write_tsv(path: Path, rows: list[list[str]]) -> None:
    path.write_text(
        "\n".join("\t".join(r) for r in rows),
        encoding="utf-8"
    )


def read_metrics_from_saved(save_dir: Path) -> dict:
    """Read evaluation metrics from the results.json written by pykeen."""
    rjson = save_dir / "results.json"
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


def run_experiment(model_name: str, size_label: str,
                   train_path: Path, valid_path: str, test_path: str,
                   fraction: float) -> dict:

    print(f"\n{'='*55}")
    print(f"  {model_name}  |  size={size_label}  |  fraction={fraction:.0%}")
    print(f"{'='*55}")

    # Build training triples
    if fraction < 1.0:
        rows = subsample_train(train_path, fraction, SEED)
        tmp_train = DATA_DIR / f"_tmp_train_{size_label}.tsv"
        write_tsv(tmp_train, rows)
        train_src = str(tmp_train)
    else:
        train_src = str(train_path)
        rows = [l.split("\t") for l in train_path.read_text().splitlines() if l.strip()]

    print(f"  Training triples : {len(rows)}")

    tf_train = TriplesFactory.from_path(train_src)
    tf_valid = TriplesFactory.from_path(str(valid_path),
                   entity_to_id=tf_train.entity_to_id,
                   relation_to_id=tf_train.relation_to_id)
    tf_test  = TriplesFactory.from_path(str(test_path),
                   entity_to_id=tf_train.entity_to_id,
                   relation_to_id=tf_train.relation_to_id)

    model_kwargs = {"embedding_dim": EMBEDDING_DIM}
    optimizer_kwargs = {"lr": 0.01 if model_name == "TransE" else 0.001}

    t0 = time.time()
    result = pipeline(
        training=tf_train,
        validation=tf_valid,
        testing=tf_test,
        model=model_name,
        model_kwargs=model_kwargs,
        training_kwargs={"num_epochs": NUM_EPOCHS, "batch_size": BATCH_SIZE},
        optimizer="Adam",
        optimizer_kwargs=optimizer_kwargs,
        device="cpu",
        random_seed=SEED,
        use_testing_data=True,
    )
    elapsed = time.time() - t0
    print(f"  Training time    : {elapsed:.1f}s")

    # Save model first, then read metrics from the written results.json
    save_dir = RES_DIR / f"{model_name}_{size_label}"
    result.save_to_directory(str(save_dir))
    print(f"  Saved to         : {save_dir.name}/")

    metrics = read_metrics_from_saved(save_dir)
    for k, v in metrics.items():
        print(f"  {k:<10}: {v}")

    # Cleanup tmp file
    if fraction < 1.0 and tmp_train.exists():
        tmp_train.unlink()

    return {
        "model":   model_name,
        "size":    size_label,
        "triples": len(rows),
        **metrics,
        "time_s":  round(elapsed, 1),
    }


def main():
    train_path = DATA_DIR / "train.tsv"
    valid_path = DATA_DIR / "valid.tsv"
    test_path  = DATA_DIR / "test.tsv"

    experiments = [
        ("TransE",   "small", 0.20),
        ("TransE",   "full",  1.00),
        ("DistMult", "small", 0.20),
        ("DistMult", "full",  1.00),
    ]

    results = []
    for model_name, size_label, fraction in experiments:
        row = run_experiment(
            model_name, size_label,
            train_path, valid_path, test_path,
            fraction
        )
        results.append(row)

    # Print comparison table
    print(f"\n{'='*75}")
    print("  COMPARISON TABLE")
    print(f"{'='*75}")
    header = f"{'Model':<12}{'Size':<8}{'Triples':<10}{'MRR':<8}{'Hits@1':<8}{'Hits@3':<8}{'Hits@10':<9}{'Time(s)'}"
    print(header)
    print("-" * 75)
    for r in results:
        print(
            f"{r['model']:<12}{r['size']:<8}{r['triples']:<10}"
            f"{str(r['MRR']):<8}{str(r['Hits@1']):<8}"
            f"{str(r['Hits@3']):<8}{str(r['Hits@10']):<9}{r['time_s']}"
        )

    # Save summary
    (RES_DIR / "summary.json").write_text(
        json.dumps(results, indent=2), encoding="utf-8"
    )
    print(f"\nSummary saved to {RES_DIR / 'summary.json'}")


if __name__ == "__main__":
    main()
