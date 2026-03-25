"""
NBA Knowledge Graph RAG Pipeline (TD6)
Natural Language -> SPARQL -> Execute -> Answer
Includes self-repair and baseline vs RAG comparison.
"""

import io
import json
import re
import sys
import requests
from pathlib import Path
from rdflib import Graph, Namespace, RDF, RDFS

# Force UTF-8 output on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

ROOT        = Path(__file__).parent.parent.parent
INITIAL_KG  = ROOT / "kg_artifacts" / "initial_kg.ttl"
OLLAMA_URL  = "http://localhost:11434/api/generate"
MODEL       = "qwen2.5:3b"
MAX_REPAIR  = 3

NBA = Namespace("http://nba-kg.org/ontology#")
RES = Namespace("http://nba-kg.org/resource/")

_STANDARD_PREFIXES = {
    "rdf":  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
    "owl":  "http://www.w3.org/2002/07/owl#",
    "nba":  "http://nba-kg.org/ontology#",
    "res":  "http://nba-kg.org/resource/",
    "xsd":  "http://www.w3.org/2001/XMLSchema#",
}

MAX_PREDICATES = 40
MAX_CLASSES    = 20
SAMPLE_TRIPLES = 10

# ── Dynamic schema summary (built from graph at runtime) ──────────────────────

def _abbrev(uri: str, ns_map: dict) -> str:
    """Abbreviate a full URI to prefix:local using the namespace map."""
    for prefix, ns in sorted(ns_map.items(), key=lambda x: -len(x[1])):
        if uri.startswith(ns):
            return f"{prefix}:{uri[len(ns):]}"
    return f"<{uri}>"


def build_schema_summary(g: Graph) -> str:
    """Dynamically build schema from the loaded graph.
    Extracts prefixes, distinct predicates, distinct classes, and sample
    triples — all abbreviated with prefix notation for LLM readability."""
    defaults = {
        "rdf":  "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        "owl":  "http://www.w3.org/2002/07/owl#",
    }
    ns_map = {p: str(ns) for p, ns in g.namespace_manager.namespaces()}
    for k, v in defaults.items():
        ns_map.setdefault(k, v)

    prefix_block = "\n".join(
        f"PREFIX {p}: <{ns}>" for p, ns in sorted(ns_map.items())
    )

    preds = [
        _abbrev(str(row[0]), ns_map)
        for row in g.query(
            f"SELECT DISTINCT ?p WHERE {{ ?s ?p ?o . }} LIMIT {MAX_PREDICATES}"
        )
    ]
    classes = [
        _abbrev(str(row[0]), ns_map)
        for row in g.query(
            f"SELECT DISTINCT ?c WHERE {{ ?s a ?c . }} LIMIT {MAX_CLASSES}"
        )
    ]
    samples = [
        (
            _abbrev(str(r[0]), ns_map),
            _abbrev(str(r[1]), ns_map),
            _abbrev(str(r[2]), ns_map),
        )
        for r in g.query(
            f"SELECT ?s ?p ?o WHERE {{ ?s ?p ?o . }} LIMIT {SAMPLE_TRIPLES}"
        )
    ]

    pred_lines   = "\n".join(f"  - {p}" for p in preds)
    class_lines  = "\n".join(f"  - {c}" for c in classes)
    sample_lines = "\n".join(f"  - {s}  {p}  {o}" for s, p, o in samples)

    rules = """\
Rules:
  - Use ONLY prefixes/IRIs shown above.
  - Entity names use underscores: res:LeBron_James  (not "LeBron James")
  - Always declare PREFIX lines at the top.
  - Use LIMIT to keep results manageable.

Example 1 - count players:
  PREFIX nba: <http://nba-kg.org/ontology#>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  SELECT (COUNT(?p) AS ?count) WHERE { ?p rdf:type nba:Player . }

Example 2 - list teams:
  PREFIX nba:  <http://nba-kg.org/ontology#>
  PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  SELECT ?label WHERE { ?t rdf:type nba:Team . ?t rdfs:label ?label . } LIMIT 10

Example 3 - check entity type:
  PREFIX nba: <http://nba-kg.org/ontology#>
  PREFIX res: <http://nba-kg.org/resource/>
  PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  SELECT ?type WHERE { res:LeBron_James rdf:type ?type . }"""

    return f"""{prefix_block}

# Predicates (up to {MAX_PREDICATES} distinct, abbreviated)
{pred_lines}

# Classes / rdf:type values (up to {MAX_CLASSES} distinct)
{class_lines}

# Sample triples (up to {SAMPLE_TRIPLES})
{sample_lines}

{rules}"""

# ── Demo questions ─────────────────────────────────────────────────────────────

DEMO_QUESTIONS = [
    "How many NBA players are recorded in this knowledge graph?",
    "How many NBA teams are recorded in this knowledge graph?",
    "How many cities are in the knowledge graph?",
    "What awards are tracked in the knowledge base?",
    "What type of entity is LeBron James in the knowledge graph?",
    "How many NBA seasons are recorded in this knowledge graph?",
]


# ── Helpers ────────────────────────────────────────────────────────────────────

def load_graph() -> Graph:
    g = Graph()
    g.parse(str(INITIAL_KG), format="turtle")
    print(f"[KB] Loaded {len(g)} triples from {INITIAL_KG.name}")
    return g


def ollama_ask(prompt: str) -> str:
    try:
        resp = requests.post(
            OLLAMA_URL,
            json={"model": MODEL, "prompt": prompt, "stream": False},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("response", "").strip().encode("ascii", "ignore").decode("ascii")
    except Exception as exc:
        return f"[OLLAMA ERROR] {exc}"


def inject_missing_prefixes(sparql: str) -> str:
    """Inject any standard PREFIX declarations missing from the query."""
    declared = set(re.findall(r'PREFIX\s+(\w+)\s*:', sparql, re.IGNORECASE))
    used = set(re.findall(r'\b(\w+):', sparql))
    missing = [
        f"PREFIX {p}: <{uri}>"
        for p, uri in _STANDARD_PREFIXES.items()
        if p in used and p not in declared
    ]
    return ("\n".join(missing) + "\n" + sparql) if missing else sparql


def extract_sparql(text: str) -> str:
    """Extract SPARQL query from LLM response (handles code blocks)."""
    # Try fenced code blocks first
    for pattern in [r"```sparql\s*(.*?)```", r"```\s*(SELECT.*?)```"]:
        m = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if m:
            return m.group(1).strip()
    # Fall back: look for SELECT ... WHERE block
    m = re.search(r"(PREFIX.*?SELECT.*?WHERE\s*\{.*?\}(?:\s*LIMIT\s*\d+)?)", text, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def execute_sparql(g: Graph, query: str):
    """Execute SPARQL and return (rows, error)."""
    try:
        results = g.query(query)
        rows = [list(row) for row in results]
        return rows, None
    except Exception as exc:
        return [], str(exc)


def format_rows(rows: list) -> str:
    if not rows:
        return "(no results)"
    lines = []
    for row in rows[:15]:
        cells = []
        for cell in row:
            val = str(cell)
            val = val.replace("http://nba-kg.org/resource/", "").replace("http://nba-kg.org/ontology#", "")
            val = val.replace("_", " ")
            cells.append(val)
        lines.append("  " + " | ".join(cells))
    if len(rows) > 15:
        lines.append(f"  ... ({len(rows)} total)")
    return "\n".join(lines)


# ── Core RAG functions ─────────────────────────────────────────────────────────

def baseline_answer(question: str) -> str:
    """Ask LLM directly without any KB context."""
    prompt = f"Answer this question about NBA basketball briefly:\n{question}"
    return ollama_ask(prompt)


def rag_answer(g: Graph, question: str, schema: str) -> tuple[str, str, int]:
    """
    RAG pipeline: schema + NL -> SPARQL -> execute -> summarize.
    Returns (final_answer, sparql_used, repair_count).
    """
    # Step 1: generate SPARQL
    gen_prompt = (
        f"You are a SPARQL generator. Use the schema below to write a SPARQL query.\n\n"
        f"SCHEMA SUMMARY:\n{schema}\n\n"
        f"QUESTION: {question}\n\n"
        f"Return ONLY the SPARQL query inside a ```sparql code block. No explanation."
    )
    sparql_text = ollama_ask(gen_prompt)
    sparql = extract_sparql(sparql_text)

    repair_count = 0
    rows, error = [], "No query extracted"

    if sparql:
        sparql = inject_missing_prefixes(sparql)
        rows, error = execute_sparql(g, sparql)

    # Step 2: self-repair loop
    while error and repair_count < MAX_REPAIR:
        repair_count += 1
        repair_prompt = (
            f"The previous SPARQL failed. Fix it using the schema and error below.\n\n"
            f"SCHEMA SUMMARY:\n{schema}\n\n"
            f"ORIGINAL QUESTION: {question}\n\n"
            f"BAD SPARQL:\n{sparql}\n\n"
            f"ERROR: {error}\n\n"
            f"Return ONLY the corrected SPARQL query inside a ```sparql code block."
        )
        sparql_text = ollama_ask(repair_prompt)
        sparql = extract_sparql(sparql_text)
        if sparql:
            sparql = inject_missing_prefixes(sparql)
            rows, error = execute_sparql(g, sparql)
        else:
            error = "Could not extract SPARQL from repair response"

    if error:
        return f"[Query failed after {repair_count} repair attempts: {error}]", sparql or "", repair_count

    # Step 3: format results and summarize
    results_text = format_rows(rows)
    summary_prompt = (
        f"Answer the question using ONLY the data shown in Results. "
        f"Do not add any information not present in Results.\n\n"
        f"Question: {question}\n\n"
        f"Results:\n{results_text}\n\n"
        f"Give a short, direct answer based strictly on the Results above."
    )
    summary = ollama_ask(summary_prompt)
    answer = f"{summary}\n    [Raw results]\n{results_text}"
    return answer, sparql, repair_count


# ── Demo and interactive modes ─────────────────────────────────────────────────

def run_demo(g: Graph, schema: str) -> None:
    print("\n" + "=" * 70)
    print("  NBA-KG RAG DEMO  —  Baseline vs RAG")
    print("=" * 70)

    for i, question in enumerate(DEMO_QUESTIONS, 1):
        print(f"\n[Q{i}] {question}")
        print("-" * 70)

        print("  BASELINE (LLM only):")
        b_ans = baseline_answer(question)
        for line in b_ans.splitlines():
            print(f"    {line}")

        print("\n  RAG (LLM + Knowledge Graph):")
        r_ans, sparql, repairs = rag_answer(g, question, schema)
        if sparql:
            print(f"    SPARQL generated{f' (repaired {repairs}x)' if repairs else ''}:")
            for line in sparql.splitlines():
                print(f"      {line}")
        print(f"    Answer:")
        for line in r_ans.splitlines():
            print(f"    {line}")

    print("\n" + "=" * 70)
    print("  Demo complete.")
    print("=" * 70)


def run_interactive(g: Graph, schema: str) -> None:
    print("\n" + "=" * 70)
    print("  NBA-KG Interactive RAG  (type 'quit' to exit)")
    print("=" * 70)

    while True:
        try:
            question = input("\nYour question: ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if question.lower() in ("quit", "exit", "q"):
            break
        if not question:
            continue

        print("\n  [Baseline]")
        b = baseline_answer(question)
        print(f"  {b}\n")

        print("  [RAG]")
        r, sparql, repairs = rag_answer(g, question, schema)
        if sparql:
            print(f"  SPARQL{f' (repaired {repairs}x)' if repairs else ''}:")
            for line in sparql.splitlines():
                print(f"    {line}")
        print(f"  Answer: {r}")

    print("\nGoodbye.")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    g = load_graph()
    schema = build_schema_summary(g)
    print(f"[Schema] Built from graph: {len(schema.splitlines())} lines")

    mode = sys.argv[1] if len(sys.argv) > 1 else "demo"

    if mode == "interactive":
        run_interactive(g, schema)
    else:
        run_demo(g, schema)
