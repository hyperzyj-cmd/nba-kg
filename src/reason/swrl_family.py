"""
SWRL-style reasoning on family.owl.
Rules are defined as SWRL patterns and applied via Python inference
(owlready2 SWRL execution requires Java; rules are documented below).
"""
import os
from pathlib import Path
from rdflib import Graph, Namespace, RDF, RDFS, OWL, Literal
from rdflib.namespace import XSD

FAMILY_OWL = Path(os.path.dirname(os.path.abspath(__file__))) / "family.owl"

BASE = Namespace("http://www.owl-ontologies.com/unnamed.owl#")

# ── Load ontology ─────────────────────────────────────────────────
g = Graph()
g.parse(str(FAMILY_OWL), format="xml")

print("=" * 58)
print("  SWRL Reasoning -- family.owl")
print("=" * 58)
print(f"  Triples loaded: {len(g)}")

# ── Helper: get individuals of a class ───────────────────────────
def instances_of(cls_uri):
    return set(s for s, _, _ in g.triples((None, RDF.type, cls_uri)))

def name_of(uri):
    label = g.value(uri, BASE.name)
    if label:
        return str(label)
    return str(uri).split("#")[-1]

def age_of(uri):
    a = g.value(uri, BASE.age)
    return int(a) if a else None

# ── SWRL Rules ────────────────────────────────────────────────────
print("""
SWRL Rules:
  [1] Male(?x) ^ isFatherOf(?x,?y) ^ isParentOf(?y,?z)
      -> Grandfather(?x)

  [2] Male(?x) ^ isBrotherOf(?x,?p) ^ isParentOf(?p,?c)
      -> Uncle(?x)

  [3] Person(?x) ^ age(?x,?a) ^ swrlb:greaterThan(?a,60)
      -> OldPerson(?x)
""")

print("Applying rules ...")

# Collect existing facts
males        = instances_of(BASE.Male)
females      = instances_of(BASE.Female)
persons      = instances_of(BASE.Person) | males | females
father_of    = {(s, o) for s, _, o in g.triples((None, BASE.isFatherOf, None))}
mother_of    = {(s, o) for s, _, o in g.triples((None, BASE.isMotherOf, None))}
son_of       = {(s, o) for s, _, o in g.triples((None, BASE.isSonOf, None))}
daughter_of  = {(s, o) for s, _, o in g.triples((None, BASE.isDaughterOf, None))}
parent_of    = {(s, o) for s, _, o in g.triples((None, BASE.isParentOf, None))}

# isParentOf = isFatherOf ∪ isMotherOf (subPropertyOf)
parent_of = parent_of | father_of | mother_of

# ── Rule 1: Grandfather ───────────────────────────────────────────
grandfathers = set()
for (x, y) in father_of:
    if x not in males:
        continue
    for (y2, z) in parent_of:
        if y2 == y:
            grandfathers.add(x)
            g.add((x, RDF.type, BASE.Grandfather))

# ── Rule 2: Uncle ─────────────────────────────────────────────────
# infer isBrotherOf from shared parents
brother_of = {(s, o) for s, _, o in g.triples((None, BASE.isBrotherOf, None))}
all_parents = son_of | daughter_of
for x in males:
    x_parents = {p for (child, p) in all_parents if child == x}
    for (p, child) in {(p, c) for (c, p) in all_parents}:
        # 'p' is a parent, 'child' is their child
        pass

# Simpler: find siblings by shared parent
child_to_parents = {}
for (child, parent) in all_parents:
    child_to_parents.setdefault(child, set()).add(parent)

uncles = set()
for x in males:
    x_parents = child_to_parents.get(x, set())
    if not x_parents:
        continue
    for other, other_parents in child_to_parents.items():
        if other == x:
            continue
        if x_parents & other_parents:
            # x and other share a parent → siblings
            # check if other has children
            other_children = [o for (s, o) in parent_of if s == other]
            if other_children:
                uncles.add(x)
                g.add((x, RDF.type, BASE.Uncle))
                break

# ── Rule 3: OldPerson ─────────────────────────────────────────────
old_persons = set()
for person in persons:
    age = age_of(person)
    if age is not None and age > 60:
        old_persons.add(person)
        g.add((person, RDF.type, BASE.OldPerson))

# ── Print results ─────────────────────────────────────────────────
sep = "-" * 58
print(f"\n{sep}")
print("INFERRED RESULTS")
print(sep)

print(f"\n[Rule 1] Grandfathers ({len(grandfathers)}):")
for p in sorted(grandfathers, key=name_of):
    print(f"  -> {name_of(p):<12}  age={age_of(p)}")

print(f"\n[Rule 2] Uncles ({len(uncles)}):")
for p in sorted(uncles, key=name_of):
    print(f"  -> {name_of(p)}")

print(f"\n[Rule 3] OldPersons - age > 60 ({len(old_persons)}):")
for p in sorted(old_persons, key=name_of):
    print(f"  -> {name_of(p):<12}  age={age_of(p)}")

print(f"\n{sep}")
print("ALL INDIVIDUALS WITH KEY TYPES")
print(sep)

all_types = {
    BASE.Male: "Male", BASE.Female: "Female",
    BASE.Grandfather: "Grandfather", BASE.Grandmother: "Grandmother",
    BASE.Uncle: "Uncle", BASE.OldPerson: "OldPerson",
    BASE.Father: "Father", BASE.Mother: "Mother",
    BASE.Parent: "Parent",
}

for ind in sorted(persons, key=name_of):
    ind_types = [label for uri, label in all_types.items()
                 if (ind, RDF.type, uri) in g]
    age = age_of(ind)
    age_str = f"age={age}" if age else ""
    print(f"  {name_of(ind):<12}  {age_str:<8}  {ind_types}")

print("\nDone.")
