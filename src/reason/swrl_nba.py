"""
SWRL-style reasoning on NBA knowledge graph.
Rules are defined as SWRL patterns and applied via Python inference.
"""
from collections import defaultdict

NBA_NS = "http://nba-kg.org/ontology#"

# ── Roster facts (player -> team) ─────────────────────────────────
ROSTER = {
    "LeBron James":           "Los Angeles Lakers",
    "Anthony Davis":          "Los Angeles Lakers",
    "Stephen Curry":          "Golden State Warriors",
    "Kevin Durant":           "Golden State Warriors",
    "Klay Thompson":          "Golden State Warriors",
    "Jayson Tatum":           "Boston Celtics",
    "Jaylen Brown":           "Boston Celtics",
    "Giannis Antetokounmpo":  "Milwaukee Bucks",
    "Damian Lillard":         "Milwaukee Bucks",
    "Nikola Jokic":           "Denver Nuggets",
    "Jamal Murray":           "Denver Nuggets",
    "Luka Doncic":            "Dallas Mavericks",
    "Kyrie Irving":           "Dallas Mavericks",
    "Joel Embiid":            "Philadelphia 76ers",
    "Victor Wembanyama":      "San Antonio Spurs",
}

MVP_PLAYERS = {
    "LeBron James", "Stephen Curry", "Kevin Durant",
    "Giannis Antetokounmpo", "Nikola Jokic", "Joel Embiid",
    "Michael Jordan", "Kobe Bryant", "Dirk Nowitzki",
    "Tim Duncan", "Shaquille O'Neal",
}

# ── Build facts ───────────────────────────────────────────────────
plays_for   = {}          # player -> team
team_roster = defaultdict(set)  # team -> {players}
mvp_flag    = {}          # player -> bool

for player, team in ROSTER.items():
    plays_for[player]  = team
    team_roster[team].add(player)

for p in plays_for:
    mvp_flag[p] = (p in MVP_PLAYERS)

# ── Print header ──────────────────────────────────────────────────
print("=" * 58)
print("  SWRL Reasoning -- NBA Knowledge Graph")
print("=" * 58)
print(f"  Players: {len(plays_for)}   Teams: {len(team_roster)}")
print("""
SWRL Rules:
  [1] Player(?x) ^ Player(?y) ^ Team(?t)
      ^ playsFor(?x,?t) ^ playsFor(?y,?t) ^ differentFrom(?x,?y)
      -> teammateOf(?x,?y)

  [2] Player(?x) ^ wonMVP(?x, true)
      -> MVPWinner(?x)
""")

print("Applying rules ...")

# ── Rule 1: Teammate ──────────────────────────────────────────────
teammate_pairs = set()
for team, members in team_roster.items():
    members_list = sorted(members)
    for i, p1 in enumerate(members_list):
        for p2 in members_list[i + 1:]:
            teammate_pairs.add((p1, p2))

# ── Rule 2: MVPWinner ─────────────────────────────────────────────
mvp_winners = {p for p in plays_for if mvp_flag.get(p)}

# ── Results ───────────────────────────────────────────────────────
sep = "-" * 58
print(f"\n{sep}")
print("INFERRED RESULTS")
print(sep)

print(f"\n[Rule 1] Teammate pairs inferred ({len(teammate_pairs)}):")
for p1, p2 in sorted(teammate_pairs):
    print(f"  {p1:<28} <-> {p2}")

print(f"\n[Rule 2] MVPWinner ({len(mvp_winners)}):")
for p in sorted(mvp_winners):
    team = plays_for.get(p, "?")
    print(f"  {p:<28} (team: {team})")

print(f"\n{sep}")
print("ALL PLAYERS WITH INFERRED TYPES")
print(sep)
for player in sorted(plays_for):
    team   = plays_for[player]
    types  = ["Player"]
    mates  = [p2 for (p1, p2) in teammate_pairs if p1 == player] + \
             [p1 for (p1, p2) in teammate_pairs if p2 == player]
    if player in mvp_winners:
        types.append("MVPWinner")
    if mates:
        types.append(f"Teammate({len(mates)})")
    print(f"  {player:<28} team={team:<25} {types}")

print("\nDone.")
