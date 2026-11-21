"""
Generate dbt/seeds/metro_aliases.csv: principal-city aliases per metro (CBSA),
used by silver_news_items to tag news with a metro (design doc §9.3).

Built from the metro names on scored zips (HUD/OMB names such as
"Dallas-Fort Worth-Arlington, TX"): each principal city x each state of the
metro. Re-run after a scoring refresh adds metros; the CSV is committed and
can be edited by hand.

    python scripts/make_metro_aliases.py [--warehouse data/rpof.duckdb]

Columns:
  cbsa_code, metro_name, city, state_abbr, state_name
  bare_ok   the city name alone (no state) is enough to tag. Only for big
            metros (top 50 by scored zips) whose city name is unique among all
            aliases and not a common word or person's name.
"""

import argparse
import csv
import re
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "dbt" / "seeds" / "metro_aliases.csv"
BARE_TOP_N = 50

STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "DC": "District of Columbia", "FL": "Florida", "GA": "Georgia", "HI": "Hawaii",
    "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas",
    "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming", "PR": "Puerto Rico",
}  # fmt: skip

# City names that contain a hyphen (OMB joins principal cities with "-").
HYPHENATED = {"Winston-Salem", "Wilkes-Barre", "Sedro-Woolley", "Fuquay-Varina"}
# Parts of OMB names that are not cities people write in news.
DROP = {"Davidson", "Jefferson County"}
RENAME = {"Urban Honolulu": "Honolulu", "Ca±on City": "Cañon City"}
# Unique city names that are still too ambiguous alone (people, words, brands).
NOT_BARE = {
    "Austin", "Phoenix", "Orlando", "Charlotte", "Elgin", "Newton", "Warren",
    "Franklin", "Ontario", "Riverside", "Anaheim", "Irvine", "Bellevue",
    "Cambridge", "Columbia", "Concord", "Paradise", "Enterprise", "Sugar Land",
    "Round Rock", "The Woodlands", "Chandler", "Mesa", "Plano", "Hillsboro",
    "Vancouver", "Kingston", "Bloomington", "Alexandria", "Arlington",
    # Federal government / the state, or the name of several US places.
    "Washington", "Providence", "Newark", "Aurora", "Lakewood", "Georgetown",
    "Camden", "Carmel", "Warwick", "Long Beach", "Roseville", "Folsom",
    "Santa Clara",
    # Also the state; "New York City" and the boroughs below tag NYC instead.
    "New York",
}  # fmt: skip
# Extra unambiguous aliases (cbsa_code -> city, state), all bare_ok.
EXTRA = {
    "35620": [("New York City", "NY"), ("Manhattan", "NY"), ("Brooklyn", "NY"),
              ("Queens", "NY"), ("Bronx", "NY"), ("Staten Island", "NY")],
}  # fmt: skip


def split_cities(metro: str) -> tuple[list[str], list[str]]:
    """'Dallas-Fort Worth-Arlington, TX' -> (['Dallas', 'Fort Worth', ...], ['TX'])."""
    for bad, good in RENAME.items():
        metro = metro.replace(bad, good)
    names, _, states = metro.rpartition(", ")
    cities = []
    for major in names.split("--"):
        for part in major.split("/"):
            pieces = part.split("-")
            i = 0
            while i < len(pieces):
                pair = "-".join(pieces[i : i + 2])
                if pair in HYPHENATED:
                    cities.append(pair)
                    i += 2
                else:
                    cities.append(pieces[i])
                    i += 1
    cities = [RENAME.get(c.strip(), c.strip()) for c in cities]
    out = []
    for city in cities:
        if city and city not in DROP and city not in out:
            out.append(city)
    if "Boise City" in out:  # OMB name; everyone writes "Boise"
        out[out.index("Boise City")] = "Boise"
    return out, [s for s in states.split("-") if s in STATES]


def build_rows(metros: list[tuple[str, str, int]]) -> list[dict]:
    """metros: (cbsa_code, metro_name, scored_zips) -> alias rows."""
    parsed = [(code, name, n, *split_cities(name)) for code, name, n in metros]
    counts: dict[str, int] = {}
    for _, _, _, cities, _ in parsed:
        for c in cities:
            counts[c] = counts.get(c, 0) + 1
    top = {code for code, _, _ in sorted(metros, key=lambda m: -m[2])[:BARE_TOP_N]}
    rows = []
    for code, name, _, cities, states in parsed:
        for city in cities:
            bare = (
                code in top
                and counts[city] == 1
                and city not in NOT_BARE
                and len(city) >= 5
                and re.fullmatch(r"[A-Za-z .'ñ-]+", city) is not None
            )
            for st in states:
                rows.append(
                    {
                        "cbsa_code": code,
                        "metro_name": name,
                        "city": city,
                        "state_abbr": st,
                        "state_name": STATES[st],
                        "bare_ok": str(bare).lower(),
                    }
                )
    names = {code: name for code, name, _ in metros}
    for code, extras in EXTRA.items():
        if code not in names:
            continue
        for city, st in extras:
            rows.append(
                {
                    "cbsa_code": code,
                    "metro_name": names[code],
                    "city": city,
                    "state_abbr": st,
                    "state_name": STATES[st],
                    "bare_ok": "true",
                }
            )
    return rows


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Generate dbt/seeds/metro_aliases.csv")
    p.add_argument("--warehouse", default=str(ROOT / "data" / "rpof.duckdb"))
    args = p.parse_args(argv)
    con = duckdb.connect(args.warehouse, read_only=True)
    metros = con.execute("""
        select cbsa_code, mode(metro) as metro, count(*) as scored_zips
        from gold.zip_opportunity_score
        where cbsa_code is not null and metro is not null
        group by cbsa_code order by cbsa_code
        """).fetchall()
    rows = build_rows(metros)
    with OUT.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} aliases for {len(metros)} metros -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
