#!/usr/bin/env python3
"""
DEPRECATED: Superseded by build_from_pfr.py which uses nflverse draft_picks
(PFR-sourced career totals) to produce correct stats for all players,
including those whose careers span the 1999 boundary.

Backfill pre-1999 career stats for players whose careers started before nflverse coverage.

Downloads yearly CSVs (1970-1998) from fantasydatapros GitHub, matches players
by name+position to People.csv, and merges pre-1999 stats into Stats.csv/People.csv.

Usage:
    python3 backfill_pre1999.py
"""

import csv
import io
import os
import re
import sys
import time
import requests

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(OUT_DIR, "historical")
FDP_URL = "https://raw.githubusercontent.com/fantasydatapros/data/master/yearly/{year}.csv"
BACKFILL_START = 1970
BACKFILL_END = 1998


def download_csv(url, cache_path=None):
    """Download a CSV, optionally caching to disk."""
    if cache_path and os.path.exists(cache_path):
        with open(cache_path, "r") as f:
            reader = csv.DictReader(f)
            return list(reader)

    resp = requests.get(url, timeout=60)
    if resp.status_code != 200:
        return []

    if cache_path:
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        with open(cache_path, "w") as f:
            f.write(resp.text)

    reader = csv.DictReader(io.StringIO(resp.text))
    return list(reader)


def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def safe_float(val, default=0.0):
    try:
        v = float(val)
        return v if v == v else default  # NaN check
    except (ValueError, TypeError):
        return default


def normalize_name(name):
    """Normalize a player name for matching."""
    name = name.strip()
    # Remove common suffixes
    name = re.sub(r'\s+(Jr\.?|Sr\.?|III|II|IV|V)$', '', name, flags=re.IGNORECASE)
    # Remove periods and apostrophes
    name = name.replace(".", "").replace("'", "").replace("'", "")
    # Lowercase
    name = name.lower()
    # Collapse whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def normalize_pos(pos):
    """Normalize position to our categories."""
    pos = pos.strip().upper()
    if pos == "FB":
        return "RB"
    if pos in ("QB", "RB", "WR", "TE"):
        return pos
    return None


def main():
    # ─── Load current People.csv and Stats.csv ───
    people_path = os.path.join(OUT_DIR, "People.csv")
    stats_path = os.path.join(OUT_DIR, "Stats.csv")
    legends_path = os.path.join(OUT_DIR, "Legends.csv")

    with open(people_path) as f:
        people = list(csv.DictReader(f))
    with open(stats_path) as f:
        stats = list(csv.DictReader(f))
    with open(legends_path) as f:
        legends = list(csv.DictReader(f))

    # Index stats by playerID
    stats_map = {}
    for row in stats:
        stats_map[row["playerID"]] = row

    # Get legend IDs (these are already complete, skip them)
    legend_ids = set(row["playerID"] for row in legends)

    # Find players with pre-2000 decades who are NOT legends
    candidates = []
    for row in people:
        pid = row["playerID"]
        if pid in legend_ids:
            continue
        decades = row.get("decades", "").split("|")
        # Check if any decade is pre-2000 (1990 or earlier)
        has_pre2000 = any(safe_int(d) <= 1990 for d in decades if d)
        if has_pre2000:
            candidates.append(row)

    print(f"Candidates for backfill: {len(candidates)} (excluding {len(legend_ids)} legends)")

    # ─── Download fantasydatapros yearly data (1970-1998) ───
    # Build: (normalized_name, pos) -> {year -> {stats}}
    historical = {}
    years_available = []

    for year in range(BACKFILL_START, BACKFILL_END + 1):
        url = FDP_URL.format(year=year)
        cache_path = os.path.join(CACHE_DIR, f"{year}.csv")
        sys.stdout.write(f"\rDownloading {year}...")
        sys.stdout.flush()
        rows = download_csv(url, cache_path)
        if not rows:
            continue
        years_available.append(year)

        for row in rows:
            name = row.get("Player", "").strip()
            pos_raw = row.get("Pos", "").strip()
            if not name or not pos_raw:
                continue
            pos = normalize_pos(pos_raw)
            if not pos:
                continue

            norm = normalize_name(name)
            key = (norm, pos)

            if key not in historical:
                historical[key] = {"name": name, "pos": pos, "seasons": {}}

            historical[key]["seasons"][year] = {
                "games": safe_int(row.get("G")),
                "games_started": safe_int(row.get("GS")),
                "passing_yards": safe_int(row.get("PassingYds")),
                "passing_tds": safe_int(row.get("PassingTD")),
                "pass_attempts": safe_int(row.get("PassingAtt")),
                "interceptions": safe_int(row.get("Int")),
                "rushing_yards": safe_int(row.get("RushingYds")),
                "rushing_tds": safe_int(row.get("RushingTD")),
                "rushing_att": safe_int(row.get("RushingAtt")),
                "targets": safe_int(row.get("Tgt")),
                "receptions": safe_int(row.get("Rec")),
                "receiving_yards": safe_int(row.get("ReceivingYds")),
                "receiving_tds": safe_int(row.get("ReceivingTD")),
                "fumbles": safe_int(row.get("Fumbles")),
            }
        # Brief pause to avoid rate limiting
        time.sleep(0.1)

    print(f"\rDownloaded {len(years_available)} yearly files ({min(years_available)}-{max(years_available)})")
    print(f"Historical player-position combos: {len(historical)}")

    # ─── Match candidates to historical data ───
    matched = 0
    unmatched = []
    backfill_data = {}  # playerID -> aggregated pre-1999 stats

    for prow in candidates:
        pid = prow["playerID"]
        name = prow["name"]
        pos = prow["pos"]
        norm = normalize_name(name)
        key = (norm, pos)

        if key not in historical:
            unmatched.append(f"{name} ({pos})")
            continue

        hist = historical[key]
        # Only use seasons strictly before 1999
        pre99_seasons = {y: s for y, s in hist["seasons"].items() if y < 1999}
        if not pre99_seasons:
            continue

        # Aggregate pre-1999 stats
        agg = {
            "games": 0, "games_started": 0,
            "passing_yards": 0, "passing_tds": 0, "pass_attempts": 0,
            "interceptions": 0,
            "rushing_yards": 0, "rushing_tds": 0, "rushing_att": 0,
            "targets": 0, "receptions": 0,
            "receiving_yards": 0, "receiving_tds": 0,
            "fumbles": 0,
            "num_seasons": 0,
            "season_years": set(),
            # Season highs
            "sh_passing_yards": 0, "sh_passing_tds": 0,
            "sh_rushing_yards": 0, "sh_rushing_tds": 0,
            "sh_receptions": 0, "sh_receiving_yards": 0,
            "sh_receiving_tds": 0,
        }

        for year, s in pre99_seasons.items():
            agg["games"] += s["games"]
            agg["games_started"] += s["games_started"]
            agg["passing_yards"] += s["passing_yards"]
            agg["passing_tds"] += s["passing_tds"]
            agg["pass_attempts"] += s["pass_attempts"]
            agg["interceptions"] += s["interceptions"]
            agg["rushing_yards"] += s["rushing_yards"]
            agg["rushing_tds"] += s["rushing_tds"]
            agg["rushing_att"] += s["rushing_att"]
            agg["targets"] += s["targets"]
            agg["receptions"] += s["receptions"]
            agg["receiving_yards"] += s["receiving_yards"]
            agg["receiving_tds"] += s["receiving_tds"]
            agg["fumbles"] += s["fumbles"]
            agg["num_seasons"] += 1
            agg["season_years"].add(year)

            # Track season highs
            agg["sh_passing_yards"] = max(agg["sh_passing_yards"], s["passing_yards"])
            agg["sh_passing_tds"] = max(agg["sh_passing_tds"], s["passing_tds"])
            agg["sh_rushing_yards"] = max(agg["sh_rushing_yards"], s["rushing_yards"])
            agg["sh_rushing_tds"] = max(agg["sh_rushing_tds"], s["rushing_tds"])
            agg["sh_receptions"] = max(agg["sh_receptions"], s["receptions"])
            agg["sh_receiving_yards"] = max(agg["sh_receiving_yards"], s["receiving_yards"])
            agg["sh_receiving_tds"] = max(agg["sh_receiving_tds"], s["receiving_tds"])

        backfill_data[pid] = agg
        matched += 1

    print(f"\nMatched: {matched} players with pre-1999 data")
    print(f"Unmatched: {len(unmatched)} (likely 1999 rookies with no pre-1999 seasons)")

    if not backfill_data:
        print("No backfill data to apply.")
        return

    # ─── Merge into Stats.csv ───
    stats_fields = None
    with open(stats_path) as f:
        reader = csv.DictReader(f)
        stats_fields = reader.fieldnames

    updated_stats = 0
    for srow in stats:
        pid = srow["playerID"]
        if pid not in backfill_data:
            continue

        bf = backfill_data[pid]
        pos_row = next((p for p in people if p["playerID"] == pid), None)
        pos = pos_row["pos"] if pos_row else ""

        # Add counting stats
        srow["careerPassYards"] = str(safe_int(srow["careerPassYards"]) + bf["passing_yards"])
        srow["careerPassTDs"] = str(safe_int(srow["careerPassTDs"]) + bf["passing_tds"])
        srow["careerINTs"] = str(safe_int(srow["careerINTs"]) + bf["interceptions"])
        srow["careerSacks"] = srow["careerSacks"]  # no sacks data in fantasydatapros
        srow["careerGamesStarted"] = str(safe_int(srow["careerGamesStarted"]) + bf["games"])
        srow["careerRushYards"] = str(safe_int(srow["careerRushYards"]) + bf["rushing_yards"])
        srow["careerRushTDs"] = str(safe_int(srow["careerRushTDs"]) + bf["rushing_tds"])
        srow["careerRushAttempts"] = str(safe_int(srow["careerRushAttempts"]) + bf["rushing_att"])
        srow["careerFumbles"] = str(safe_int(srow["careerFumbles"]) + bf["fumbles"])
        srow["careerRecYards"] = str(safe_int(srow["careerRecYards"]) + bf["receiving_yards"])
        srow["careerReceptions"] = str(safe_int(srow["careerReceptions"]) + bf["receptions"])
        srow["careerRecTDs"] = str(safe_int(srow["careerRecTDs"]) + bf["receiving_tds"])
        srow["careerTargets"] = str(safe_int(srow["careerTargets"]) + bf["targets"])

        # QB rush yards
        if pos == "QB":
            srow["careerQBRushYards"] = str(safe_int(srow["careerQBRushYards"]) + bf["rushing_yards"])

        # Total TDs (rush + rec)
        total_rush = safe_int(srow["careerRushTDs"])
        total_rec = safe_int(srow["careerRecTDs"])
        srow["careerTotalTDs"] = str(total_rush + total_rec)

        # Recalculate rate stats
        total_carries = safe_int(srow["careerRushAttempts"])
        total_rush_yds = safe_int(srow["careerRushYards"])
        srow["careerYPC"] = str(round(total_rush_yds / total_carries, 2)) if total_carries > 0 else "0"

        total_rec_count = safe_int(srow["careerReceptions"])
        total_rec_yds = safe_int(srow["careerRecYards"])
        srow["careerYPR"] = str(round(total_rec_yds / total_rec_count, 1)) if total_rec_count > 0 else "0"

        # Note: comp% and passer_rating stay based on 1999+ data only
        # (fantasydatapros doesn't have completions data)

        # Season highs: take max of existing and pre-1999
        srow["seasonHighPassYards"] = str(max(safe_int(srow["seasonHighPassYards"]), bf["sh_passing_yards"]))
        srow["seasonHighPassTDs"] = str(max(safe_int(srow["seasonHighPassTDs"]), bf["sh_passing_tds"]))
        srow["seasonHighRushYards"] = str(max(safe_int(srow["seasonHighRushYards"]), bf["sh_rushing_yards"]))
        srow["seasonHighRushTDs"] = str(max(safe_int(srow.get("seasonHighRushTDs", "0")), bf["sh_rushing_tds"]))
        srow["seasonHighRecYards"] = str(max(safe_int(srow["seasonHighRecYards"]), bf["sh_receiving_yards"]))
        srow["seasonHighReceptions"] = str(max(safe_int(srow["seasonHighReceptions"]), bf["sh_receptions"]))
        srow["seasonHighRecTDs"] = str(max(safe_int(srow.get("seasonHighRecTDs", "0")), bf["sh_receiving_tds"]))

        # Update seasons count
        srow["seasons"] = str(safe_int(srow["seasons"]) + bf["num_seasons"])

        updated_stats += 1

    # Write updated Stats.csv
    with open(stats_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=stats_fields)
        w.writeheader()
        w.writerows(stats)

    print(f"Updated Stats.csv: {updated_stats} players modified")

    # ─── Merge into People.csv ───
    people_fields = None
    with open(people_path) as f:
        reader = csv.DictReader(f)
        people_fields = reader.fieldnames

    updated_people = 0
    for prow in people:
        pid = prow["playerID"]
        if pid not in backfill_data:
            continue

        bf = backfill_data[pid]

        # Update games and seasons
        prow["games"] = str(safe_int(prow["games"]) + bf["games"])
        prow["seasons"] = str(safe_int(prow["seasons"]) + bf["num_seasons"])

        # Update decades
        existing_decades = set(safe_int(d) for d in prow["decades"].split("|") if d)
        for y in bf["season_years"]:
            existing_decades.add((y // 10) * 10)
        prow["decades"] = "|".join(str(d) for d in sorted(existing_decades))

        updated_people += 1

    # Write updated People.csv
    with open(people_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=people_fields)
        w.writeheader()
        w.writerows(people)

    print(f"Updated People.csv: {updated_people} players modified")

    # ─── Print verification stats ───
    print("\n─── Spot Check ───")
    check_names = ["Peyton Manning", "Brett Favre", "Randy Moss", "Edgerrin James",
                   "Marshall Faulk", "Kurt Warner", "Vinny Testaverde", "Rich Gannon",
                   "Tim Brown", "Isaac Bruce"]
    for prow in people:
        if prow["name"] in check_names:
            pid = prow["playerID"]
            s = stats_map.get(pid)
            if not s:
                continue
            # Re-read from our updated stats
            for sr in stats:
                if sr["playerID"] == pid:
                    s = sr
                    break
            bf = backfill_data.get(pid, {})
            pre99_seasons = bf.get("num_seasons", 0) if bf else 0
            print(f"  {prow['name']} ({prow['pos']}): "
                  f"decades={prow['decades']}, seasons={prow['seasons']}, games={prow['games']}, "
                  f"pre99_seasons={pre99_seasons}, "
                  f"passYds={s.get('careerPassYards','?')}, "
                  f"rushYds={s.get('careerRushYards','?')}, "
                  f"recYds={s.get('careerRecYards','?')}")


if __name__ == "__main__":
    main()
