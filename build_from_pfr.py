#!/usr/bin/env python3
"""
Build NFL career stats CSVs using nflverse data sourced from Pro-Football-Reference.

Replaces both build_data.py (nflverse 1999-2025) and backfill_pre1999.py
(fantasydatapros 1970-1998) with a single unified pipeline.

Data sources:
  1. nflverse draft_picks.csv — career totals (from PFR) for all drafted
     players 1980-2025.  Includes PFR player IDs for correct profile links.
  2. nflverse player_stats — weekly stats 1999-2025 for season highs,
     sacks, targets, fumbles, and undrafted free agents.
  3. Legends.csv — manually curated pre-1980 legends (Tarkenton, Montana,
     Payton, Rice, etc.) that predate the draft_picks dataset.

This eliminates the fragile name-matching merge that caused incorrect stats
for 376 overlapping players (e.g., Favre showing 44,964 instead of 71,838
pass yards).

Usage:
    python3 build_from_pfr.py
"""

import csv
import io
import math
import os
import sys

import requests

OUT_DIR = os.path.dirname(os.path.abspath(__file__))

DRAFT_PICKS_URL = "https://github.com/nflverse/nflverse-data/releases/download/draft_picks/draft_picks.csv"
PLAYER_STATS_URL = "https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{year}.csv"
START_YEAR = 1999
END_YEAR = 2025

VALID_POSITIONS = {"QB", "RB", "WR", "TE", "FB"}


def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def safe_float(val, default=0.0):
    try:
        v = float(val)
        return v if not math.isnan(v) else default
    except (ValueError, TypeError):
        return default


def calc_passer_rating(comp, att, yds, td, interceptions):
    """NFL passer rating formula."""
    if att == 0:
        return 0.0
    a = max(0, min(((comp / att) - 0.3) * 5, 2.375))
    b = max(0, min(((yds / att) - 3) * 0.25, 2.375))
    c = max(0, min((td / att) * 20, 2.375))
    d = max(0, min(2.375 - ((interceptions / att) * 25), 2.375))
    return round(((a + b + c + d) / 6) * 100, 1)


def download_csv(url, label=""):
    """Download and parse a CSV from a URL. Returns list of dicts."""
    try:
        resp = requests.get(url, timeout=120)
    except requests.RequestException as e:
        print(f"\n  WARNING: Failed to download {label or url}: {e}")
        return []
    if resp.status_code != 200:
        print(f"\n  WARNING: HTTP {resp.status_code} for {label or url}")
        return []
    return list(csv.DictReader(io.StringIO(resp.text)))


def main():
    # ═══════════════════════════════════════════════════════════════════════
    # Stage 1: Download nflverse draft_picks (career totals from PFR)
    # ═══════════════════════════════════════════════════════════════════════
    print("Stage 1: Downloading nflverse draft_picks (career totals from PFR)...")
    draft_rows = download_csv(DRAFT_PICKS_URL, "draft_picks")
    print(f"  Downloaded {len(draft_rows)} draft picks")

    # Build lookup: gsis_id -> draft pick data, pfr_id -> draft pick data
    # Filter to skill positions
    draft_by_gsis = {}
    draft_by_pfr = {}
    for row in draft_rows:
        pos = row.get("position", "").strip()
        if pos not in VALID_POSITIONS:
            continue
        gsis = row.get("gsis_id", "").strip()
        pfr = row.get("pfr_player_id", "").strip()
        if gsis and gsis != "NA":
            draft_by_gsis[gsis] = row
        if pfr and pfr != "NA":
            draft_by_pfr[pfr] = row

    print(f"  Skill-position draft picks: {len(draft_by_pfr)} with PFR IDs, {len(draft_by_gsis)} with GSIS IDs")

    # ═══════════════════════════════════════════════════════════════════════
    # Stage 2: Download nflverse player_stats (1999-2025) for season highs
    #          and supplementary stats (sacks, targets, fumbles)
    # ═══════════════════════════════════════════════════════════════════════
    print("\nStage 2: Downloading nflverse player_stats (1999-2025)...")
    weekly_players = {}   # gsis_id -> accumulated stats
    season_highs = {}     # gsis_id -> {stat: max_value}
    season_data = {}      # gsis_id -> {year: {stat: value}}

    for year in range(START_YEAR, END_YEAR + 1):
        url = PLAYER_STATS_URL.format(year=year)
        sys.stdout.write(f"\r  Downloading stats {year}...")
        sys.stdout.flush()
        rows = download_csv(url, label=f"stats {year}")
        if not rows:
            continue

        # Aggregate by player+season for season highs
        season_agg = {}  # (gsis_id, season) -> {stat: value}

        for row in rows:
            pid = row.get("player_id", "").strip()
            if not pid:
                continue
            season_type = row.get("season_type", "REG")
            if season_type != "REG":
                continue

            season = safe_int(row.get("season", year))
            name = row.get("player_display_name", row.get("player_name", "")).strip()
            pos = row.get("position", "").strip()

            if pid not in weekly_players:
                weekly_players[pid] = {
                    "name": name, "pos": pos,
                    "seasons": set(), "games": 0,
                    "completions": 0, "pass_attempts": 0,
                    "passing_yards": 0, "passing_tds": 0,
                    "interceptions": 0, "sacks": 0,
                    "carries": 0, "rushing_yards": 0, "rushing_tds": 0,
                    "rushing_fumbles": 0,
                    "receptions": 0, "targets": 0,
                    "receiving_yards": 0, "receiving_tds": 0,
                    "receiving_fumbles": 0,
                }
            p = weekly_players[pid]
            if name:
                p["name"] = name
            if pos and pos in VALID_POSITIONS:
                p["pos"] = pos

            p["seasons"].add(season)
            p["games"] += 1
            p["completions"] += safe_int(row.get("completions"))
            p["pass_attempts"] += safe_int(row.get("attempts"))
            p["passing_yards"] += safe_int(row.get("passing_yards"))
            p["passing_tds"] += safe_int(row.get("passing_tds"))
            p["interceptions"] += safe_int(row.get("interceptions"))
            p["sacks"] += safe_int(row.get("sacks"))
            p["carries"] += safe_int(row.get("carries"))
            p["rushing_yards"] += safe_int(row.get("rushing_yards"))
            p["rushing_tds"] += safe_int(row.get("rushing_tds"))
            p["rushing_fumbles"] += safe_int(row.get("rushing_fumbles"))
            p["receptions"] += safe_int(row.get("receptions"))
            p["targets"] += safe_int(row.get("targets"))
            p["receiving_yards"] += safe_int(row.get("receiving_yards"))
            p["receiving_tds"] += safe_int(row.get("receiving_tds"))
            p["receiving_fumbles"] += safe_int(row.get("receiving_fumbles"))

            # Season-level aggregation
            key = (pid, season)
            if key not in season_agg:
                season_agg[key] = {
                    "passing_yards": 0, "passing_tds": 0,
                    "rushing_yards": 0, "rushing_tds": 0,
                    "receptions": 0, "receiving_yards": 0, "receiving_tds": 0,
                }
            sa = season_agg[key]
            sa["passing_yards"] += safe_int(row.get("passing_yards"))
            sa["passing_tds"] += safe_int(row.get("passing_tds"))
            sa["rushing_yards"] += safe_int(row.get("rushing_yards"))
            sa["rushing_tds"] += safe_int(row.get("rushing_tds"))
            sa["receptions"] += safe_int(row.get("receptions"))
            sa["receiving_yards"] += safe_int(row.get("receiving_yards"))
            sa["receiving_tds"] += safe_int(row.get("receiving_tds"))

        for (pid, season), sa in season_agg.items():
            if pid not in season_highs:
                season_highs[pid] = {}
            sh = season_highs[pid]
            for stat, val in sa.items():
                if stat not in sh or val > sh[stat]:
                    sh[stat] = val

    print(f"\r  Downloaded stats 1999-2025. Players in weekly data: {len(weekly_players)}    ")

    # ═══════════════════════════════════════════════════════════════════════
    # Stage 3: Merge — draft_picks as primary, supplemented by player_stats
    # ═══════════════════════════════════════════════════════════════════════
    print("\nStage 3: Merging data sources...")

    people_rows = []
    stats_rows = []
    seen_gsis = set()  # track which GSIS IDs we've already processed

    # 3A: Process all draft picks with PFR IDs (1980-2025)
    for pfr_id, dp in draft_by_pfr.items():
        pos = dp["position"].strip()
        if pos not in VALID_POSITIONS:
            continue
        if pos == "FB":
            pos = "RB"

        name = dp.get("pfr_player_name", "").strip()
        if not name:
            continue

        gsis_id = dp.get("gsis_id", "").strip()
        if gsis_id == "NA":
            gsis_id = ""

        # Career totals from draft_picks (sourced from PFR)
        completions = safe_int(dp.get("pass_completions"))
        pass_att = safe_int(dp.get("pass_attempts"))
        pass_yds = safe_int(dp.get("pass_yards"))
        pass_tds = safe_int(dp.get("pass_tds"))
        pass_ints = safe_int(dp.get("pass_ints"))
        rush_att = safe_int(dp.get("rush_atts"))
        rush_yds = safe_int(dp.get("rush_yards"))
        rush_tds = safe_int(dp.get("rush_tds"))
        receptions = safe_int(dp.get("receptions"))
        rec_yds = safe_int(dp.get("rec_yards"))
        rec_tds = safe_int(dp.get("rec_tds"))
        games = safe_int(dp.get("games"))

        draft_year = safe_int(dp.get("season"))
        last_year = safe_int(dp.get("to"))
        if last_year == 0:
            last_year = draft_year
        seasons_started = safe_int(dp.get("seasons_started"))

        # Calculate decades from draft year to last year
        decades = set()
        for y in range(draft_year, last_year + 1):
            decades.add((y // 10) * 10)
        num_seasons = max(1, last_year - draft_year + 1) if last_year >= draft_year else 1

        # Supplementary stats from weekly data (sacks, targets, fumbles, season highs)
        sacks = 0
        targets = 0
        fumbles = 0
        sh = {}

        if gsis_id and gsis_id in weekly_players:
            wp = weekly_players[gsis_id]
            sacks = wp["sacks"]
            targets = wp["targets"]
            fumbles = wp["rushing_fumbles"] + wp["receiving_fumbles"]
            sh = season_highs.get(gsis_id, {})
            seen_gsis.add(gsis_id)

        # Derived stats
        comp_pct = round(completions / pass_att, 3) if pass_att > 0 else 0
        passer_rating = calc_passer_rating(completions, pass_att, pass_yds, pass_tds, pass_ints)
        ypc = round(rush_yds / rush_att, 2) if rush_att > 0 else 0
        ypr = round(rec_yds / receptions, 1) if receptions > 0 else 0

        people_rows.append({
            "playerID": pfr_id,
            "name": name,
            "pos": pos,
            "decades": "|".join(str(d) for d in sorted(decades)),
            "seasons": num_seasons,
            "games": games,
        })

        stats_rows.append({
            "playerID": pfr_id,
            "careerPassYards": pass_yds,
            "careerPassTDs": pass_tds,
            "careerINTs": pass_ints,
            "careerPasserRating": passer_rating,
            "careerCompPct": comp_pct,
            "careerSacks": sacks,
            "careerGamesStarted": games,  # best approximation
            "careerQBRushYards": rush_yds if pos == "QB" else 0,
            "seasonHighPassYards": sh.get("passing_yards", 0),
            "seasonHighPassTDs": sh.get("passing_tds", 0),
            "careerRushYards": rush_yds,
            "careerRushTDs": rush_tds,
            "careerYPC": ypc,
            "careerRushAttempts": rush_att,
            "careerFumbles": fumbles,
            "seasonHighRushYards": sh.get("rushing_yards", 0),
            "careerRecYards": rec_yds,
            "careerReceptions": receptions,
            "careerRecTDs": rec_tds,
            "careerYPR": ypr,
            "seasonHighRecYards": sh.get("receiving_yards", 0),
            "seasonHighReceptions": sh.get("receptions", 0),
            "careerTargets": targets,
            "careerTotalTDs": rush_tds + rec_tds,
            "seasonHighRecTDs": sh.get("receiving_tds", 0),
            "seasonHighRushTDs": sh.get("rushing_tds", 0),
            "seasons": num_seasons,
        })

    draft_count = len(people_rows)
    print(f"  Draft picks processed: {draft_count}")

    # 3B: Add undrafted players from weekly data (only those not already covered)
    undrafted_count = 0
    for gsis_id, wp in weekly_players.items():
        if gsis_id in seen_gsis:
            continue
        pos = wp["pos"]
        if pos not in VALID_POSITIONS:
            continue
        if pos == "FB":
            pos = "RB"

        name = wp["name"]
        if not name:
            continue

        seasons = sorted(wp["seasons"])
        if not seasons:
            continue

        decades = sorted(set((s // 10) * 10 for s in seasons))
        num_seasons = len(seasons)
        games = wp["games"]

        comp_pct = round(wp["completions"] / wp["pass_attempts"], 3) if wp["pass_attempts"] > 0 else 0
        passer_rating = calc_passer_rating(
            wp["completions"], wp["pass_attempts"],
            wp["passing_yards"], wp["passing_tds"], wp["interceptions"]
        )
        ypc = round(wp["rushing_yards"] / wp["carries"], 2) if wp["carries"] > 0 else 0
        ypr = round(wp["receiving_yards"] / wp["receptions"], 1) if wp["receptions"] > 0 else 0
        fumbles = wp["rushing_fumbles"] + wp["receiving_fumbles"]
        sh = season_highs.get(gsis_id, {})

        # Check if we have a PFR ID via draft_by_gsis (shouldn't, but check)
        dp = draft_by_gsis.get(gsis_id)
        player_id = dp.get("pfr_player_id", gsis_id) if dp else gsis_id

        people_rows.append({
            "playerID": player_id,
            "name": name,
            "pos": pos,
            "decades": "|".join(str(d) for d in decades),
            "seasons": num_seasons,
            "games": games,
        })

        stats_rows.append({
            "playerID": player_id,
            "careerPassYards": wp["passing_yards"],
            "careerPassTDs": wp["passing_tds"],
            "careerINTs": wp["interceptions"],
            "careerPasserRating": passer_rating,
            "careerCompPct": comp_pct,
            "careerSacks": wp["sacks"],
            "careerGamesStarted": games,
            "careerQBRushYards": wp["rushing_yards"] if pos == "QB" else 0,
            "seasonHighPassYards": sh.get("passing_yards", 0),
            "seasonHighPassTDs": sh.get("passing_tds", 0),
            "careerRushYards": wp["rushing_yards"],
            "careerRushTDs": wp["rushing_tds"],
            "careerYPC": ypc,
            "careerRushAttempts": wp["carries"],
            "careerFumbles": fumbles,
            "seasonHighRushYards": sh.get("rushing_yards", 0),
            "careerRecYards": wp["receiving_yards"],
            "careerReceptions": wp["receptions"],
            "careerRecTDs": wp["receiving_tds"],
            "careerYPR": ypr,
            "seasonHighRecYards": sh.get("receiving_yards", 0),
            "seasonHighReceptions": sh.get("receptions", 0),
            "careerTargets": wp["targets"],
            "careerTotalTDs": wp["rushing_tds"] + wp["receiving_tds"],
            "seasonHighRecTDs": sh.get("receiving_tds", 0),
            "seasonHighRushTDs": sh.get("rushing_tds", 0),
            "seasons": num_seasons,
        })
        undrafted_count += 1

    print(f"  Undrafted/weekly-only players added: {undrafted_count}")
    print(f"  Total players: {len(people_rows)}")

    # ═══════════════════════════════════════════════════════════════════════
    # Stage 4: Write output CSVs
    # ═══════════════════════════════════════════════════════════════════════
    people_path = os.path.join(OUT_DIR, "People.csv")
    with open(people_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["playerID", "name", "pos", "decades", "seasons", "games"])
        w.writeheader()
        w.writerows(sorted(people_rows, key=lambda r: r["name"]))

    stats_fields = [
        "playerID",
        "careerPassYards", "careerPassTDs", "careerINTs",
        "careerPasserRating", "careerCompPct", "careerSacks",
        "careerGamesStarted", "careerQBRushYards",
        "seasonHighPassYards", "seasonHighPassTDs",
        "careerRushYards", "careerRushTDs", "careerYPC", "careerRushAttempts",
        "careerFumbles", "seasonHighRushYards",
        "careerRecYards", "careerReceptions", "careerRecTDs", "careerYPR",
        "seasonHighRecYards", "seasonHighReceptions",
        "careerTargets", "careerTotalTDs",
        "seasonHighRecTDs", "seasonHighRushTDs",
        "seasons",
    ]
    stats_path = os.path.join(OUT_DIR, "Stats.csv")
    with open(stats_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=stats_fields)
        w.writeheader()
        w.writerows(sorted(stats_rows, key=lambda r: r["playerID"]))

    print(f"\nOutput:")
    print(f"  {people_path} ({len(people_rows)} players)")
    print(f"  {stats_path} ({len(stats_rows)} stat rows)")

    # Summary
    pos_counts = {}
    for r in people_rows:
        pos_counts[r["pos"]] = pos_counts.get(r["pos"], 0) + 1
    print(f"\nBy position: {pos_counts}")

    # ─── Verification spot-checks ────
    stats_map = {r["playerID"]: r for r in stats_rows}
    checks = [
        ("BradTo00", "Tom Brady", "careerPassYards", 89214),
        ("FavrBr00", "Brett Favre", "careerPassYards", 71838),
        ("FaulMa00", "Marshall Faulk", "careerRushYards", 12279),
        ("RiceJe00", "Jerry Rice", "careerRecYards", 22895),
        ("MannPe00", "Peyton Manning", "careerPassYards", 71940),
    ]
    print("\nVerification spot-checks:")
    for pid, name, stat, expected in checks:
        actual = stats_map.get(pid, {}).get(stat, "NOT FOUND")
        status = "OK" if actual == expected else f"MISMATCH (expected {expected})"
        print(f"  {name}: {stat} = {actual} — {status}")

    # Check that Legends.csv players are noted
    legends_path = os.path.join(OUT_DIR, "Legends.csv")
    if os.path.exists(legends_path):
        with open(legends_path, "r") as f:
            legends = list(csv.DictReader(f))
        legend_names = [r["name"] for r in legends]
        covered = sum(1 for r in legends if r["playerID"] in stats_map)
        print(f"\nLegends.csv: {len(legends)} legends, {covered} now covered by draft_picks data")
        print(f"  Still need Legends.csv for: {[r['name'] for r in legends if r['playerID'] not in stats_map]}")


if __name__ == "__main__":
    main()
