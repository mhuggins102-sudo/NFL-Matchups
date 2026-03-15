#!/usr/bin/env python3
"""
Build NFL career stats CSVs from nflverse open-source data.

Downloads weekly player stats (1999-2024) from nflverse GitHub releases,
aggregates into career totals, and outputs People.csv and Stats.csv
for the NFL Matchups app.

Usage:
    python3 build_data.py
"""

import csv
import io
import math
import os
import sys
import requests

STATS_URL = "https://github.com/nflverse/nflverse-data/releases/download/player_stats/player_stats_{year}.csv"
ROSTER_URL = "https://github.com/nflverse/nflverse-data/releases/download/rosters/roster_{year}.csv"
START_YEAR = 1999
END_YEAR = 2025
OUT_DIR = os.path.dirname(os.path.abspath(__file__))


def download_csv(url, label=""):
    """Download and parse a CSV from a URL. Returns list of dicts."""
    try:
        resp = requests.get(url, timeout=60)
    except requests.RequestException as e:
        print(f"\n  WARNING: Failed to download {label or url}: {e}")
        return []
    if resp.status_code != 200:
        print(f"\n  WARNING: HTTP {resp.status_code} for {label or url} — data will be missing!")
        return []
    reader = csv.DictReader(io.StringIO(resp.text))
    return list(reader)


def safe_float(val, default=0.0):
    try:
        v = float(val)
        return v if not math.isnan(v) else default
    except (ValueError, TypeError):
        return default


def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def calc_passer_rating(comp, att, yds, td, interceptions):
    """Calculate NFL passer rating from career totals."""
    if att == 0:
        return 0.0
    a = max(0, min(((comp / att) - 0.3) * 5, 2.375))
    b = max(0, min(((yds / att) - 3) * 0.25, 2.375))
    c = max(0, min((td / att) * 20, 2.375))
    d = max(0, min(2.375 - ((interceptions / att) * 25), 2.375))
    return round(((a + b + c + d) / 6) * 100, 1)


def main():
    # ─── Phase 1: Download weekly stats for all years ───
    players = {}  # player_id -> accumulated data
    season_highs = {}  # player_id -> {stat: {season: val}}

    for year in range(START_YEAR, END_YEAR + 1):
        url = STATS_URL.format(year=year)
        sys.stdout.write(f"\rDownloading stats {year}...")
        sys.stdout.flush()
        rows = download_csv(url, label=f"stats {year}")
        if not rows:
            print(f" [skipped — no data returned]")
            continue

        # Aggregate by player+season first for season highs
        season_agg = {}  # (player_id, season) -> stats

        for row in rows:
            pid = row.get("player_id", "").strip()
            if not pid:
                continue

            season = safe_int(row.get("season", year))
            season_type = row.get("season_type", "REG")
            # Only count regular season stats
            if season_type != "REG":
                continue

            name = row.get("player_display_name", row.get("player_name", "")).strip()
            pos = row.get("position", "").strip()

            # Initialize player record
            if pid not in players:
                players[pid] = {
                    "id": pid,
                    "name": name,
                    "pos": pos,
                    "seasons": set(),
                    "games": 0,
                    # Passing
                    "completions": 0, "pass_attempts": 0,
                    "passing_yards": 0, "passing_tds": 0,
                    "interceptions": 0, "sacks": 0,
                    # Rushing
                    "carries": 0, "rushing_yards": 0, "rushing_tds": 0,
                    "rushing_fumbles": 0,
                    # Receiving
                    "receptions": 0, "targets": 0,
                    "receiving_yards": 0, "receiving_tds": 0,
                    "receiving_fumbles": 0,
                }

            p = players[pid]
            # Update name/pos if newer (prefer most recent)
            if name:
                p["name"] = name
            if pos and pos in ("QB", "RB", "WR", "TE", "FB"):
                p["pos"] = pos

            p["seasons"].add(season)

            # Count games (any week with stats = 1 game)
            p["games"] += 1

            # Accumulate career totals
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

            # Track season-level for season highs
            key = (pid, season)
            if key not in season_agg:
                season_agg[key] = {
                    "passing_yards": 0, "passing_tds": 0,
                    "rushing_yards": 0, "rushing_tds": 0,
                    "receptions": 0,
                    "receiving_yards": 0, "receiving_tds": 0,
                }
            sa = season_agg[key]
            sa["passing_yards"] += safe_int(row.get("passing_yards"))
            sa["passing_tds"] += safe_int(row.get("passing_tds"))
            sa["rushing_yards"] += safe_int(row.get("rushing_yards"))
            sa["rushing_tds"] += safe_int(row.get("rushing_tds"))
            sa["receptions"] += safe_int(row.get("receptions"))
            sa["receiving_yards"] += safe_int(row.get("receiving_yards"))
            sa["receiving_tds"] += safe_int(row.get("receiving_tds"))

        # Update season highs
        for (pid, season), sa in season_agg.items():
            if pid not in season_highs:
                season_highs[pid] = {}
            sh = season_highs[pid]
            for stat, val in sa.items():
                if stat not in sh or val > sh[stat]:
                    sh[stat] = val

    print(f"\rDownloaded stats for {START_YEAR}-{END_YEAR}. Total players: {len(players)}")

    # ─── Phase 2: Download roster data for games started ───
    gs_counts = {}  # player_id -> games started count (approximate)

    for year in range(START_YEAR, END_YEAR + 1):
        url = ROSTER_URL.format(year=year)
        sys.stdout.write(f"\rDownloading roster {year}...")
        sys.stdout.flush()
        rows = download_csv(url, label=f"roster {year}")
        if not rows:
            continue

        # Collect unique player entries per season
        seen = set()
        for row in rows:
            pid = row.get("gsis_id", "").strip()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            # Update player position/name from roster if available
            if pid in players:
                pos = row.get("position", "").strip()
                name = row.get("full_name", "").strip()
                if pos and pos in ("QB", "RB", "WR", "TE", "FB"):
                    players[pid]["pos"] = pos
                if name:
                    players[pid]["name"] = name

    print(f"\rRoster data processed.                    ")

    # ─── Phase 3: Filter and classify players ───
    # Only keep QB, RB, WR, TE, FB (reclassify FB as RB)
    valid_pos = {"QB", "RB", "WR", "TE", "FB"}
    filtered = {}
    for pid, p in players.items():
        pos = p["pos"]
        if pos not in valid_pos:
            continue
        if pos == "FB":
            p["pos"] = "RB"
        filtered[pid] = p

    print(f"Filtered to skill positions: {len(filtered)} players")

    # ─── Phase 4: Build output CSVs ───
    people_rows = []
    stats_rows = []

    for pid, p in filtered.items():
        seasons = sorted(p["seasons"])
        if not seasons:
            continue

        decades = sorted(set((s // 10) * 10 for s in seasons))
        num_seasons = len(seasons)
        games = p["games"]
        pos = p["pos"]

        # Calculate derived stats
        comp_pct = round(p["completions"] / p["pass_attempts"], 3) if p["pass_attempts"] > 0 else 0
        passer_rating = calc_passer_rating(
            p["completions"], p["pass_attempts"],
            p["passing_yards"], p["passing_tds"], p["interceptions"]
        )
        ypc = round(p["rushing_yards"] / p["carries"], 2) if p["carries"] > 0 else 0
        ypr = round(p["receiving_yards"] / p["receptions"], 1) if p["receptions"] > 0 else 0

        # Season highs
        sh = season_highs.get(pid, {})

        people_rows.append({
            "playerID": pid,
            "name": p["name"],
            "pos": pos,
            "decades": "|".join(str(d) for d in decades),
            "seasons": num_seasons,
            "games": games,
        })

        stats_rows.append({
            "playerID": pid,
            "careerPassYards": p["passing_yards"],
            "careerPassTDs": p["passing_tds"],
            "careerINTs": p["interceptions"],
            "careerPasserRating": passer_rating,
            "careerCompPct": comp_pct,
            "careerSacks": p["sacks"],
            "careerGamesStarted": games,  # approx: games with stats
            "careerQBRushYards": p["rushing_yards"] if pos == "QB" else 0,
            "seasonHighPassYards": sh.get("passing_yards", 0),
            "seasonHighPassTDs": sh.get("passing_tds", 0),
            "careerRushYards": p["rushing_yards"],
            "careerRushTDs": p["rushing_tds"],
            "careerYPC": ypc,
            "careerRushAttempts": p["carries"],
            "careerFumbles": p["rushing_fumbles"] + p["receiving_fumbles"],
            "seasonHighRushYards": sh.get("rushing_yards", 0),
            "careerRecYards": p["receiving_yards"],
            "careerReceptions": p["receptions"],
            "careerRecTDs": p["receiving_tds"],
            "careerYPR": ypr,
            "seasonHighRecYards": sh.get("receiving_yards", 0),
            "seasonHighReceptions": sh.get("receptions", 0),
            "careerTargets": p["targets"],
            "careerTotalTDs": p["rushing_tds"] + p["receiving_tds"],
            "seasonHighRecTDs": sh.get("receiving_tds", 0),
            "seasonHighRushTDs": sh.get("rushing_tds", 0),
            "seasons": num_seasons,
        })

    # Write People.csv
    people_path = os.path.join(OUT_DIR, "People.csv")
    with open(people_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["playerID", "name", "pos", "decades", "seasons", "games"])
        w.writeheader()
        w.writerows(sorted(people_rows, key=lambda r: r["name"]))

    # Write Stats.csv
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

    # Summary stats
    pos_counts = {}
    for r in people_rows:
        pos_counts[r["pos"]] = pos_counts.get(r["pos"], 0) + 1
    print(f"\nBy position: {pos_counts}")

    # Eligibility tier counts
    vet = sum(1 for r in stats_rows if (
        (next(p for p in people_rows if p["playerID"] == r["playerID"])["pos"] == "QB" and r["careerGamesStarted"] >= 64) or
        (next(p for p in people_rows if p["playerID"] == r["playerID"])["pos"] == "RB" and r["careerRushAttempts"] >= 750) or
        (next(p for p in people_rows if p["playerID"] == r["playerID"])["pos"] in ("WR", "TE") and r["careerReceptions"] >= 250)
    ))
    print(f"Veteran-eligible: ~{vet} players")


if __name__ == "__main__":
    main()
