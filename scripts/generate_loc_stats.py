#!/usr/bin/env python3
"""Generate a gruvbox-themed SVG card showing lines of code edited across all repos."""

import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GITHUB_USER = os.environ.get("GITHUB_USER", "ctf05")

TOKENS = {
    "personal": os.environ.get("LOC_STATS_TOKEN_PERSONAL", ""),
    "LOME-AI": os.environ.get("LOC_STATS_TOKEN_LOME_AI", ""),
}

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
CACHE_PATH = REPO_ROOT / "data" / "loc-cache.json"
SVG_PATH = REPO_ROOT / "loc-stats.svg"

API_BASE = "https://api.github.com"
PASS2_WAIT = 10  # seconds to wait between pass 1 and pass 2 for 202 repos
RATE_LIMIT_FLOOR = 100  # pause if remaining drops below this

TIME_FRAMES = {
    "All Time": 0,
    "Last Year": 365,
    "Last Month": 30,
    "Last Week": 7,
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("loc-stats")

# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------


def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _check_rate_limit(resp: requests.Response, token_label: str) -> None:
    remaining = resp.headers.get("X-RateLimit-Remaining")
    if remaining is not None:
        remaining = int(remaining)
        if remaining < RATE_LIMIT_FLOOR:
            reset_ts = int(resp.headers.get("X-RateLimit-Reset", 0))
            wait = max(reset_ts - int(time.time()), 1)
            log.warning(
                "  [RATE LIMIT] %s — %d remaining, sleeping %ds until reset",
                token_label,
                remaining,
                wait,
            )
            time.sleep(wait)


def list_user_repos(token: str, token_label: str) -> list[dict]:
    """Return all repos accessible via *token* (paginated)."""
    repos: list[dict] = []
    url = (
        f"{API_BASE}/user/repos"
        "?affiliation=owner,collaborator,organization_member"
        "&visibility=all&per_page=100&sort=pushed"
    )
    page = 1
    while url:
        log.info("  GET %s (page %d)", url.split("?")[0], page)
        resp = requests.get(url, headers=_headers(token), timeout=30)
        log.info("    → %d — rate limit remaining: %s",
                 resp.status_code, resp.headers.get("X-RateLimit-Remaining", "?"))
        _check_rate_limit(resp, token_label)
        resp.raise_for_status()
        repos.extend(resp.json())
        # Follow pagination via Link header
        url = resp.links.get("next", {}).get("url")
        page += 1
    return repos


def get_contributor_stats(
    token: str, repo_full_name: str, etag: str | None = None
) -> tuple[list[dict] | None, str | None, int]:
    """Fetch weekly contributor stats for a repo (single attempt).

    Returns (weeks_for_user, new_etag, http_status).
    - weeks_for_user is None on 304 (cache hit), 202 (computing), or if user not found.
    - http_status is the final status code (200, 202, 304, etc.).
    """
    url = f"{API_BASE}/repos/{repo_full_name}/stats/contributors"
    headers = _headers(token)
    if etag:
        headers["If-None-Match"] = etag

    resp = requests.get(url, headers=headers, timeout=30)
    status = resp.status_code
    new_etag = resp.headers.get("ETag")

    if status == 304:
        return None, etag, 304

    if status == 202:
        return None, new_etag, 202

    if status == 200:
        _check_rate_limit(resp, repo_full_name)
        contributors = resp.json()
        for contrib in contributors:
            if contrib.get("author", {}).get("login", "").lower() == GITHUB_USER.lower():
                return contrib.get("weeks", []), new_etag, 200
        # User has no commits in this repo
        return [], new_etag, 200

    # Other error
    log.warning("    → %d — %s", status, resp.text[:200])
    resp.raise_for_status()
    return None, new_etag, status


def clone_and_count(
    token: str, repo_full_name: str
) -> list[dict]:
    """Fallback: blobless clone + git log to count additions/deletions.

    Returns a list of synthetic week entries [{w, a, d}] grouped by week.
    Note: log messages here must NOT include repo_full_name (may be private).
    The caller is responsible for logging the display name.
    """
    clone_url = f"https://x-access-token:{token}@github.com/{repo_full_name}.git"
    tmpdir = tempfile.mkdtemp(prefix="loc-stats-")
    try:
        log.info("    Cloning (blobless)...")
        subprocess.run(
            ["git", "clone", "--filter=blob:none", "--bare", clone_url, tmpdir + "/repo"],
            capture_output=True,
            text=True,
            check=True,
            timeout=300,
        )
        repo_path = tmpdir + "/repo"

        log.info("    Running git log --numstat ...")
        result = subprocess.run(
            [
                "git", "-C", repo_path, "log",
                f"--author={GITHUB_USER}",
                "--pretty=format:%at",  # unix timestamp
                "--numstat",
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=300,
        )

        # Parse git log output into weekly buckets
        weekly: dict[int, dict] = {}  # week_start_ts -> {a, d}
        current_ts = None

        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            # Timestamp line (just a number)
            if line.isdigit():
                current_ts = int(line)
                continue
            # numstat line: additions\tdeletions\tfilename
            parts = line.split("\t")
            if len(parts) >= 3 and current_ts is not None:
                try:
                    adds = int(parts[0]) if parts[0] != "-" else 0
                    dels = int(parts[1]) if parts[1] != "-" else 0
                except ValueError:
                    continue
                # Round timestamp down to week start (Monday)
                dt = datetime.fromtimestamp(current_ts, tz=timezone.utc)
                week_start = dt - timedelta(days=dt.weekday())
                week_ts = int(week_start.replace(hour=0, minute=0, second=0).timestamp())
                if week_ts not in weekly:
                    weekly[week_ts] = {"a": 0, "d": 0}
                weekly[week_ts]["a"] += adds
                weekly[week_ts]["d"] += dels

        weeks = [{"w": ts, "a": data["a"], "d": data["d"]} for ts, data in sorted(weekly.items())]
        total_a = sum(w["a"] for w in weeks)
        total_d = sum(w["d"] for w in weeks)
        log.info("    git log complete — +%s / -%s", f"{total_a:,}", f"{total_d:,}")
        return weeks

    except subprocess.TimeoutExpired:
        log.warning("    Clone/log timed out")
        return []
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr[:200] if e.stderr else str(e))
        stderr = stderr.replace(token, "***")
        stderr = stderr.replace(repo_full_name, "***")
        log.warning("    Clone/log failed: %s", stderr)
        return []
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------


def load_cache() -> dict:
    if CACHE_PATH.exists():
        with open(CACHE_PATH) as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {}
    return {}


def save_cache(cache: dict) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    cache["last_updated"] = datetime.now(timezone.utc).isoformat()
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f, indent=2)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def compute_timeframes(cache: dict) -> dict[str, dict]:
    """Aggregate cached weekly data into time-frame buckets."""
    now = datetime.now(timezone.utc)
    boundaries = {}
    for label, days in TIME_FRAMES.items():
        if days == 0:
            boundaries[label] = 0
        else:
            boundaries[label] = int((now - timedelta(days=days)).timestamp())

    result = {label: {"additions": 0, "deletions": 0} for label in TIME_FRAMES}

    repos = cache.get("repos", {})
    for repo_name, repo_data in repos.items():
        weeks = repo_data.get("weeks", {})
        for week_ts_str, week in weeks.items():
            week_ts = int(week_ts_str)
            for label, start_ts in boundaries.items():
                if week_ts >= start_ts:
                    result[label]["additions"] += week.get("a", 0)
                    result[label]["deletions"] += week.get("d", 0)

    # Compute totals
    for label in result:
        r = result[label]
        r["total"] = r["additions"] + r["deletions"]

    return result


# ---------------------------------------------------------------------------
# SVG generation
# ---------------------------------------------------------------------------


def _fmt(n: int) -> str:
    """Format number with commas."""
    return f"{n:,}"


def generate_svg(stats: dict[str, dict]) -> str:
    """Generate a gruvbox-themed SVG card."""
    # Colors
    bg = "#282828"
    border = "#3c3836"
    title_color = "#fabd2f"
    label_color = "#8ec07c"
    value_color = "#ebdbb2"
    add_color = "#b8bb26"
    del_color = "#fb4934"
    icon_color = "#fe8019"
    muted_color = "#a89984"

    # Build stat rows
    rows_svg = ""
    y = 65
    for label in TIME_FRAMES:
        s = stats.get(label, {"additions": 0, "deletions": 0, "total": 0})
        rows_svg += f'  <text x="25" y="{y}" fill="{label_color}" font-size="14" font-family="\'Segoe UI\', Ubuntu, \'Helvetica Neue\', Sans-Serif" font-weight="400">{label}</text>\n'
        rows_svg += f'  <text x="200" y="{y}" fill="{value_color}" font-size="14" font-family="\'Segoe UI\', Ubuntu, \'Helvetica Neue\', Sans-Serif" font-weight="700">{_fmt(s["total"])} lines</text>\n'
        y += 20
        rows_svg += f'  <text x="200" y="{y}" font-size="12" font-family="\'Segoe UI\', Ubuntu, \'Helvetica Neue\', Sans-Serif">'
        rows_svg += f'<tspan fill="{add_color}">+{_fmt(s["additions"])}</tspan>'
        rows_svg += f'  <tspan fill="{muted_color}">/</tspan>  '
        rows_svg += f'<tspan fill="{del_color}">-{_fmt(s["deletions"])}</tspan>'
        rows_svg += "</text>\n"
        y += 30

    # Code icon (simple brackets icon)
    icon_svg = (
        f'<g transform="translate(25, 18)">'
        f'<path d="M8 2L2 8L8 14" stroke="{icon_color}" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/>'
        f'<path d="M16 2L22 8L16 14" stroke="{icon_color}" stroke-width="2" fill="none" stroke-linecap="round" stroke-linejoin="round"/>'
        f'<line x1="13" y1="0" x2="11" y2="16" stroke="{icon_color}" stroke-width="2" stroke-linecap="round"/>'
        f"</g>"
    )

    card_height = y + 10
    svg = f"""<svg xmlns="http://www.w3.org/2000/svg" width="495" height="{card_height}" viewBox="0 0 495 {card_height}">
  <rect x="0.5" y="0.5" rx="4.5" width="494" height="{card_height - 1}" fill="{bg}" stroke="{border}"/>
{icon_svg}
  <text x="58" y="33" fill="{title_color}" font-size="18" font-family="'Segoe UI', Ubuntu, 'Helvetica Neue', Sans-Serif" font-weight="600">Lines of Code Edited (Personal Repos, Excludes my Job)</text>
{rows_svg}</svg>
"""
    return svg


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _display_name(repo: dict, private_counter: dict) -> str:
    """Return a log-safe display name. Redacts private repo names."""
    if repo.get("private"):
        private_counter["n"] += 1
        return f"[private-{private_counter['n']}]"
    return repo["full_name"]


def _process_repo(
    full_name: str,
    repo: dict,
    token: str,
    cache: dict,
    display: str,
    weeks: list[dict] | None,
    new_etag: str | None,
    status: int,
) -> None:
    """Process a single repo's stats result and update the cache."""
    cached_repo = cache["repos"].get(full_name, {})

    if status == 304:
        a = sum(w.get("a", 0) for w in cached_repo.get("weeks", {}).values())
        d = sum(w.get("d", 0) for w in cached_repo.get("weeks", {}).values())
        log.info("  %s — 304 (cache hit) — +%s / -%s", display, f"{a:,}", f"{d:,}")
        return

    if weeks is not None and len(weeks) > 0:
        total_a = sum(w.get("a", 0) for w in weeks)
        total_d = sum(w.get("d", 0) for w in weeks)

        if total_a == 0 and total_d == 0:
            log.info("  %s — all zeroes, 10k+ fallback → cloning", display)
            weeks = clone_and_count(token, full_name)
        else:
            log.info("  %s — 200 — +%s / -%s", display, f"{total_a:,}", f"{total_d:,}")

    elif weeks is not None and len(weeks) == 0:
        log.info("  %s — no commits by %s", display, GITHUB_USER)
        cache["repos"][full_name] = {
            "etag": new_etag,
            "is_private": repo.get("private", False),
            "weeks": {},
            "last_fetched": datetime.now(timezone.utc).isoformat(),
        }
        return

    else:
        # 202 — try clone fallback if no cache
        if cached_repo.get("weeks"):
            log.info("  %s — 202, using cached data", display)
            return
        log.info("  %s — 202, falling back to clone", display)
        weeks = clone_and_count(token, full_name)

    # Update cache
    if weeks is not None:
        weeks_dict = {}
        for w in weeks:
            ts = str(w.get("w", 0))
            weeks_dict[ts] = {"a": w.get("a", 0), "d": w.get("d", 0)}

        cache["repos"][full_name] = {
            "etag": new_etag,
            "is_private": repo.get("private", False),
            "weeks": weeks_dict,
            "last_fetched": datetime.now(timezone.utc).isoformat(),
        }


def main() -> None:
    log.info("=" * 60)
    log.info("Lines of Code Stats Generator")
    log.info("User: %s", GITHUB_USER)
    log.info("=" * 60)

    # Validate tokens
    active_tokens: list[tuple[str, str]] = []
    for label, token in TOKENS.items():
        if token:
            active_tokens.append((label, token))
            log.info("Token configured: %s", label)
        else:
            log.warning("Token NOT configured: %s — skipping", label)

    if not active_tokens:
        log.error("No tokens configured. Set LOC_STATS_TOKEN_PERSONAL and/or LOC_STATS_TOKEN_LOME_AI.")
        sys.exit(1)

    cache = load_cache()
    if "repos" not in cache:
        cache["repos"] = {}

    # Collect all repos across tokens, deduplicate
    all_repos: dict[str, tuple[dict, str, str]] = {}  # full_name -> (repo_dict, token, label)
    for idx, (label, token) in enumerate(active_tokens, 1):
        log.info("")
        log.info("[TOKEN %d/%d] Fetching repos for: %s", idx, len(active_tokens), label)
        try:
            repos = list_user_repos(token, label)
        except requests.HTTPError as e:
            log.error("  Failed to list repos for %s: %s", label, e)
            continue
        log.info("  Found %d repos", len(repos))
        for repo in repos:
            full_name = repo["full_name"]
            if full_name not in all_repos:
                all_repos[full_name] = (repo, token, label)

    log.info("")
    log.info("Total unique repos: %d", len(all_repos))
    log.info("-" * 60)

    # Private repo name counter for redaction
    private_counter = {"n": 0}

    # -----------------------------------------------------------------------
    # Pass 1: Request stats for all repos. Collect 200/304 immediately,
    #         queue 202s for a second pass.
    # -----------------------------------------------------------------------
    log.info("")
    log.info("[PASS 1] Requesting stats for all repos...")
    pending_202: list[tuple[str, dict, str, str, str]] = []  # (full_name, repo, token, display, etag)
    total = len(all_repos)

    for idx, (full_name, (repo, token, token_label)) in enumerate(sorted(all_repos.items()), 1):
        display = _display_name(repo, private_counter)
        cached_repo = cache["repos"].get(full_name, {})
        etag = cached_repo.get("etag")

        weeks, new_etag, status = get_contributor_stats(token, full_name, etag)

        if status == 202:
            log.info("  [%d/%d] %s — 202 (queued for pass 2)", idx, total, display)
            pending_202.append((full_name, repo, token, display, new_etag))
        else:
            _process_repo(full_name, repo, token, cache, f"[{idx}/{total}] {display}", weeks, new_etag, status)

    # -----------------------------------------------------------------------
    # Pass 2: Retry 202 repos after waiting for GitHub to compute stats.
    # -----------------------------------------------------------------------
    if pending_202:
        log.info("")
        log.info("[PASS 2] Waiting %ds for GitHub to compute stats for %d repos...", PASS2_WAIT, len(pending_202))
        time.sleep(PASS2_WAIT)

        for idx, (full_name, repo, token, display, old_etag) in enumerate(pending_202, 1):
            weeks, new_etag, status = get_contributor_stats(token, full_name)

            if status == 202:
                log.info("  [%d/%d] %s — still 202, falling back to clone", idx, len(pending_202), display)
                # Fall through to clone via _process_repo
            _process_repo(full_name, repo, token, cache, f"[{idx}/{len(pending_202)}] {display}", weeks, new_etag, status)

    # Compute aggregated stats
    log.info("")
    log.info("=" * 60)
    log.info("[SUMMARY]")
    stats = compute_timeframes(cache)
    for label in TIME_FRAMES:
        s = stats[label]
        log.info(
            "  %-12s +%-10s / -%-10s = %s lines",
            label + ":",
            f"{s['additions']:,}",
            f"{s['deletions']:,}",
            f"{s['total']:,}",
        )

    # Generate SVG
    svg_content = generate_svg(stats)
    SVG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SVG_PATH, "w") as f:
        f.write(svg_content)
    log.info("")
    log.info("SVG written to %s", SVG_PATH)

    # Save cache
    save_cache(cache)
    log.info("Cache written to %s", CACHE_PATH)
    log.info("Done.")


if __name__ == "__main__":
    main()
