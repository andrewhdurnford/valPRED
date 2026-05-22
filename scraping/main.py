import argparse

from link_scraper import get_all_tier1_teams, log, update_tier1_matchlinks
from paths import MATCH_LINKS
from stats_scraper import process_matches, read_links, update_tier1 as scrape_tier1


def main():
    parser = argparse.ArgumentParser(description="Run the incremental tier 1 scraper.")
    parser.add_argument(
        "--refresh-teams",
        action="store_true",
        help="Refresh tier 1 team metadata before scraping. Slower and usually not needed.",
    )
    parser.add_argument(
        "--full-rescrape",
        action="store_true",
        help=(
            "Re-process every match in the existing tier1 match-link file "
            "(UPSERT, no delete). Use this to backfill the maps table after a "
            "scraper change. Skips the link-refresh step."
        ),
    )
    args = parser.parse_args()

    log("Starting scraper")
    if args.full_rescrape:
        log("Full rescrape mode: re-processing all known tier 1 match links")
        process_matches(read_links(MATCH_LINKS), replace=False)
        log("Scraper finished")
        return

    if args.refresh_teams:
        log("Step 1/3: refresh tier 1 teams")
        get_all_tier1_teams()
    else:
        log("Step 1/3: skip tier 1 team refresh (use --refresh-teams to run it)")
    log("Step 2/3: refresh tier 1 match links")
    update_tier1_matchlinks()
    log("Step 3/3: scrape tier 1 match stats")
    scrape_tier1()
    log("Scraper finished")


if __name__ == "__main__":
    main()
