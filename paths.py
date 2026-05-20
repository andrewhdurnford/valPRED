from pathlib import Path

ROOT = Path(__file__).resolve().parent

DB              = ROOT / "data" / "valpred.db"

MATCH_LINKS     = ROOT / "scraping" / "tier1_match_links.csv"
NEW_MATCH_LINKS = ROOT / "scraping" / "new_tier1_match_links.csv"

MODELS          = ROOT / "models"
CONFIG          = ROOT / "config.toml"
