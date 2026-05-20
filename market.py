import math
import tomllib

from paths import CONFIG

DEFAULT_HOUSE_VIG = 0.07


def load_house_vig():
    try:
        with open(CONFIG, "rb") as config_file:
            return float(tomllib.load(config_file).get("betting", {}).get("house_vig", DEFAULT_HOUSE_VIG))
    except (OSError, tomllib.TOMLDecodeError, TypeError, ValueError):
        return DEFAULT_HOUSE_VIG


HOUSE_VIG = load_house_vig()
MARKET_OVERROUND = 1 + HOUSE_VIG


def vig_opposite_probability(probability, vig=HOUSE_VIG):
    """Infer the other side's implied probability for a two-outcome market."""
    if probability is None:
        return None
    probability = float(probability)
    if math.isnan(probability):
        return None

    opposite = 1 + vig - probability
    if not 0 < opposite < 1:
        return None
    return opposite
