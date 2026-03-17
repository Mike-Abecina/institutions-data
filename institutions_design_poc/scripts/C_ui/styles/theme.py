"""
VIBE CHECK Design System -- colour constants, gradients, and CSS injection helper.

Palette designed for Gen Z dark-mode-first aesthetic with high-saturation
accents that pop on OLED screens.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Colour constants
# ---------------------------------------------------------------------------
MIDNIGHT = "#0D0D0D"
CORAL_BLAST = "#FF5733"
MINT_FRESH = "#00E5A0"
ELECTRIC_PURPLE = "#7C5CFC"
GOLDEN_HOUR = "#FFD166"
CLOUD_WHITE = "#F5F5F5"

# Slightly muted variants for backgrounds / borders
MIDNIGHT_LIGHT = "#1A1A2E"
MIDNIGHT_MID = "#16213E"
CARD_BG = "#181818"
CARD_BORDER = "#2A2A2A"
TEXT_SECONDARY = "#AAAAAA"

# ---------------------------------------------------------------------------
# Gradient presets (CSS linear-gradient strings)
# ---------------------------------------------------------------------------
GRAD_WARM = f"linear-gradient(135deg, {CORAL_BLAST}, {GOLDEN_HOUR})"
GRAD_COOL = f"linear-gradient(135deg, {ELECTRIC_PURPLE}, {MINT_FRESH})"
GRAD_DARK = f"linear-gradient(135deg, {MIDNIGHT}, {ELECTRIC_PURPLE})"
GRAD_CORAL = f"linear-gradient(135deg, {CORAL_BLAST} 0%, #FF8A65 100%)"
GRAD_MINT = f"linear-gradient(135deg, {MINT_FRESH} 0%, #00B8D4 100%)"
GRAD_PURPLE = f"linear-gradient(135deg, {ELECTRIC_PURPLE} 0%, #B388FF 100%)"
GRAD_GOLDEN = f"linear-gradient(135deg, {GOLDEN_HOUR} 0%, #FFB74D 100%)"

# Mapping that cards can reference by name
GRADIENT_MAP = {
    "warm": GRAD_WARM,
    "cool": GRAD_COOL,
    "dark": GRAD_DARK,
    "coral": GRAD_CORAL,
    "mint": GRAD_MINT,
    "purple": GRAD_PURPLE,
    "golden": GRAD_GOLDEN,
}

# ---------------------------------------------------------------------------
# Tag colour cycling -- gives each pill a unique accent
# ---------------------------------------------------------------------------
TAG_COLOURS = [
    CORAL_BLAST,
    MINT_FRESH,
    ELECTRIC_PURPLE,
    GOLDEN_HOUR,
    "#FF8A65",
    "#00B8D4",
    "#B388FF",
    "#FFB74D",
]


def get_tag_colour(index: int) -> str:
    """Return a tag accent colour, cycling through the palette."""
    return TAG_COLOURS[index % len(TAG_COLOURS)]


# ---------------------------------------------------------------------------
# CSS injection
# ---------------------------------------------------------------------------
def get_custom_css() -> str:
    """Read the custom.css file and return its contents for st.markdown injection."""
    css_path = Path(__file__).parent / "custom.css"
    if css_path.exists():
        return css_path.read_text()
    return ""


def inject_css():
    """Inject the full custom CSS into the Streamlit page via st.markdown."""
    import streamlit as st

    css = get_custom_css()
    if css:
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)
