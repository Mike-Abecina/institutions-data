"""
Institution card component -- hero gradient, tags, and tab container.

Renders a full-width card for a single institution with:
  - Gradient hero section (name, city, tagline)
  - Tag pills
  - VIBE / EATS / LIFE / COST tabs (delegated to tab_* components)
  - SAVE button with session state tracking
"""

import random
import streamlit as st

from scripts.C_ui.styles.theme import GRADIENT_MAP, GRAD_WARM, get_tag_colour
from scripts.C_ui.components.tab_vibe import render_tab_vibe
from scripts.C_ui.components.tab_eats import render_tab_eats
from scripts.C_ui.components.tab_life import render_tab_life
from scripts.C_ui.components.tab_cost import render_tab_cost


def _resolve_gradient(grad_key: str) -> str:
    """Return a CSS gradient string from a key or pass through raw CSS."""
    if grad_key in GRADIENT_MAP:
        return GRADIENT_MAP[grad_key]
    if grad_key.startswith("linear-gradient"):
        return grad_key
    return GRAD_WARM


def render_card(institution: dict) -> None:
    """Render a full institution card with hero, tags, and tabbed content."""

    inst_id = institution["id"]
    gradient = _resolve_gradient(institution.get("hero_gradient", "warm"))

    # --- Hero Section ---
    hero_html = f"""
    <div class="vibe-card animate-in">
        <div class="hero-gradient" style="background: {gradient};">
            <h1>{institution['name']}</h1>
            <div class="hero-location">{institution['city']}, {institution['state']}</div>
            <div class="hero-tagline">"{institution['tagline']}"</div>
        </div>
    </div>
    """
    st.markdown(hero_html, unsafe_allow_html=True)

    # --- Tags ---
    tags_html = ""
    colour_classes = ["tag-coral", "tag-mint", "tag-purple", "tag-golden"]
    for i, tag in enumerate(institution.get("tags", [])):
        cls = colour_classes[i % len(colour_classes)]
        tags_html += f'<span class="tag-pill {cls}">{tag}</span>'

    if tags_html:
        st.markdown(f'<div style="margin: 0.75rem 0 1rem;">{tags_html}</div>', unsafe_allow_html=True)

    # --- Save Button ---
    saved_set = st.session_state.get("saved_institutions", set())
    is_saved = inst_id in saved_set

    col_save, col_proof = st.columns([1, 2])
    with col_save:
        label = "SAVED" if is_saved else "SAVE"
        icon = ":white_check_mark:" if is_saved else ":heart:"
        if st.button(f"{label}", key=f"save_{inst_id}", use_container_width=True):
            if is_saved:
                saved_set.discard(inst_id)
            else:
                saved_set.add(inst_id)
            st.session_state["saved_institutions"] = saved_set
            st.rerun()

    with col_proof:
        viewers = random.randint(50, 200)
        st.markdown(
            f'<div class="social-proof" style="margin-top:0.5rem;">'
            f'<span>&#128293;</span> {viewers} students are looking at this right now'
            f'</div>',
            unsafe_allow_html=True,
        )

    # --- Tabs ---
    tab_vibe, tab_eats, tab_life, tab_cost = st.tabs(["VIBE", "EATS", "LIFE", "COST"])

    with tab_vibe:
        render_tab_vibe(institution)

    with tab_eats:
        render_tab_eats(institution)

    with tab_life:
        render_tab_life(institution)

    with tab_cost:
        render_tab_cost(institution)
