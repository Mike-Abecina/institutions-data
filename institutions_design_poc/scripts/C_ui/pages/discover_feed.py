"""
Discover Feed page -- TikTok-style one-institution-at-a-time card feed.

Institutions are sorted by match score derived from the vibe quiz answers.
Navigation via Previous / Next buttons.
"""

import streamlit as st

from scripts.C_ui.components.institution_card import render_card


def _compute_match_score(institution: dict, answers: dict) -> int:
    """Return a simple match score (0-3) based on quiz answers vs institution fields."""
    score = 0
    if answers.get("vibe_location") == institution.get("vibe_location"):
        score += 1
    if answers.get("vibe_energy") == institution.get("vibe_energy"):
        score += 1
    if answers.get("vibe_weekend") == institution.get("vibe_weekend"):
        score += 1
    return score


def render_discover_feed(institutions: list[dict]) -> None:
    """Main entry point for the discover feed."""

    if not institutions:
        st.warning("No institutions loaded. Check data/fixtures or fallback data.")
        return

    # --- Sort by match score ---
    answers = st.session_state.get("vibe_answers", {})
    if answers:
        scored = [(inst, _compute_match_score(inst, answers)) for inst in institutions]
        scored.sort(key=lambda x: x[1], reverse=True)
        institutions = [inst for inst, _ in scored]

    # --- Current index ---
    if "current_index" not in st.session_state:
        st.session_state["current_index"] = 0

    idx = st.session_state["current_index"]
    total = len(institutions)

    # Clamp
    if idx < 0:
        idx = 0
    if idx >= total:
        idx = total - 1
    st.session_state["current_index"] = idx

    institution = institutions[idx]

    # --- Counter ---
    st.markdown(
        f"""
        <div style="text-align: center; margin-bottom: 0.5rem;">
            <span style="font-family: var(--font-heading); font-weight: 700; font-size: 0.85rem;
                         color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.08em;">
                {idx + 1} / {total}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Match badge (if quiz completed) ---
    if answers:
        score = _compute_match_score(institution, answers)
        pct = int((score / 3) * 100)
        badge_colour = "#00E5A0" if pct >= 66 else ("#FFD166" if pct >= 33 else "#FF5733")
        st.markdown(
            f"""
            <div style="text-align: center; margin-bottom: 1rem;">
                <span style="background: {badge_colour}; color: #0D0D0D; padding: 0.3rem 0.9rem;
                             border-radius: 999px; font-family: var(--font-heading); font-weight: 700;
                             font-size: 0.85rem; text-transform: uppercase;">
                    {pct}% Vibe Match
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --- Render the card ---
    render_card(institution)

    # --- Navigation ---
    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
    nav_cols = st.columns([1, 1, 1])

    with nav_cols[0]:
        if idx > 0:
            if st.button("\u2190  PREVIOUS", key="nav_prev", use_container_width=True):
                st.session_state["current_index"] = idx - 1
                st.rerun()

    with nav_cols[1]:
        st.markdown(
            """
            <div style="text-align: center; font-size: 0.75rem; color: var(--text-secondary);
                        margin-top: 0.6rem;">
                Use buttons to navigate
            </div>
            """,
            unsafe_allow_html=True,
        )

    with nav_cols[2]:
        if idx < total - 1:
            if st.button("NEXT  \u2192", key="nav_next", use_container_width=True):
                st.session_state["current_index"] = idx + 1
                st.rerun()
