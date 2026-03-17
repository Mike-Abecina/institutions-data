"""
Compare page -- side-by-side comparison of saved institutions.

Shows mini hero cards + comparison rows for key metrics.
Highlights the "winner" in each row with mint green.
"""

import streamlit as st

from scripts.C_ui.styles.theme import GRADIENT_MAP, GRAD_WARM, MINT_FRESH


def _resolve_gradient(grad_key: str) -> str:
    if grad_key in GRADIENT_MAP:
        return GRADIENT_MAP[grad_key]
    if grad_key.startswith("linear-gradient"):
        return grad_key
    return GRAD_WARM


def _compare_value(vals: list, higher_is_better: bool = True) -> list[bool]:
    """Return a list of booleans marking the winner(s)."""
    if not vals:
        return []
    # Handle None values
    numeric = []
    for v in vals:
        if v is None:
            numeric.append(None)
        elif isinstance(v, (int, float)):
            numeric.append(v)
        else:
            try:
                numeric.append(float(str(v).replace("$", "").replace(",", "").replace("/week", "").strip()))
            except (ValueError, TypeError):
                numeric.append(None)

    valid = [n for n in numeric if n is not None]
    if not valid:
        return [False] * len(vals)

    if higher_is_better:
        best = max(valid)
    else:
        best = min(valid)

    return [n == best and n is not None for n in numeric]


def _styled_val(val, is_winner: bool) -> str:
    """Return HTML-styled value, highlighted mint if winner."""
    if is_winner:
        return f'<span class="compare-winner">{val}</span>'
    return f'<span>{val}</span>'


def render_compare(institutions: list[dict]) -> None:
    """Main entry point for the compare page."""

    saved_ids = st.session_state.get("saved_institutions", set())

    if len(saved_ids) < 2:
        st.markdown(
            """
            <div style="text-align: center; margin: 4rem 0;">
                <h2>COMPARE: PICK YOUR FIGHTERS</h2>
                <p style="color: var(--text-secondary); font-size: 1.1rem; margin-top: 1rem;">
                    Save at least 2 institutions from the Discover feed to unlock comparison.
                </p>
                <p style="color: var(--text-secondary); font-size: 0.9rem; margin-top: 0.5rem;">
                    Hit the <strong>SAVE</strong> button on any institution card.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    # Filter to saved institutions
    saved = [inst for inst in institutions if inst.get("id") in saved_ids]

    if len(saved) < 2:
        st.warning("Could not find the saved institutions in the data. Try saving again.")
        return

    st.markdown(
        """
        <div style="text-align: center; margin-bottom: 1.5rem;">
            <h2>COMPARE: PICK YOUR FIGHTERS</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Mini hero cards ---
    cols = st.columns(len(saved))
    for ci, inst in enumerate(saved):
        with cols[ci]:
            gradient = _resolve_gradient(inst.get("hero_gradient", "warm"))
            st.markdown(
                f"""
                <div style="background: {gradient}; border-radius: 16px; padding: 1.5rem 1rem;
                            text-align: center; margin-bottom: 1rem;">
                    <div style="font-family: var(--font-heading); font-weight: 800; font-size: 1.2rem;
                                color: white; text-shadow: 0 2px 6px rgba(0,0,0,0.4);">
                        {inst['name']}
                    </div>
                    <div style="font-size: 0.85rem; color: rgba(255,255,255,0.85); margin-top: 0.25rem;">
                        {inst['city']}, {inst['state']}
                    </div>
                    <div style="font-style: italic; font-size: 0.9rem; color: rgba(255,255,255,0.9); margin-top: 0.4rem;">
                        "{inst['tagline']}"
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # --- Comparison rows ---
    def _row(label: str, values: list, higher_is_better: bool = True, fmt=str):
        winners = _compare_value(
            [v if isinstance(v, (int, float)) else v for v in values],
            higher_is_better,
        )
        row_cols = st.columns([1] + [1] * len(saved))
        with row_cols[0]:
            st.markdown(f'<div class="compare-label" style="margin-top: 0.4rem;">{label}</div>', unsafe_allow_html=True)
        for ci, (val, win) in enumerate(zip(values, winners)):
            with row_cols[ci + 1]:
                st.markdown(_styled_val(fmt(val) if val is not None else "--", win), unsafe_allow_html=True)
        # Divider
        st.markdown('<div class="compare-row"></div>', unsafe_allow_html=True)

    # Vibe tags
    row_cols = st.columns([1] + [1] * len(saved))
    with row_cols[0]:
        st.markdown('<div class="compare-label" style="margin-top: 0.4rem;">Vibe</div>', unsafe_allow_html=True)
    for ci, inst in enumerate(saved):
        with row_cols[ci + 1]:
            tags = inst.get("vibe_tags", [])[:3]
            pills = " ".join(f'<span class="tag-pill tag-muted" style="font-size:0.7rem;">{t}</span>' for t in tags)
            st.markdown(pills, unsafe_allow_html=True)
    st.markdown('<div class="compare-row"></div>', unsafe_allow_html=True)

    # Rent / week
    _row(
        "Rent / week",
        [inst.get("cost", {}).get("rent_range_low") for inst in saved],
        higher_is_better=False,
        fmt=lambda v: f"${v}/wk",
    )

    # Walk Score
    _row(
        "Walk Score",
        [inst.get("walk_score") for inst in saved],
        higher_is_better=True,
        fmt=lambda v: f"{v}/100",
    )

    # Cafes nearby
    _row(
        "Cafes (10 min)",
        [inst.get("cafes_10min") for inst in saved],
        higher_is_better=True,
        fmt=lambda v: str(v),
    )

    # Top courses
    row_cols = st.columns([1] + [1] * len(saved))
    with row_cols[0]:
        st.markdown('<div class="compare-label" style="margin-top: 0.4rem;">Top Courses</div>', unsafe_allow_html=True)
    for ci, inst in enumerate(saved):
        with row_cols[ci + 1]:
            courses = inst.get("top_courses", [])[:3]
            st.markdown("<br>".join(courses) if courses else "--", unsafe_allow_html=True)
    st.markdown('<div class="compare-row"></div>', unsafe_allow_html=True)

    # Campus mood -- show dominant mood
    row_cols = st.columns([1] + [1] * len(saved))
    with row_cols[0]:
        st.markdown('<div class="compare-label" style="margin-top: 0.4rem;">Campus Mood</div>', unsafe_allow_html=True)
    mood_dominants = []
    for inst in saved:
        mood = inst.get("campus_mood", {})
        if mood:
            top_mood = max(mood, key=mood.get)
            mood_dominants.append(f"{top_mood.capitalize()} {mood[top_mood]}%")
        else:
            mood_dominants.append("--")
    winners = _compare_value(
        [inst.get("campus_mood", {}).get(max(inst.get("campus_mood", {"x": 0}), key=inst.get("campus_mood", {"x": 0}).get), 0) for inst in saved],
        higher_is_better=True,
    )
    for ci, (val, win) in enumerate(zip(mood_dominants, winners)):
        with row_cols[ci + 1]:
            st.markdown(_styled_val(val, win), unsafe_allow_html=True)
    st.markdown('<div class="compare-row"></div>', unsafe_allow_html=True)

    # Student count
    _row(
        "Students",
        [inst.get("student_count") for inst in saved],
        higher_is_better=True,
        fmt=lambda v: f"~{v:,}" if isinstance(v, int) else str(v),
    )

    # --- Actions ---
    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)
    act_cols = st.columns(3)
    with act_cols[0]:
        if st.button("ADD ANOTHER", key="compare_add", use_container_width=True):
            st.session_state["page"] = "discover"
            st.rerun()
    with act_cols[1]:
        st.button("SAVE COMPARISON", key="compare_save", use_container_width=True)
    with act_cols[2]:
        st.button("SHARE", key="compare_share", use_container_width=True)
