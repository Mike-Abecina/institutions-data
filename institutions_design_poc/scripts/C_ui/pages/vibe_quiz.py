"""
Vibe Quiz page -- 3-question personality hook that takes ~15 seconds.

Questions:
  1. WHERE'S YOUR VIBE?  (Beach / City / Country / Campus Town)
  2. WHAT FIRES YOU UP?  (Creating / Discovering / Building / Helping)
  3. WEEKEND ENERGY?     (Bushwalk / Live Music / Markets / Netflix & Study)

On completion, show a brief "algorithm" animation and redirect to the discover feed.
"""

import time
import streamlit as st

from scripts.C_ui.styles.theme import (
    CORAL_BLAST,
    MINT_FRESH,
    ELECTRIC_PURPLE,
    GOLDEN_HOUR,
    GRAD_CORAL,
    GRAD_MINT,
    GRAD_PURPLE,
    GRAD_GOLDEN,
)

# ---------------------------------------------------------------------------
# Quiz data
# ---------------------------------------------------------------------------

QUESTIONS = [
    {
        "title": "WHERE'S YOUR VIBE?",
        "subtitle": "Pick one. Trust your gut.",
        "key": "vibe_location",
        "options": [
            {"value": "beach", "emoji": "\U0001F3D6\uFE0F", "label": "Beach", "sub": "Surf & study", "gradient": GRAD_CORAL},
            {"value": "city", "emoji": "\U0001F307", "label": "City", "sub": "Skyline hustle", "gradient": GRAD_PURPLE},
            {"value": "country", "emoji": "\U0001F333", "label": "Country", "sub": "Space & quiet", "gradient": GRAD_MINT},
            {"value": "campus", "emoji": "\U0001F3DB\uFE0F", "label": "Campus Town", "sub": "Classic quad", "gradient": GRAD_GOLDEN},
        ],
    },
    {
        "title": "WHAT FIRES YOU UP?",
        "subtitle": "What makes you lose track of time?",
        "key": "vibe_energy",
        "options": [
            {"value": "creating", "emoji": "\U0001F3A8", "label": "Creating", "sub": "Art, design, music", "gradient": GRAD_CORAL},
            {"value": "discovering", "emoji": "\U0001F52C", "label": "Discovering", "sub": "Science, research", "gradient": GRAD_PURPLE},
            {"value": "building", "emoji": "\U0001F6E0\uFE0F", "label": "Building", "sub": "Tech, engineering", "gradient": GRAD_MINT},
            {"value": "helping", "emoji": "\U0001F91D", "label": "Helping", "sub": "People, community", "gradient": GRAD_GOLDEN},
        ],
    },
    {
        "title": "WEEKEND ENERGY?",
        "subtitle": "Saturday morning. What's the move?",
        "key": "vibe_weekend",
        "options": [
            {"value": "bushwalk", "emoji": "\U0001F6B6", "label": "Bushwalk", "sub": "Nature recharge", "gradient": GRAD_MINT},
            {"value": "live_music", "emoji": "\U0001F3B5", "label": "Live Music", "sub": "Gigs & festivals", "gradient": GRAD_CORAL},
            {"value": "markets", "emoji": "\U0001F6CD\uFE0F", "label": "Markets", "sub": "Food & finds", "gradient": GRAD_GOLDEN},
            {"value": "netflix", "emoji": "\U0001F4FA", "label": "Netflix & Study", "sub": "Cosy productivity", "gradient": GRAD_PURPLE},
        ],
    },
]


def _render_option_button(option: dict, question_key: str, question_index: int, col_index: int) -> None:
    """Render a styled quiz option as a clickable card using st.button + HTML preview."""
    gradient = option["gradient"]
    emoji = option["emoji"]
    label = option["label"]
    sub = option["sub"]

    # Visual card (non-interactive HTML)
    st.markdown(
        f"""
        <div class="quiz-option" style="background: {gradient}; margin-bottom: 0.5rem;">
            <div class="quiz-emoji">{emoji}</div>
            <div class="quiz-label">{label}</div>
            <div class="quiz-sub">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Actual clickable button
    if st.button(
        label,
        key=f"quiz_{question_key}_{option['value']}",
        use_container_width=True,
    ):
        # Store answer
        if "vibe_answers" not in st.session_state:
            st.session_state["vibe_answers"] = {}
        st.session_state["vibe_answers"][question_key] = option["value"]
        # Advance question
        st.session_state["quiz_question"] = question_index + 1
        st.rerun()


def render_vibe_quiz() -> None:
    """Main entry point -- render the current quiz question or the completion screen."""

    # Initialise state
    if "quiz_question" not in st.session_state:
        st.session_state["quiz_question"] = 0
    if "vibe_answers" not in st.session_state:
        st.session_state["vibe_answers"] = {}

    q_idx = st.session_state["quiz_question"]

    # ----- Completion screen -----
    if q_idx >= len(QUESTIONS):
        _render_completion()
        return

    # ----- Question screen -----
    question = QUESTIONS[q_idx]

    # Centered header
    st.markdown(
        f"""
        <div style="text-align: center; margin: 2rem 0 0.5rem;">
            <h1 style="margin-bottom: 0.25rem;">{question['title']}</h1>
            <p style="color: var(--text-secondary); font-size: 1.05rem;">{question['subtitle']}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 2x2 grid of options
    options = question["options"]
    row1_cols = st.columns(2, gap="medium")
    for ci, opt in enumerate(options[:2]):
        with row1_cols[ci]:
            _render_option_button(opt, question["key"], q_idx, ci)

    row2_cols = st.columns(2, gap="medium")
    for ci, opt in enumerate(options[2:4]):
        with row2_cols[ci]:
            _render_option_button(opt, question["key"], q_idx, ci + 2)

    # Progress bar
    progress = (q_idx + 1) / len(QUESTIONS)
    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)
    st.progress(progress)
    st.markdown(
        f"""
        <div style="text-align: center; font-size: 0.85rem; color: var(--text-secondary);">
            Question {q_idx + 1} of {len(QUESTIONS)}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_completion() -> None:
    """Show the 'algorithm found you' animation, then redirect to discover."""

    st.markdown(
        """
        <div style="text-align: center; margin: 4rem 0 2rem;">
            <h1 style="margin-bottom: 0.5rem;">HERE'S WHAT THE ALGORITHM FOUND FOR YOU</h1>
            <p style="color: var(--text-secondary); font-size: 1.05rem;">
                Matching your vibe to real institutions...
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.spinner("Crunching the vibes..."):
        time.sleep(1.5)

    # Transition to discover page
    st.session_state["page"] = "discover"
    st.rerun()
