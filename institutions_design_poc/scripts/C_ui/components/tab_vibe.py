"""
VIBE tab component -- student quote, vibe tags, campus mood bars,
day-in-life video placeholder, and social proof.
"""

import random
import streamlit as st

from scripts.C_ui.styles.theme import (
    CORAL_BLAST,
    MINT_FRESH,
    ELECTRIC_PURPLE,
    GOLDEN_HOUR,
)


def _mood_bars(campus_mood: dict) -> str:
    """Return HTML for study / social / chill mood bars."""
    bar_styles = {
        "Study": "coral",
        "Social": "mint",
        "Chill": "purple",
    }
    # Map data keys to display labels
    key_map = {"study": "Study", "social": "Social", "chill": "Chill"}

    html = ""
    for data_key, label in key_map.items():
        value = campus_mood.get(data_key, 0)
        style = bar_styles.get(label, "coral")
        html += f"""
        <div class="mood-bar-container">
            <span class="mood-bar-label">{label}</span>
            <div class="mood-bar-track">
                <div class="mood-bar-fill {style}" style="width: {value}%;"></div>
            </div>
            <span class="mood-bar-value">{value}%</span>
        </div>
        """
    return html


def render_tab_vibe(institution: dict) -> None:
    """Render the VIBE tab contents."""

    inst_name = institution["name"]

    # --- Student quote ---
    quote = institution.get("student_quote", "")
    author = institution.get("student_quote_author", "")
    year = institution.get("student_quote_year", "")
    author_line = f"-- {author}"
    if year:
        author_line += f", {year}"

    st.markdown(
        f"""
        <div class="student-quote">
            <p>{quote}</p>
            <div class="quote-author">{author_line}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Vibe tags ---
    st.markdown("### Vibe Tags")
    vibe_tags = institution.get("vibe_tags", [])
    colour_classes = ["tag-coral", "tag-mint", "tag-purple", "tag-golden", "tag-muted"]
    tags_html = ""
    for i, tag in enumerate(vibe_tags):
        cls = colour_classes[i % len(colour_classes)]
        tags_html += f'<span class="tag-pill {cls}">{tag}</span>'
    st.markdown(tags_html, unsafe_allow_html=True)

    # --- Campus mood bars ---
    st.markdown("### Campus Mood")
    campus_mood = institution.get("campus_mood", {"study": 50, "social": 50, "chill": 50})
    st.markdown(_mood_bars(campus_mood), unsafe_allow_html=True)

    # --- Day-in-life video placeholder ---
    st.markdown("### Day In The Life")
    st.markdown(
        f"""
        <div class="video-placeholder">
            <div class="play-icon">&#9654;</div>
            <div style="font-family: var(--font-heading); font-weight: 700; font-size: 1rem;">
                "Day in My Life: {inst_name}"
            </div>
            <div style="font-size: 0.8rem; margin-top: 0.25rem; color: var(--text-secondary);">
                2:34 &middot; Student-created content
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Social proof ---
    viewers = random.randint(80, 250)
    st.markdown(
        f'<div class="social-proof">'
        f'<span>&#128064;</span> {viewers} students are exploring {inst_name} right now'
        f'</div>',
        unsafe_allow_html=True,
    )
