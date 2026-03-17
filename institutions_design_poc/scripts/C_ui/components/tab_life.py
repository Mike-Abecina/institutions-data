"""
LIFE tab component -- day-in-the-life timeline, clubs, transport, safety.
"""

import streamlit as st


def _timeline_html(entries: list[dict]) -> str:
    """Build a vertical timeline from DayInLifeEntry dicts."""
    items = ""
    for entry in entries:
        time = entry.get("time", "")
        emoji = entry.get("emoji", "")
        desc = entry.get("description", entry.get("activity", ""))
        items += f"""
        <div class="timeline-entry">
            <span class="timeline-time">{time}</span>
            <span class="timeline-emoji">{emoji}</span>
            <span class="timeline-desc">{desc}</span>
        </div>
        """
    return f'<div class="timeline">{items}</div>'


def _club_pills(clubs: list[str]) -> str:
    """Return HTML for club tag pills."""
    colour_classes = ["tag-coral", "tag-mint", "tag-purple", "tag-golden", "tag-muted"]
    html = ""
    for i, club in enumerate(clubs):
        cls = colour_classes[i % len(colour_classes)]
        html += f'<span class="tag-pill {cls}">{club}</span>'
    return html


def _info_card(title: str, items: list[str]) -> str:
    """Return HTML for a styled info card with a bulleted list."""
    li_items = "".join(f"<li>{item}</li>" for item in items)
    return f"""
    <div class="info-card">
        <h4>{title}</h4>
        <ul style="padding-left: 0; margin: 0;">{li_items}</ul>
    </div>
    """


def render_tab_life(institution: dict) -> None:
    """Render the LIFE tab contents."""

    # --- Day in the Life ---
    st.markdown("### A Day In Your Life Here")
    day_entries = institution.get("day_in_life", [])
    if day_entries:
        st.markdown(_timeline_html(day_entries), unsafe_allow_html=True)
    else:
        st.info("Day-in-the-life content coming soon.")

    # --- Clubs ---
    st.markdown("### Clubs & Things That Actually Matter")
    clubs = institution.get("clubs", [])
    if clubs:
        st.markdown(_club_pills(clubs), unsafe_allow_html=True)
    else:
        st.info("Club information coming soon.")

    # --- Transport ---
    transport = institution.get("transport", {})
    if transport:
        transport_items = []
        label_map = {
            "bus_to_cbd": "Bus to CBD",
            "airport": "Airport",
            "train": "Train to city",
            "tram": "Tram to city",
            "other": "Other",
        }
        for key, value in transport.items():
            label = label_map.get(key, key.replace("_", " ").title())
            transport_items.append(f"{label}: {value}")
        st.markdown(_info_card("Transport", transport_items), unsafe_allow_html=True)

    # --- Safety & Support ---
    safety = institution.get("safety_support", [])
    if safety:
        st.markdown(_info_card("Safety & Support", safety), unsafe_allow_html=True)
