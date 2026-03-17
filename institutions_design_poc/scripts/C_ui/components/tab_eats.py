"""
EATS tab component -- walk/transit/bike scores, student favourite venues,
and "vibes nearby" summary metrics.
"""

import streamlit as st


# Emoji map for venue types
_VENUE_EMOJI = {
    "cafe": "\u2615",       # coffee
    "restaurant": "\U0001F374",  # fork & knife
    "bar": "\U0001F37A",    # beer
}


def _venue_card_html(venue: dict) -> str:
    """Return HTML for a single venue mini-card."""
    emoji = _VENUE_EMOJI.get(venue.get("venue_type", ""), "\U0001F37D")
    name = venue.get("name", "Unknown")
    distance = venue.get("distance_walk_min", "?")
    review = venue.get("student_review", "")
    reviewer = venue.get("student_reviewer", "")
    price = venue.get("price_level", "")

    review_html = ""
    if review:
        review_html = f'<div class="venue-quote">&ldquo;{review}&rdquo;</div>'
    reviewer_html = ""
    if reviewer:
        reviewer_html = f'<div class="venue-reviewer">-- {reviewer} &middot; Verified student</div>'
    price_html = ""
    if price:
        price_html = f' &middot; {price}'

    return f"""
    <div class="venue-card">
        <div class="venue-name">{emoji} {name}</div>
        <div class="venue-meta">{distance} min walk{price_html}</div>
        {review_html}
        {reviewer_html}
    </div>
    """


def render_tab_eats(institution: dict) -> None:
    """Render the EATS tab contents."""

    st.markdown("### The Student Food Map")

    # --- Scores row ---
    walk = institution.get("walk_score", 0)
    transit = institution.get("transit_score")
    bike = institution.get("bike_score")

    score_pills = f"""
    <div class="metric-pill-row">
        <div class="metric-pill">
            <span class="metric-value">{walk}</span>
            <span class="metric-label">Walk Score</span>
        </div>
    """
    if transit is not None:
        score_pills += f"""
        <div class="metric-pill">
            <span class="metric-value">{transit}</span>
            <span class="metric-label">Transit Score</span>
        </div>
        """
    if bike is not None:
        score_pills += f"""
        <div class="metric-pill">
            <span class="metric-value">{bike}</span>
            <span class="metric-label">Bike Score</span>
        </div>
        """
    score_pills += "</div>"
    st.markdown(score_pills, unsafe_allow_html=True)

    # --- Student Favourites ---
    venues = institution.get("nearby_venues", [])
    if venues:
        st.markdown("### Student Favourites")

        # Render 2 per row
        for row_start in range(0, len(venues), 2):
            cols = st.columns(2)
            for col_idx, venue in enumerate(venues[row_start : row_start + 2]):
                with cols[col_idx]:
                    st.markdown(_venue_card_html(venue), unsafe_allow_html=True)

    # --- Vibes Nearby ---
    st.markdown("### Vibes Nearby")
    cafes = institution.get("cafes_10min", 0)
    bars = institution.get("bars_10min", 0)
    beaches = institution.get("beaches_15min")

    vibes_html = f"""
    <div class="metric-pill-row">
        <div class="metric-pill">
            <span class="metric-value">{cafes}</span>
            <span class="metric-label">Cafes (10 min)</span>
        </div>
        <div class="metric-pill">
            <span class="metric-value">{bars}</span>
            <span class="metric-label">Bars (10 min)</span>
        </div>
    """
    if beaches is not None:
        vibes_html += f"""
        <div class="metric-pill">
            <span class="metric-value">{beaches}</span>
            <span class="metric-label">Beaches (15 min)</span>
        </div>
        """
    vibes_html += "</div>"
    st.markdown(vibes_html, unsafe_allow_html=True)
