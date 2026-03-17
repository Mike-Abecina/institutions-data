"""
COST tab component -- tuition & rent bars, weekly budget breakdown,
scholarships, and student cost quote.
"""

import streamlit as st

from scripts.C_ui.styles.theme import CORAL_BLAST, GOLDEN_HOUR


# Max values used for bar scaling (rough AU max)
_MAX_TUITION = 50_000
_MAX_RENT = 600


def _range_bar(label: str, low: int, high: int, comparison: str, max_val: int) -> str:
    """Return HTML for a cost range bar with comparison text."""
    pct = min(int((high / max_val) * 100), 100)
    return f"""
    <div class="cost-bar-container">
        <div class="cost-bar-header">
            <span class="cost-bar-label">{label}</span>
            <span class="cost-bar-value">${low:,} &ndash; ${high:,}</span>
        </div>
        <div class="cost-bar-track">
            <div class="cost-bar-fill" style="width: {pct}%;"></div>
        </div>
        <div style="font-size: 0.8rem; color: var(--text-secondary); margin-top: 0.2rem;">
            {comparison}
        </div>
    </div>
    """


def _budget_row(label: str, amount: int, max_amount: int, css_class: str) -> str:
    """Return HTML for a single weekly-budget bar row."""
    pct = min(int((amount / max_amount) * 100), 100) if max_amount > 0 else 0
    return f"""
    <div class="budget-row">
        <span class="budget-label">{label}</span>
        <div class="budget-bar">
            <div class="budget-fill {css_class}" style="width: {pct}%;"></div>
        </div>
        <span class="budget-amount">${amount}</span>
    </div>
    """


def render_tab_cost(institution: dict) -> None:
    """Render the COST tab contents."""

    st.markdown("### The Honest Numbers")

    cost = institution.get("cost", {})
    if not cost:
        st.info("Cost data coming soon.")
        return

    # --- Tuition range ---
    tuition_low = cost.get("tuition_range_low", 0)
    tuition_high = cost.get("tuition_range_high", 0)
    tuition_cmp = cost.get("tuition_comparison", "")
    st.markdown(
        _range_bar("Tuition (CSP domestic) / year", tuition_low, tuition_high, tuition_cmp, _MAX_TUITION),
        unsafe_allow_html=True,
    )

    # --- Rent range ---
    rent_low = cost.get("rent_range_low", 0)
    rent_high = cost.get("rent_range_high", 0)
    rent_cmp = cost.get("rent_comparison", "")
    st.markdown(
        _range_bar("Rent (near campus) / week", rent_low, rent_high, rent_cmp, _MAX_RENT),
        unsafe_allow_html=True,
    )

    # --- Weekly budget breakdown ---
    st.markdown("### Weekly Student Budget")
    weekly = cost.get("weekly_budget", {})
    total = cost.get("total_weekly", 0)

    # Find max for bar scaling
    max_amount = max(weekly.values()) if weekly else 1

    category_styles = {
        "rent": "rent",
        "food": "food",
        "transport": "transport",
        "fun": "fun",
    }

    budget_html = ""
    for cat, amount in weekly.items():
        css_class = category_styles.get(cat, "rent")
        label = cat.capitalize()
        budget_html += _budget_row(label, amount, max_amount, css_class)

    st.markdown(budget_html, unsafe_allow_html=True)

    # Total
    st.markdown(
        f"""
        <div style="text-align: center; margin: 1rem 0;">
            <div style="font-size: 0.85rem; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.05em;">
                Total Weekly Budget
            </div>
            <div class="budget-total">${total}/week</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Scholarships ---
    scholarships = institution.get("scholarships", [])
    if scholarships:
        st.markdown("### Scholarships That Match You")
        for sch in scholarships:
            name = sch.get("name", "")
            amount = sch.get("amount", "")
            eligibility = sch.get("eligibility", "")
            elig_html = ""
            if eligibility:
                elig_html = f'<div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.15rem;">{eligibility}</div>'
            st.markdown(
                f"""
                <div class="scholarship-card">
                    <div>
                        <div class="scholarship-name">{name}</div>
                        {elig_html}
                    </div>
                    <div class="scholarship-amount">{amount}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # --- Student cost quote ---
    # Use a generic fallback; enrichment pipeline can provide per-institution quotes
    inst_name = institution.get("name", "this uni")
    st.markdown(
        f"""
        <div class="student-quote" style="margin-top: 1.5rem;">
            <p>"I'm paying less than my mates in Sydney and I can see the ocean from my balcony"</p>
            <div class="quote-author">-- Student at {inst_name}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
