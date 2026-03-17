"""
Generate a Confluence-ready markdown report of vibe + student metrics.

For each metric: histogram (PNG) + NSW top 10 / bottom 10 table.

Output:
  reports/output/vibe_report.md
  reports/output/charts/*.png

Usage:
    .venv/bin/python reports/vibe_report.py
"""

from pathlib import Path
from datetime import date
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

GEO_OUT   = Path(__file__).parent.parent / "geo_mapping" / "output"
INPUT     = GEO_OUT / "institutions_vibe_metrics.csv"
INPUT_STU = GEO_OUT / "institutions_student_metrics.csv"
INPUT_POW = GEO_OUT / "institutions_pow_metrics.csv"
INPUT_MEME = GEO_OUT / "institutions_meme_metrics.csv"
OUT_DIR   = Path(__file__).parent / "output"
CHART_DIR = OUT_DIR / "charts"
MD_FILE   = OUT_DIR / "vibe_report.md"

METRICS = [
    ("car_jail_score",       "Car Jail Score",           "% of dwellings with zero cars. Higher = car-free lifestyle is realistic.",
     "G34 → `Num_MVs_per_dweling_0_MVs` ÷ `Total_dwelings`",
     "✅ **High:** You can live here without a car — no rego, no parking bills, no Sunday afternoon panic when something rattles. Transit and your legs are genuinely enough.\n❌ **Low:** A car is non-negotiable. Factor insurance, rego and petrol on top of rent before you sign anything."),

    ("car_free_commute_pct", "Car-Free Commute",         "% of workers who don't use a private car to get to work.",
     "G62 → (`Tot_P` − `Car_as_driver_P` − `Car_as_passenger_P` − `Truck_P`) ÷ `Tot_P`",
     "✅ **High:** Real public transport or walkable commutes. You're not stuck depending on a car you can barely afford to run.\n❌ **Low:** Everyone drives. If you don't own one, getting to work or placement is genuinely painful."),

    ("wfh_pct",              "WFH Culture Index",        "% of workers who worked from home. High = daytime café culture, flexibility.",
     "G62 → `Worked_home_P` ÷ `Tot_P`",
     "✅ **High:** Cafés have daytime crowds, co-working culture is normal, and flexible work is part of the local fabric — likely to carry into your first job here.\n❌ **Low:** The suburb empties out 8–5. If you're studying from home, you'll feel it."),

    ("pedal_path_pct",       "Pedal & Path Score",       "% cycling or walking to work. High = flat, safe, bikeable streets.",
     "G62 → (`One_method_Bicycle_P` + `One_method_Walked_only_P`) ÷ `Tot_P`",
     "✅ **High:** Flat, connected, safe enough to bike or walk daily. Skip the gym membership, save on transport, arrive in a better mood.\n❌ **Low:** Not built for bikes or feet. You need wheels — or a very long playlist for the commute."),

    ("night_economy_pct",    "Night Shift Neighbours",   "% of residents employed in hospitality + arts/rec. These are the people who live here — not a measure of local venues.",
     "G54A+B → (`Accom_food_Tot` + `Art_recn_Tot`) M+F ÷ sum of all industry `_Tot` M+F",
     "✅ **High:** Your neighbours work in hospitality and creative industries — sign of a young, social, culturally active resident community.\n❌ **Low:** The local workforce is dominated by 9-to-5ers. The energy of who lives here skews corporate or suburban."),

    ("knowledge_worker_pct", "Professional Neighbours",  "% of residents employed in professional, education, and health jobs. Reflects who lives here, not what's nearby.",
     "G54A+B → (`Pro_scien_tec_Tot` + `Educ_trng_Tot` + `HlthCare_SocAs_Tot`) M+F ÷ sum of all industry `_Tot` M+F",
     "✅ **High:** Educated, career-focused residents — good for informal networking and a neighbourhood that invests in itself. Can push rents up though.\n❌ **Low:** Fewer professionals in the mix. May mean less networking pressure — or fewer people to connect with in your field."),

    ("student_bubble_pct",   "Student Bubble Density",   "% of population attending uni or TAFE aged 15–24.",
     "G15 → (`Tert_Uni_*_15_24_P` + `Tert_Voc_*_15_24_P`) ÷ `Tot_P`",
     "✅ **High:** Genuine student area — cheap food options, campus events, people in the same life stage. You won't feel out of place studying at midnight.\n❌ **Low:** Students are a small minority here. The suburb doesn't run on the academic calendar and won't particularly cater to you."),

    ("renter_republic_pct",  "Renter Republic Score",    "% of dwellings being rented. High = share-house culture, short leases.",
     "G37 → sum of `R_*_Total` cols ÷ `Total_Total`",
     "✅ **High:** Renting is the norm. Landlords are used to students and young tenants, listings are plentiful, and share-house culture is embedded.\n❌ **Low:** Owner-occupier territory. Rental stock is thin, landlords may prefer long-term stable tenants, and the culture assumes you're buying eventually."),

    ("vertical_city_pct",    "Vertical City Score",      "% of dwellings that are flats or apartments. High = dense urban living.",
     "G36 → sum of `Flt_apart_*_Dwgs` cols ÷ `OPDs_Tot_OPDs_Dwellings`",
     "✅ **High:** Dense apartment living — more stock to choose from, often closer to transit and amenities, lower per-person cost in a share.\n❌ **Low:** House-dominated suburb. Rent per person in a share house may be higher, and everything is more spread out."),

    ("housing_stress_ratio", "Housing Stress Ratio",     "Annual rent as % of personal income. Above 30% = financially tight. ⚠️ High is bad.",
     "G02 → (`Median_rent_weekly` × 52) ÷ (`Median_tot_prsnl_inc_weekly` × 52) × 100",
     "✅ **Low:** Rent is manageable relative to local incomes. Financial breathing room for going out, saving, or surviving on a part-time wage while studying.\n❌ **High:** Rent swallows a disproportionate share of income here. You will feel it every fortnight — especially on a student or entry-level wage."),

    ("fresh_energy_pct",     "Fresh Energy Score",       "% who moved to this area in the last 12 months. High = dynamic social scene.",
     "G44 → `Difnt_Usl_add_1_yr_ago_Tot_P` ÷ `Tot_P`",
     "✅ **High:** Lots of newcomers — easy to meet people, social networks are forming rather than closed. You won't be the only one figuring out where the good coffee is.\n❌ **Low:** Settled, established community. Social circles are long-standing and harder to break into. Could be stable and warm — or just insular."),

    ("community_glue_pct",   "Community Glue Score",     "% of 15+ year olds doing voluntary work. High = strong local fabric.",
     "G23 → `P_Tot_Volunteer` ÷ `P_Tot_Tot`",
     "✅ **High:** People here look out for each other. Strong clubs, events, and local networks. Easy to get involved and meet non-uni people.\n❌ **Low:** More anonymous, transient living. Less community infrastructure — can feel isolating if you don't have an existing social base."),

    ("global_mix_score",     "Global Mix Score",         "Avg of overseas-born % and non-English spoken at home %. High = diverse.",
     "G02 (`overseas_born_pct`) + G13A (sum `MOL_*_Tot` ÷ `MSEO_Tot`) → average of the two",
     "✅ **High:** Multicultural food, events, languages, and perspectives. Diverse friend groups, international students will feel at home, and the food options are usually excellent.\n❌ **Low:** Less cultural diversity. More homogeneous — could feel out of place if you're from elsewhere, and the food scene will reflect it."),
]


def load_sa2(csv_path):
    df = pd.read_csv(csv_path, dtype={"sa2_code": str})
    return df.drop_duplicates(subset=["sa2_code"]).reset_index(drop=True)


def make_histogram(sa2_df, col, title):
    norm_col = col + "_norm"
    if norm_col not in sa2_df.columns:
        return None
    all_vals = sa2_df[norm_col].dropna()
    nsw_vals = sa2_df[sa2_df["state"] == "NSW"][norm_col].dropna()

    fig, ax = plt.subplots(figsize=(8, 3.2), facecolor="#0F172A")
    ax.set_facecolor("#1E293B")
    bins = 40
    ax.hist(all_vals, bins=bins, color="#CBD5E1", alpha=0.6, label=f"All SA2s  n={len(all_vals):,}")
    ax.hist(nsw_vals, bins=bins, color="#6366F1", alpha=0.85, label=f"NSW  n={len(nsw_vals):,}")
    ax.axvline(all_vals.mean(), color="#F59E0B", lw=1.5, ls="--", label=f"AUS avg {all_vals.mean():.1f}")
    ax.axvline(nsw_vals.mean(), color="#34D399", lw=1.5, ls="--", label=f"NSW avg {nsw_vals.mean():.1f}")
    ax.set_title(title, color="white", fontsize=11, fontweight="bold")
    ax.set_xlabel("Score (0–100, normalised)", color="#94A3B8", fontsize=8)
    ax.set_ylabel("SA2 count", color="#94A3B8", fontsize=8)
    ax.tick_params(colors="#94A3B8", labelsize=8)
    for spine in ax.spines.values():
        spine.set_edgecolor("#334155")
    ax.legend(fontsize=7.5, framealpha=0.3, labelcolor="white",
              facecolor="#1E293B", edgecolor="#334155")
    fig.tight_layout()
    path = CHART_DIR / f"{col}.png"
    fig.savefig(path, dpi=120, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return path.name


def nsw_table(sa2_df, col, n=10):
    norm_col = col + "_norm"
    nsw = sa2_df[sa2_df["state"] == "NSW"][["sa2_name", "lga_name", col, norm_col]].dropna(subset=[norm_col])
    top = nsw.nlargest(n, norm_col)
    bot = nsw.nsmallest(n, norm_col)

    def md_table(df, label):
        rows = [f"**{label}**", "| # | SA2 | LGA | Score |", "|---|---|---|---|"]
        for i, (_, r) in enumerate(df.iterrows(), 1):
            rows.append(f"| {i} | {r['sa2_name']} | {r['lga_name']} | {r[norm_col]:.1f} |")
        return "\n".join(rows)

    return md_table(top, "Top 10 NSW"), md_table(bot, "Bottom 10 NSW")


POW_METRICS = [
    ("social_scene_score",      "Social Scene Score",    "Food + Entertainment workers combined. If people work here serving food and running venues — the scene is real, not just on Yelp.",
     "W09A `AcFd_Tot_P` + W09B `ArtsR_Tot_P` ÷ total POW workers (all industries W09A+B)",
     "✅ **High:** Real venues and food options within walking distance — confirmed by the people actually turning up to staff them.\n❌ **Low:** Nothing much happening here at night or on weekends. Every outing requires planning a trip elsewhere."),

    ("food_scene_pct",          "Food & Drink Scene",    "Food & hospitality workers as a share of ALL workers commuting into this area. ⚠️ Major employment hubs (e.g. Parramatta, CBD) can score lower not because they lack restaurants, but because a large corporate/government workforce dilutes the ratio — even if thousands of hospitality workers show up daily.",
     "W09A → `AcFd_Tot_P` ÷ total POW workers (all industries W09A+B)",
     "✅ **High:** Food and hospitality are a significant share of what this area actually does — real cafés, restaurants and bars with staff to prove it.\n❌ **Low:** Either genuinely thin on food options, or a large corporate/government workforce is diluting the ratio. Worth checking on foot before writing it off."),

    ("entertainment_pct",       "Entertainment Quarter", "Arts & Recreation workers signal venues, theatres, studios and events that actually exist here — not just nearby.",
     "W09B → `ArtsR_Tot_P` ÷ total POW workers (all industries W09A+B)",
     "✅ **High:** Theatres, live music, gyms, galleries and studios are physically in the area — proven by the workforce showing up to run them.\n❌ **Low:** Entertainment infrastructure doesn't exist here. Plan to travel for anything that's not Netflix."),

    ("healthcare_access_pct",   "Healthcare Access",     "Clinics, hospitals, specialists and allied health measured by workers who commute here to do that work. High = you can actually see a doctor.",
     "W09B → `HC_SA_Tot_P` ÷ total POW workers (all industries W09A+B)",
     "✅ **High:** GPs, bulk-billing clinics, allied health and specialists are actually here. Getting a Medicare rebate doesn't require a bus trip.\n❌ **Low:** Limited local health services. When you're sick or injured, add travel time to the problem."),

    ("education_hub_pct",       "Education Hub",         "Schools, unis, tutoring centres measured by education workers showing up here daily. High = learning infrastructure is real.",
     "W09B → `EdTrn_Tot_P` ÷ total POW workers (all industries W09A+B)",
     "✅ **High:** Real campus infrastructure, libraries and learning spaces are nearby — confirmed by the educators actually commuting in.\n❌ **Low:** No real education infrastructure here. You're commuting to campus from a suburb that has nothing for you in between."),

    ("retail_density_pct",      "Shops & Markets",       "Retail workers signal everyday convenience — grocery stores, markets, pharmacies, boutiques. High = you can run errands without leaving the area.",
     "W09A → `RetT_Tot_P` ÷ total POW workers (all industries W09A+B)",
     "✅ **High:** Groceries, pharmacies, and everyday shops exist locally. Life is frictionless — you can run all your errands on foot.\n❌ **Low:** Stock the fridge before you move in. Every errand is a car trip or a long walk to nowhere."),

    ("civic_services_pct",      "Civic Infrastructure",  "Public Admin & Safety workers — councils, courts, emergency services. High = this is a real functioning neighbourhood, not just bedrooms.",
     "W09B → `PubAS_Tot_P` ÷ total POW workers (all industries W09A+B)",
     "✅ **High:** Real services exist here — Centrelink, council offices, emergency services. The boring stuff works when you need it.\n❌ **Low:** Bedroom suburb. Fine until something goes wrong and you realise there's nothing locally to help you sort it."),

    ("knowledge_hub_pct",       "Knowledge Economy Hub", "Professional/Scientific/Tech + Finance workers flowing IN. High = high-value employers are here. Great for placement, networking, internships.",
     "W09B → (`ProSTS_Tot_P` + `FinIns_Tot_P`) ÷ total POW workers (all industries W09A+B)",
     "✅ **High:** High-value employers are physically here. Internships, grad jobs, and industry connections are within commuting reach — or even walking distance.\n❌ **Low:** Professionals commute away from here to work. Fewer local career opportunities and less chance of running into people in your field."),

    ("job_gravity_ratio",       "Job Gravity",           "More jobs exist here than workers who live here. Score > 50 = area pulls people in. < 50 = people leave every morning to go elsewhere.",
     "W01A (sum M_+F_ `_Tot` cols = total POW workers) ÷ G43 `lfs_Tot_LF_P` → log₂ scaled to 0–100",
     "✅ **High:** More jobs exist here than residents. The area attracts workers — bustling by day, more opportunity to pick up casual shifts or network with people in your field.\n❌ **Low:** Everyone leaves every morning. Bedroom suburb that only comes alive on weekends — and even then, maybe not."),
]

STUDENT_METRICS = [
    ("qualification_density",  "Qualification Density",    "% of residents (15+) with a bachelor's degree or higher. High = educated neighbourhood.",
     "G49B → (`P_PGrad_Deg_Total` + `P_GradDip_and_GradCert_Total` + `P_BachDeg_Total`) ÷ `P_Tot_Total`",
     "✅ **High:** Educated neighbourhood — good local amenities, coffee-shop conversations with substance, and people who take their careers seriously.\n❌ **Low:** Fewer locals with university backgrounds. Can feel intellectually quieter — but also less competitive and more affordable."),

    ("grad_capture_rate",      "Grad Capture Rate",        "% of 25–34 year olds who hold a post-school qualification. High = area retains graduates.",
     "G49B → (`P_BachDeg_25_34` + `P_GradDip_and_GradCert_25_34` + `P_AdvDip_and_Dip_25_34` + `P_Cert_III_IV_25_34` + `P_Cert_I_II_25_34`) ÷ `P_Tot_25_34`",
     "✅ **High:** People who graduate here tend to stay. Sign of a city or region with real jobs, real lifestyle, and a reason to stick around after commencement.\n❌ **Low:** Graduates leave. The area is a launchpad, not a destination — use it to get qualified, then expect to move on."),

    ("professional_job_pct",   "Professional Job Density", "% of employed workers in manager or professional roles. High = quality jobs nearby.",
     "G60B → (`P_Tot_Managers` + `P_Tot_Professionals`) ÷ `P_Tot_Tot`",
     "✅ **High:** Career-track jobs exist here. You don't have to commute to the CBD to find something in your field after graduation.\n❌ **Low:** Fewer professional roles in the local job market. A graduate role will likely require a significant commute."),

    ("stem_field_pct",         "STEM Field Concentration", "% of qualified residents whose field was STEM. High = tech and science ecosystem.",
     "G50A → M+F (`NatPhyl_Scn` + `InfoTech` + `Eng_RelTec` + `ArchtBldng`) `_Tot` ÷ sum all field `_Tot` M+F",
     "✅ **High:** Tech and science cluster — your neighbours include engineers, developers and researchers. Networking, internships, and industry events are more accessible.\n❌ **Low:** Not a STEM cluster. Peers in technical fields are scattered elsewhere — relevant if your degree is engineering, computing, or science."),

    ("income_growth_signal",   "Income Growth Signal",     "% of 15+ population earning $1,500+/week. High = strong earning potential in the area.",
     "G17A → M+F (`1500_1749` + `1750_1999` + `2000_2999` + `3000_3499` + `3500_more`) ÷ `M_Tot_Tot` + `F_Tot_Tot`",
     "✅ **High:** The job market here rewards skills and experience. Strong earning potential once you're qualified and working full-time.\n❌ **Low:** Fewer high-income earners locally. Could reflect a casualised or low-wage job market — worth knowing before you plan your post-grad finances."),

    ("employment_rate",        "Employment Rate",          "Employment-to-population ratio for 15+ year olds. High = jobs are plentiful.",
     "G43 → `Percnt_Employment_to_populn_P` (pre-computed ratio, used directly)",
     "✅ **High:** Jobs are easy to find here — both casual work while you're studying and full-time roles after graduation.\n❌ **Low:** Tighter local job market. Harder to pick up shifts or land that first role without competing hard or commuting further."),

    ("mortgage_stress_pct",    "Mortgage Stress",          "% of mortgaged dwellings paying $3,000+/month. High = expensive to buy here. ⚠️ High is bad.",
     "G38 → (`M_3000_3999_Tot` + `M_4000_over_Tot`) ÷ `Tot_Tot`",
     "✅ **Low:** Housing is relatively affordable to buy here. The property ladder exists — though you're probably still renting for now.\n❌ **High:** Extremely expensive to own property. The deposit required is enormous. Long-term renting is likely your reality here — budget accordingly."),
]

# 🎓 Student / Early Life
# 🧑‍💻 Early Career
# 🏠 Housing & Independence
# 💕 Relationships & Social
# 🧠 Funny / Viral
MEME_METRICS = [
    # 🎓 Student / Early Life
    ("uni_town_index",           "Uni Town Index",             "Full-time tertiary students aged 15–24 as % of all enrolled students. High = genuine university town, not just adult ed.",
     "G15 → (`Tert_Uni_oth_h_edu_Ft_15_24_P` + `Tert_Voc_edu_Ft_15_24_P`) ÷ `Tot_P`",
     "✅ **High:** This place IS a uni town — student discounts are real, campus life is active, and everything bends around the academic calendar.\n❌ **Low:** Students are a small minority here. The suburb doesn't know or care about O-Week."),

    ("ramen_economy_score",      "Ramen Economy Score",        "Composite: student density + renters + low-income youth. High = peak broke-but-happy energy.",
     "Composite → mean(`student_bubble_pct_norm`, `renter_republic_pct_norm`, `low_income_youth_pct_norm`)",
     "✅ **High:** Classic student-poverty energy — cheap eats, flat shares everywhere, and solidarity in being collectively broke.\n❌ **Low:** More expensive and professionally oriented. Your budget constraints will feel out of step with the neighbourhood."),

    ("sharehouse_capital",       "Sharehouse Capital",         "Group household members aged 15–34 as % of 15–34 population. High = rotating housemates, mystery leftovers, and bathroom rosters.",
     "G27B (`P_GrpH_Mem_15_24` + `P_GrpH_Mem_25_34`) ÷ G04A (`Age_yr_15_19_P` + `Age_yr_20_24_P` + `Age_yr_25_29_P` + `Age_yr_30_34_P`)",
     "✅ **High:** Share houses are the norm. Finding housemates is easy, landlords expect it, and the culture fully supports living with strangers.\n❌ **Low:** Most people live with family or as couples. Share house listings are sparse — you'll fight for every room on Flatmates."),

    ("all_nighter_index",        "All-Nighter Index",          "Composite: student density + night economy. High = somewhere between study mode and 3am kebab runs.",
     "Composite → mean(`student_bubble_pct_norm`, `night_economy_pct_norm`)",
     "✅ **High:** The area has both the student intensity and the late-night options to sustain a proper all-nighter — study session or social, your call.\n❌ **Low:** Quiet suburb. Lights go out early. Nobody is awake at 2am and there's nowhere open if they were."),

    # 🧑‍💻 Early Career
    ("first_job_energy",         "First Job Energy",           "Employment rate (%) of 20–24 year olds in the labour force. High = young people here are actually finding work — not just vibing.",
     "G46A → (`M_Tot_Emp_20_24` + `F_Tot_Emp_20_24`) ÷ (`M_Tot_LF_20_24` + `F_Tot_Emp_20_24` + `F_Tot_Unemp_20_24`)",
     "✅ **High:** 20–24 year olds are actually employed here. The local job market is working for people your age — casual shifts and grad roles alike.\n❌ **Low:** Youth unemployment is higher here. Competition for entry-level roles is real, and finding casual work while studying is genuinely hard."),

    ("promotion_pipeline",       "Promotion Pipeline",         "Managers + professionals aged 25–34 as % of all 25–34 employed. High = fast-track territory — people here are already running things.",
     "G60B → (`P25_34_Managers` + `P25_34_Professionals`) ÷ `P25_34_Tot`",
     "✅ **High:** People in their late 20s and early 30s are already in management and professional roles here. A signal that the career trajectory is steep and achievable.\n❌ **Low:** The local workforce skews toward trades, admin, or service roles. Career progression will likely require commuting further afield."),

    ("startup_dreamer_density",  "Startup Dreamer Density",    "Tech, creative and science industry workers aged 20–34 as % of all employed 20–34. High = startup ecosystem — people here are disrupting things.",
     "G54A+B → M+F (`Info_media_teleco` + `Art_recn` + `Pro_scien_tec`) aged 20–34 ÷ sum all M+F industries aged 20–34",
     "✅ **High:** Your peer group includes young tech, creative and science workers. Industry events, side projects, and informal mentoring are part of the local fabric.\n❌ **Low:** Not a startup or creative hub. Your industry peers are mostly elsewhere — the area's young workforce is in other sectors."),

    ("side_hustle_generation",   "Side Hustle Generation",     "Part-time workers aged 20–34 as % of all employed 20–34. High = gig economy hub — freelancers, baristas with a podcast, and UX consultants.",
     "G46A → M+F (`Emp_PartT_20_24` + `Emp_PartT_25_34`) ÷ M+F (`Tot_Emp_20_24` + `Tot_Emp_25_34`)",
     "✅ **High:** Part-time and flexible work is normal for young workers here. Great for studying — plenty of casual jobs and a culture that doesn't judge you for it.\n❌ **Low:** Fewer casual options. Full-time or nothing seems to be the expectation — harder to balance around a study schedule."),

    # 🏠 Housing & Independence
    ("bank_of_mum_and_dad",      "Bank of Mum & Dad",          "25–34 year olds with no dependent children as % of 25–34 population. High = still unburdened by school fees — or maybe financially cushioned by parental generosity.",
     "G27B `P_NDpChl_25_34` ÷ G04A (`Age_yr_25_29_P` + `Age_yr_30_34_P`)",
     "✅ **High:** Lots of 25–34 year olds without dependents — plenty of people in the same life stage, free to socialise without the school-pickup constraint.\n❌ **Low:** Most 25–34 year olds here already have children. The area has shifted into family mode — fewer people in your life stage."),

    ("peter_pan_index",          "Peter Pan Index",            "Compound: NDpChl rate × 30–34 share of 25–34 cohort. High = lots of 30-somethings who still have no kids. Growing up is optional.",
     "G27B + G04A → (`P_NDpChl_25_34` ÷ pop_25–34) × (`Age_yr_30_34_P` ÷ pop_25–34) × 100",
     "✅ **High:** People in their 30s here are still kid-free and socially active. The area won't age out from under you as quickly as you'd expect.\n❌ **Low:** By 30 here, most people have moved into family mode. The social scene transitions fast — great if you want that, isolating if you don't."),

    ("adulting_score",           "Adulting Score",             "Lone persons + partnered (no kids) aged 25–34 as % of 25–34 population. High = independent adults making rent, owning plants, and meal-prepping.",
     "G27B → (`P_LonePsn_25_34` + `P_Ptn_in_RM_25_34` + `P_Ptn_in_DFM_25_34`) ÷ G04A pop_25–34",
     "✅ **High:** Independent living is the norm for 25–34 year olds here — living alone or with a partner, not with parents. You'll fit right in.\n❌ **Low:** Many in this age group still live in family households. May reflect high costs forcing people home, or cultural norms around independent living."),

    ("rent_forever_index",       "Rent Forever Index",         "Composite: renters (60%) + student density (40%). High = nobody here is buying — renting is the vibe, the lifestyle, and the 10-year plan.",
     "Composite → 0.6 × `renter_republic_pct_norm` + 0.4 × `student_bubble_pct_norm`",
     "✅ **High:** Renting is the default lifestyle here — not a temporary phase. Landlords are used to young tenants and the culture supports it.\n❌ **Low:** The area assumes you're on a path to ownership. Renters are in the minority and the culture can feel designed for someone else's life stage."),

    # 💕 Relationships & Social
    ("singles_scene",            "Singles Scene",              "Never-married 20–34 year olds as % of all 20–34. High = dating-app active zone — lots of first dates and situationships.",
     "G05 → (`P_20_24_yr_Never_married` + `P_25_34_yr_Never_married`) ÷ (`P_20_24_yr_Tot` + `P_25_34_yr_Tot`)",
     "✅ **High:** Lots of single people in your age group. Active social scene, dating culture is normal, nobody cares that you haven't settled down.\n❌ **Low:** Most people your age are already partnered or married. Can feel like a couples suburb — great if you're coupled up, less so if you're not."),

    ("dink_potential",           "DINK Potential",             "Partnered (no kids) 25–34 as % of 25–34 population. High = dual income, no kids energy — brunch every weekend, two car payments.",
     "G27B → (`P_Ptn_in_RM_25_34` + `P_Ptn_in_DFM_25_34`) ÷ G04A pop_25–34",
     "✅ **High:** Lots of dual-income couples without kids — that spending power flows into local venues, food, and experiences. Great local economy.\n❌ **Low:** Either very singles-dominated or already into kids territory. Fewer couples without dependents means less of that middle-income discretionary spending locally."),

    ("delayed_adulting_score",   "Delayed Adulting Score",     "Composite: singles + sharehouse residents + renters. High = classic quarter-life zone — independent but not quite settled.",
     "Composite → mean(`singles_scene_norm`, `sharehouse_capital_norm`, `renter_republic_pct_norm`)",
     "✅ **High:** Classic quarter-life territory — everyone is roughly in the same boat. Single-ish, renting, figuring it out. You won't be the only one.\n❌ **Low:** The area has moved on. People are settled, owned, and partnered. You might feel like you arrived at a party that ended five years ago."),

    ("global_youth_hub",         "Global Youth Hub",           "Overseas-born × youth density compound score. High = young, international, multilingual — think food halls and group chats in four languages.",
     "G02 `overseas_born_pct` × G04A `youth_pct` ÷ 100 → clipped at 99th percentile, scaled 0–100",
     "✅ **High:** Young, international, multicultural community. Easy to meet people from everywhere. International students will feel at home immediately.\n❌ **Low:** Older or more locally-born population. Less international energy — may feel homogeneous if you've come from somewhere else."),

    ("nightlife_index",          "Nightlife Index",            "Composite: night economy + entertainment + food scene. High = alive after 9pm — real venues, real bars, real reason to stay out.",
     "Composite → mean(`night_economy_pct_norm`, `entertainment_pct_norm`, `food_scene_pct_norm`)",
     "✅ **High:** Actual nightlife exists here — real venues, real bars, real reason to leave the house after 8pm that isn't a petrol station.\n❌ **Low:** Roll up the footpath after dinner. Any night out requires Ubering somewhere else and negotiating who's paying."),

    ("digital_nomad_potential",  "Digital Nomad Potential",    "Composite: WFH culture + knowledge hub + knowledge workers. High = laptop-and-latte territory — Slack at 11am, gym at 2pm.",
     "Composite → mean(`wfh_pct_norm`, `knowledge_hub_pct_norm`, `knowledge_worker_pct_norm`)",
     "✅ **High:** Working from a café until 3pm is completely normal here. Co-working culture is embedded and nobody stares at your laptop.\n❌ **Low:** Very 9-to-5 energy. Everyone commutes to an office. The concept of 'laptop-friendly café' is theoretical."),

    # 🧠 Funny / Viral
    ("just_one_more_degree",     "Just One More Degree",       "Tertiary students aged 25+ (FT or PT) as % of all enrolled. High = serial degree collectors — the PhD is just in case.",
     "G15 → (`Tert_Uni_oth_h_edu_Ft_25_ov_P` + `Tert_Uni_oth_h_edu_Pt_25_ov_P` + `Tert_Voc_edu_Ft_25_ov_P` + `Tert_Voc_edu_Pt_25_ov_P`) ÷ `Tot_P`",
     "✅ **High:** Postgrad and adult-learner culture is strong. You won't be the oldest person in the tutorial, and nobody finds a second degree unusual.\n❌ **Low:** Studying is firmly for the under-25s here. Postgrad culture is thin — you may feel like an outlier continuing your education later."),

    ("flat_white_density",       "Flat White Density",         "Composite: food scene + knowledge hub + startup dreamers. High = every third person is ordering a cortado and pitching a SaaS idea.",
     "Composite → mean(`food_scene_pct_norm`, `knowledge_hub_pct_norm`, `startup_dreamer_density_norm`)",
     "✅ **High:** The trifecta — good coffee, smart people, and startup energy. Productivity and casual networking happen in the same room at the same time.\n❌ **Low:** Not that kind of suburb. No one is pitching a SaaS idea over a pour-over. Which, honestly, might be a relief."),
]


def build_metric_section(sa2_df, metric_list):
    lines = []
    for entry in metric_list:
        col, title, desc = entry[0], entry[1], entry[2]
        source    = entry[3] if len(entry) > 3 else None
        good_bad  = entry[4] if len(entry) > 4 else None
        print(f"  {title}...")
        chart_file = make_histogram(sa2_df, col, title)
        norm_col = col + "_norm"
        if norm_col not in sa2_df.columns:
            continue
        top_tbl, bot_tbl = nsw_table(sa2_df, col)
        nsw_vals = sa2_df[sa2_df["state"] == "NSW"][norm_col].dropna()

        lines += [
            f"### {title}",
            desc,
        ]
        if source:
            lines.append(f"> **Calc:** {source}")
        if good_bad:
            lines.append(good_bad)
        lines += [
            f"NSW avg: **{nsw_vals.mean():.1f}** | AUS avg: **{sa2_df[norm_col].dropna().mean():.1f}** | NSW range: {nsw_vals.min():.1f}–{nsw_vals.max():.1f}",
        ]
        if chart_file:
            lines.append(f"![{title}](charts/{chart_file})")
        lines += ["", top_tbl, "", bot_tbl, "", "---", ""]
    return lines


def build_report(vibe_df, student_df, pow_df, meme_df):
    today = date.today().strftime("%-d %B %Y")
    nsw_n = vibe_df[vibe_df["state"] == "NSW"]["sa2_code"].nunique()
    all_n = vibe_df["sa2_code"].nunique()

    lines = [
        "# Vibe Metrics — NSW Report",
        f"Generated {today}. Data: ABS Census 2021 (Census + Working Population Profile). {all_n:,} SA2s nationally, {nsw_n:,} in NSW.",
        "",
        "## Lifestyle & Liveability",
        "Resident-based Census metrics on how people live, move, and feel in each area.",
        "",
    ]

    print("\nVibe metrics...")
    lines += build_metric_section(vibe_df, METRICS)

    lines += [
        "## What's Actually Here — Amenities Inferred from Place of Work",
        "These use the Working Population Profile (WPP) — who commutes INTO each SA2 to work. "
        "More food workers → real cafés exist. More health workers → real clinics exist. "
        "It's an infrastructure signal, not a jobs metric.",
        "",
    ]
    print("\nPlace-of-work / liveability metrics...")
    lines += build_metric_section(pow_df, POW_METRICS)

    lines += ["## Academic & Job Opportunity", ""]
    print("\nStudent metrics...")
    lines += build_metric_section(student_df, STUDENT_METRICS)

    lines += [
        "## Meme Metrics — Ages 18–34",
        "Culturally-framed signals targeting student life, early career, housing independence, "
        "relationships, and the kind of things that go viral on Reddit. "
        "Composites blend existing vibe/POW/student scores with new age-specific Census data.",
        "",
    ]
    print("\nMeme metrics...")
    lines += build_metric_section(meme_df, MEME_METRICS)

    return "\n".join(lines)


def main():
    OUT_DIR.mkdir(exist_ok=True)
    CHART_DIR.mkdir(exist_ok=True)

    print("Loading vibe data...")
    vibe_df = load_sa2(INPUT)
    print(f"  {len(vibe_df):,} unique SA2s")

    def load_optional(path, label):
        if path.exists():
            print(f"Loading {label}...")
            df = load_sa2(path)
            print(f"  {len(df):,} unique SA2s")
            return df
        print(f"WARNING: {path.name} not found — section skipped.")
        return vibe_df.iloc[0:0].copy()

    student_df = load_optional(INPUT_STU, "student metrics")
    pow_df     = load_optional(INPUT_POW, "place-of-work metrics")
    meme_df    = load_optional(INPUT_MEME, "meme metrics")

    print("\nGenerating report...")
    md = build_report(vibe_df, student_df, pow_df, meme_df)
    MD_FILE.write_text(md, encoding="utf-8")
    print(f"\nDone. Report: {MD_FILE}  ({MD_FILE.stat().st_size // 1024} KB)")
    print(f"Charts: {CHART_DIR}  ({len(list(CHART_DIR.glob('*.png')))} PNGs)")


if __name__ == "__main__":
    main()
