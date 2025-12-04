# core_part2_descriptives.py
#
# Goal:
# Compare CORE vs NONCORE publications (STRICT dataset)
# Outputs:
# - Publications per year (core vs noncore)
# - Open access share (core vs noncore)
# - Top sources per group
# - Country breakdown
# - Top concepts comparison

import os
import pandas as pd
import matplotlib.pyplot as plt
from config import Config

cfg = Config()

# -----------------------------
# LOAD DATA
# -----------------------------
path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_with_core.csv")
df = pd.read_csv(path)

print("✅ Loaded dataset:", df.shape)
print(df["core_status"].value_counts())

# Standardize year column
df["year"] = pd.to_numeric(df["publication_year"], errors="coerce")

# -----------------------------
# 1. PUBLICATIONS PER YEAR
# -----------------------------
years = df.groupby(["year", "core_status"]).size().unstack(fill_value=0)

years.to_csv(os.path.join(cfg.PROCESSED_DATA_PATH, "core_publications_per_year.csv"))

years.plot(kind="bar", figsize=(10, 6), title="Publications per Year: CORE vs NONCORE")
plt.ylabel("Number of publications")
plt.tight_layout()
plt.savefig(os.path.join(cfg.PROCESSED_DATA_PATH, "core_publications_per_year.png"))
plt.close()

# -----------------------------
# 2. OPEN ACCESS SHARE
# -----------------------------
# In YOUR dataset the OA flag is called 'open_access_is_oa'
if "open_access_is_oa" not in df.columns:
    print("⚠ Column 'open_access_is_oa' not found. Available columns:")
    print(df.columns.tolist())
    raise KeyError("Expected OA column 'open_access_is_oa'")

oa_col = "open_access_is_oa"
print("✅ Using OA column:", oa_col)

# Ensure boolean (sometimes stored as 0/1 or True/False)
df[oa_col] = df[oa_col].astype(str).str.lower().isin(["1", "true", "yes"])

oa = df.groupby("core_status")[oa_col].mean().round(3)
oa.to_csv(os.path.join(cfg.PROCESSED_DATA_PATH, "core_oa_share.csv"))

oa.plot(kind="bar", title="Open Access Rate: CORE vs NONCORE")
plt.ylabel("OA Share")
plt.tight_layout()
plt.savefig(os.path.join(cfg.PROCESSED_DATA_PATH, "core_oa_share.png"))
plt.close()

# -----------------------------
# 3. TOP SOURCES (JOURNALS)
# -----------------------------
top_sources = (
    df.groupby(["core_status", "source_name"])
      .size()
      .reset_index(name="count")
      .sort_values(["core_status", "count"], ascending=[True, False])
)

top_sources.to_csv(os.path.join(cfg.PROCESSED_DATA_PATH, "core_top_sources.csv"), index=False)

# -----------------------------
# 4. COUNTRY BREAKDOWN
# -----------------------------
# we don't have a direct country column in this file,
# so we skip plotting by country if it's missing
if "country_code" in df.columns:
    countries = (
        df.groupby(["core_status", "country_code"])
          .size()
          .reset_index(name="count")
          .sort_values("count", ascending=False)
    )

    countries.to_csv(
        os.path.join(cfg.PROCESSED_DATA_PATH, "core_country_breakdown.csv"),
        index=False
    )

    # Plot (Top 10 per group)
    top_countries = countries.groupby("core_status").head(10)

    for status in top_countries["core_status"].unique():
        subset = top_countries[top_countries["core_status"] == status]
        subset.plot(
            x="country_code",
            y="count",
            kind="bar",
            title=f"Top Countries ({status.upper()})",
            legend=False,
            figsize=(8, 5)
        )
        plt.ylabel("Publications")
        plt.tight_layout()
        plt.savefig(os.path.join(cfg.PROCESSED_DATA_PATH,
                                 f"core_top_countries_{status}.png"))
        plt.close()
else:
    print("ℹ No 'country_code' column in dataset_strict_with_core.csv – skipping country plots.")

# -----------------------------
# 5. TOP CONCEPTS
# -----------------------------
# In YOUR file the concepts field is called 'concepts_list'
if "concepts_list" in df.columns:
    df["concepts_list"] = df["concepts_list"].fillna("")

    concept_rows = []

    for _, row in df.iterrows():
        for c in row["concepts_list"].split(";"):
            c = c.strip()
            if c:
                concept_rows.append([row["core_status"], c])

    concepts_df = pd.DataFrame(concept_rows, columns=["core_status", "concept"])

    concepts_summary = (
        concepts_df.groupby(["core_status", "concept"])
                   .size()
                   .reset_index(name="count")
                   .sort_values("count", ascending=False)
    )

    concepts_summary.to_csv(
        os.path.join(cfg.PROCESSED_DATA_PATH, "core_concepts_comparison.csv"),
        index=False
    )
else:
    print("ℹ No 'concepts_list' column found – skipping concept comparison.")

print("✅ Core vs Non-core analysis COMPLETE.")
print("📁 Output files saved in data/processed/")


