# core_part3_final_plots.py
# Final CORE vs NONCORE visualization panel

import os
import pandas as pd
import matplotlib.pyplot as plt
from config import Config

cfg = Config()
path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_with_core.csv")
df = pd.read_csv(path)

df["year"] = pd.to_numeric(df["publication_year"], errors="coerce")
df["open_access_is_oa"] = (
    df["open_access_is_oa"]
    .astype(str).str.lower()
    .isin(["1", "true", "yes"])
)

# -----------------------------
# Plot 1: Publications by year
# -----------------------------
years = df.groupby(["year", "core_status"]).size().unstack(fill_value=0)

# -----------------------------
# Plot 2: OA Share
# -----------------------------
oa = df.groupby("core_status")["open_access_is_oa"].mean()

# -----------------------------
# Create multi-panel figure
# -----------------------------
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

years.plot(ax=axes[0])
axes[0].set_title("Publications per Year (CORE vs NONCORE)")
axes[0].set_ylabel("Publications")
axes[0].set_xlabel("Year")

oa.plot(kind="bar", ax=axes[1], color=["green","red"])
axes[1].set_title("Open Access Rate")
axes[1].set_ylabel("OA Share")
axes[1].set_ylim(0,1)

plt.tight_layout()
plt.savefig(os.path.join(cfg.PROCESSED_DATA_PATH, "core_comparison_panel.png"))
plt.close()

print("✅ Final CORE vs NONCORE panel saved: core_comparison_panel.png")
