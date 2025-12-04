import pandas as pd
import matplotlib.pyplot as plt
import os
from config import Config

cfg = Config()

# Load STRICT dataset
path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.json")
df = pd.read_json(path)

# Keep only records with source info
df_sources = df.dropna(subset=["source_name"])

# Group by source
source_counts = (
    df_sources
    .groupby(["source_name", "source_issn_l", "is_scopus_indexed"])
    .size()
    .reset_index(name="works_count")
    .sort_values("works_count", ascending=False)
)

# Keep top 15
top_sources = source_counts.head(15)

# Rename column
top_sources["core_status"] = top_sources["is_scopus_indexed"].apply(
    lambda x: "Core (Scopus)" if x else "Non-core"
)

# Save table
out_table = os.path.join(cfg.PROCESSED_DATA_PATH, "top_sources.csv")
top_sources.to_csv(out_table, index=False)

# Plot
plt.figure()
plt.barh(top_sources["source_name"], top_sources["works_count"])
plt.title("Top Journals / Sources (STRICT)")
plt.xlabel("Number of Publications")
plt.gca().invert_yaxis()
plt.tight_layout()

# Save plot
out_plot = os.path.join(cfg.PROCESSED_DATA_PATH, "top_sources.png")
plt.savefig(out_plot)
plt.close()

print("✅ Top sources saved:")
print(out_table)
print(out_plot)
