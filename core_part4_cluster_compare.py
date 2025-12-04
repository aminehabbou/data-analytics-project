import pandas as pd
import os
import matplotlib.pyplot as plt
from config import Config

cfg = Config()

# -------------------------------
# 1. LOAD DATA
# -------------------------------

clusters_path = os.path.join(cfg.PROCESSED_DATA_PATH, "institution_clusters.csv")
inst_path = os.path.join(cfg.PROCESSED_DATA_PATH, "institutions_strict.csv")

clusters = pd.read_csv(clusters_path)
institutions = pd.read_csv(inst_path)

# Fix possible whitespace problems
clusters.columns = clusters.columns.str.strip()
institutions.columns = institutions.columns.str.strip()

print("Clusters columns:", clusters.columns.tolist())
print("Institutions columns:", institutions.columns.tolist())

# -------------------------------
# 2. MERGE BY institution_id
# -------------------------------

df = clusters.merge(institutions, on="institution_id", how="left")
print("Merged dataframe:", df.shape)
print(df.head())

# -------------------------------
# 3. SUMMARY STATISTICS PER CLUSTER
# -------------------------------

summary = df.groupby("cluster").agg({
    "works_count": "mean",
    "citations_sum": "mean",
    "oa_share": "mean",
    "collaboration_rate": "mean",
    "multi_country_rate": "mean"
}).reset_index()

print("\nCluster summary:")
print(summary)

# -------------------------------
# 4. SAVE SUMMARY
# -------------------------------

out_csv = os.path.join(cfg.PROCESSED_DATA_PATH, "cluster_summary.csv")
summary.to_csv(out_csv, index=False)
print("Saved:", out_csv)

# -------------------------------
# 5. PLOT: OA SHARE BY CLUSTER
# -------------------------------

plt.figure(figsize=(7,5))
plt.bar(summary["cluster"], summary["oa_share"], color="skyblue")
plt.title("Open Access Share by Cluster")
plt.xlabel("Cluster")
plt.ylabel("OA Share")
plt.tight_layout()

oa_plot_path = os.path.join(cfg.PROCESSED_DATA_PATH, "cluster_oa_share.png")
plt.savefig(oa_plot_path)
plt.close()
print("Saved:", oa_plot_path)

# -------------------------------
# 6. PLOT: CITATIONS BY CLUSTER
# -------------------------------

plt.figure(figsize=(7,5))
plt.bar(summary["cluster"], summary["citations_sum"], color="lightgreen")
plt.title("Average Citations by Cluster")
plt.xlabel("Cluster")
plt.ylabel("Citations (avg)")
plt.tight_layout()

cit_plot_path = os.path.join(cfg.PROCESSED_DATA_PATH, "cluster_citations.png")
plt.savefig(cit_plot_path)
plt.close()
print("Saved:", cit_plot_path)

print("\n✅ PART 4 FINISHED SUCCESSFULLY.")

