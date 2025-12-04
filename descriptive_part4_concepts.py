import pandas as pd
import matplotlib.pyplot as plt
import os
from collections import Counter
from config import Config

cfg = Config()

# Load dataset
path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.json")
df = pd.read_json(path)

# Extract concepts
all_concepts = []

for row in df["concepts_list"].dropna():
    concepts = [c.strip() for c in row.split(";") if c.strip()]
    all_concepts.extend(concepts)

# Count frequency
concept_counts = Counter(all_concepts)

# Convert to DataFrame
df_concepts = pd.DataFrame(
    concept_counts.items(),
    columns=["concept", "count"]
).sort_values("count", ascending=False)

# Keep top 20
top_concepts = df_concepts.head(20)

# Save to CSV
out_table = os.path.join(cfg.PROCESSED_DATA_PATH, "top_concepts.csv")
top_concepts.to_csv(out_table, index=False)

# Plot
plt.figure()
plt.barh(top_concepts["concept"], top_concepts["count"])
plt.gca().invert_yaxis()
plt.title("Top Concepts / Keywords (STRICT)")
plt.xlabel("Frequency")
plt.tight_layout()

# Save plot
out_plot = os.path.join(cfg.PROCESSED_DATA_PATH, "top_concepts.png")
plt.savefig(out_plot)
plt.close()

print("✅ Top concepts saved:")
print(out_table)
print(out_plot)
