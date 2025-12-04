import pandas as pd
import os
from config import Config
import matplotlib.pyplot as plt

cfg = Config()

# Load STRICT dataset
strict_path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.json")
df = pd.read_json(strict_path)

# Count publications per year
pubs_per_year = df["publication_year"].value_counts().sort_index()

# Save table
out_table = os.path.join(cfg.PROCESSED_DATA_PATH, "publications_per_year.csv")
pubs_per_year.to_csv(out_table)

# Plot
plt.figure()
pubs_per_year.plot(kind="bar")
plt.title("Publications per Year (STRICT)")
plt.xlabel("Year")
plt.ylabel("Number of Publications")
plt.tight_layout()

# Save plot
out_plot = os.path.join(cfg.PROCESSED_DATA_PATH, "publications_per_year.png")
plt.savefig(out_plot)
plt.close()

print("✅ Publications per year table and plot saved:")
print(out_table)
print(out_plot)
