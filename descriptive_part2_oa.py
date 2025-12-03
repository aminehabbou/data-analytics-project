import pandas as pd
import matplotlib.pyplot as plt
import os
from config import Config

cfg = Config()

# Load STRICT dataset
path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.json")
df = pd.read_json(path)

# Count OA vs Non-OA
oa_counts = df["open_access_is_oa"].value_counts()

# Rename for clarity
oa_counts.index = ["Closed", "Open Access"]

# Save table
out_table = os.path.join(cfg.PROCESSED_DATA_PATH, "open_access_counts.csv")
oa_counts.to_csv(out_table)

# Plot pie chart
plt.figure()
oa_counts.plot(kind="pie", autopct="%1.1f%%")
plt.title("Open Access Share (STRICT)")
plt.ylabel("")
plt.tight_layout()

# Save figure
out_plot = os.path.join(cfg.PROCESSED_DATA_PATH, "open_access_share.png")
plt.savefig(out_plot)
plt.close()

print("✅ Open Access stats saved:")
print(out_table)
print(out_plot)
