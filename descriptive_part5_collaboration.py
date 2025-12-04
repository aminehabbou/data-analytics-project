import pandas as pd
import matplotlib.pyplot as plt
import os
from config import Config

cfg = Config()

# Load STRICT dataset
path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.json")
df = pd.read_json(path)

# Compute rates
multi_inst_pct = df["multi_institution"].mean() * 100
multi_country_pct = df["multi_country"].mean() * 100

# Save summary table
summary = pd.DataFrame({
    "Metric": ["Multi-institution papers", "Multi-country papers"],
    "Percentage": [round(multi_inst_pct, 2), round(multi_country_pct, 2)]
})

out_table = os.path.join(cfg.PROCESSED_DATA_PATH, "collaboration_metrics.csv")
summary.to_csv(out_table, index=False)

# Plot
plt.figure()
plt.bar(summary["Metric"], summary["Percentage"])
plt.ylabel("Percentage (%)")
plt.title("Collaboration Patterns (STRICT)")
plt.ylim(0, 100)
plt.tight_layout()

out_plot = os.path.join(cfg.PROCESSED_DATA_PATH, "collaboration_metrics.png")
plt.savefig(out_plot)
plt.close()

print("✅ Collaboration metrics saved:")
print(out_table)
print(out_plot)


