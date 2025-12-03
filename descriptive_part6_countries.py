import pandas as pd
import matplotlib.pyplot as plt
import os
from config import Config

cfg = Config()

# Load institutions table (STRICT)
path = os.path.join(cfg.PROCESSED_DATA_PATH, "institutions_strict.csv")
df = pd.read_csv(path)

# Count institutions by country
country_counts = df["country_code"].value_counts().reset_index()
country_counts.columns = ["country", "institutions"]

# Save table
out_table = os.path.join(cfg.PROCESSED_DATA_PATH, "institutions_by_country.csv")
country_counts.to_csv(out_table, index=False)

# Plot (Top 15)
top = country_counts.head(15)

plt.figure()
plt.barh(top["country"], top["institutions"])
plt.title("Top EU Countries by Institution Count (STRICT)")
plt.xlabel("Number of Institutions")
plt.gca().invert_yaxis()
plt.tight_layout()

out_plot = os.path.join(cfg.PROCESSED_DATA_PATH, "institutions_by_country.png")
plt.savefig(out_plot)
plt.close()

print("✅ Country breakdown saved:")
print(out_table)
print(out_plot)
