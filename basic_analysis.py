# basic_analysis.py
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from config import Config
import os


class BasicAnalyzer:
    def __init__(self):
        self.config = Config() 

    def load_all_dataset(self):
        """Load the ALL dataset"""
        file_path = os.path.join(self.config.PROCESSED_DATA_PATH, "works_all.csv")
        df = pd.read_csv(file_path)
        print(f"Loaded ALL dataset: {len(df)} works")
        return df

    def generate_basic_stats(self, df):
        """Generate basic statistics"""
        print("\n=== BASIC STATISTICS ===")
        print(f"Total publications: {len(df)}")
        print(
            f"Time range: {df['publication_year'].min()} - {df['publication_year'].max()}"
        )
        print(f"Open Access rate: {df['open_access_is_oa'].mean():.1%}")
        print(f"Multi-institution collaboration: {df['multi_institution'].mean():.1%}")
        print(f"International collaboration: {df['multi_country'].mean():.1%}")

        # Yearly distribution
        yearly_counts = df["publication_year"].value_counts().sort_index()
        print("\nYearly distribution:")
        for year, count in yearly_counts.items():
            print(f"  {year}: {count} publications")

        # Country distribution
        all_countries = []
        for countries in df["countries"]:
            if isinstance(countries, str):
                countries = eval(countries)  # Convert string to list
            if isinstance(countries, list):
                all_countries.extend(countries)

        country_counts = pd.Series(all_countries).value_counts()
        print("\nTop 10 EU countries:")
        for country, count in country_counts.head(10).items():
            print(f"  {country}: {count} affiliations")


if __name__ == "__main__":
    analyzer = BasicAnalyzer()
    df_all = analyzer.load_all_dataset()
    analyzer.generate_basic_stats(df_all)
