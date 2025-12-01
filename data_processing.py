import pandas as pd
import json
import os
from tqdm import tqdm
from config import Config


class DataProcessor:
    def __init__(self):
        self.config = Config()
        os.makedirs(self.config.PROCESSED_DATA_PATH, exist_ok=True)

    def load_raw_data(self):
        """Load the latest raw JSON data"""
        raw_files = [
            f
            for f in os.listdir(self.config.RAW_DATA_PATH)
            if f.endswith(".json") and "eu_works_on_AI" in f
        ]
        if not raw_files:
            raise FileNotFoundError("No raw data files found!")

        latest_file = sorted(raw_files)[-1]
        file_path = os.path.join(self.config.RAW_DATA_PATH, latest_file)

        print(f"Loading raw data from: {latest_file}")
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        eu_works = data["eu_works"]
        print(f"Loaded {len(eu_works)} EU works")
        return eu_works

    def parse_works_to_dataframe(self, works):
        """Parse raw works into structured DataFrame"""
        records = []

        for work in tqdm(works, desc="Parsing works"):
            record = {
                "work_id": work.get("id"),
                "doi": work.get("doi"),
                "title": work.get("title"),
                "publication_year": work.get("publication_year"),
                "publication_date": work.get("publication_date"),
                "type": work.get("type"),
                "language": work.get("language"),
                "cited_by_count": work.get("cited_by_count", 0),
                "open_access_is_oa": work.get("open_access", {}).get("is_oa", False),
                "open_access_oa_status": work.get("open_access", {}).get(
                    "oa_status", ""
                ),
            }

            authorships = work.get("authorships", [])
            record["authors"] = []
            record["institutions"] = []
            record["countries"] = []
            record["institution_ids"] = []

            for authorship in authorships:
                author = authorship.get("author", {})
                if author:
                    record["authors"].append(
                        {
                            "author_id": author.get("id"),
                            "author_name": author.get("display_name"),
                            "orcid": author.get("orcid"),
                        }
                    )

                institutions = authorship.get("institutions", [])
                for inst in institutions:
                    if inst:
                        institution_data = {
                            "institution_id": inst.get("id"),
                            "institution_name": inst.get("display_name"),
                            "country_code": inst.get("country_code"),
                            "institution_type": inst.get("type"),
                        }
                        record["institutions"].append(institution_data)
                        record["institution_ids"].append(inst.get("id"))

                        if inst.get("country_code"):
                            record["countries"].append(inst.get("country_code"))

            record["countries"] = list(set(record["countries"]))
            record["institution_ids"] = list(set(record["institution_ids"]))

            records.append(record)

        return pd.DataFrame(records)

    def create_all_dataset(self, df):
        """Create the ALL dataset with derived fields"""
        df_all = df.copy()

        df_all["multi_institution"] = df_all["institution_ids"].apply(
            lambda x: len(x) > 1 if isinstance(x, list) else False
        )

        df_all["multi_country"] = df_all["countries"].apply(
            lambda x: len(x) > 1 if isinstance(x, list) else False
        )

        df_all["authors_count"] = df_all["authors"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )

        df_all["institutions_count"] = df_all["institutions"].apply(
            lambda x: len(x) if isinstance(x, list) else 0
        )

        print(f"ALL dataset created: {len(df_all)} works")
        print(f"Multi-institution works: {df_all['multi_institution'].sum()}")
        print(f"Multi-country works: {df_all['multi_country'].sum()}")

        return df_all


if __name__ == "__main__":
    processor = DataProcessor()

    raw_works = processor.load_raw_data()
    df = processor.parse_works_to_dataframe(raw_works)

    df_all = processor.create_all_dataset(df)

    output_file = os.path.join(processor.config.PROCESSED_DATA_PATH, "works_all.csv")
    df_all.to_csv(output_file, index=False)
    print(f"ALL dataset saved to: {output_file}")

    json_file = os.path.join(processor.config.PROCESSED_DATA_PATH, "works_all.json")
    df_all.to_json(json_file, orient="records", indent=2)
    print(f"ALL dataset saved as JSON: {json_file}")
