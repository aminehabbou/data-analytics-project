import requests
import pandas as pd
import time
import json
import os
from tqdm import tqdm
from config import Config


class OpenAlexCollector:
    def __init__(self):
        self.config = Config()
        os.makedirs(self.config.RAW_DATA_PATH, exist_ok=True)

    def build_search_query(self):
        search_terms = " OR ".join(
            [f'"{keyword}"' for keyword in self.config.AI_EDU_KEYWORDS]
        )
        return search_terms

    def has_eu_affiliation(self, work):
        authorships = work.get("authorships", [])
        for authorship in authorships:
            institutions = authorship.get("institutions", [])
            for inst in institutions:
                country_code = inst.get("country_code")
                if country_code and country_code in self.config.EU_COUNTRIES:
                    return True
        return False

    def collect_all_works(self):
        print("COLLECTING PUBLICATIONS")
        print(f"Keywords: {len(self.config.AI_EDU_KEYWORDS)}")
        print(f"EU Countries: {len(self.config.EU_COUNTRIES)}")
        print(f"Time: {self.config.START_YEAR}-{self.config.END_YEAR}")

        all_works = []
        page = 1
        per_page = 200

        progress_bar = tqdm(total=10000, desc="Total works")

        while True:
            query_params = {
                "filter": f"publication_year:{self.config.START_YEAR}-{self.config.END_YEAR},type:article",
                "search": self.build_search_query(),
                "per-page": per_page,
                "page": page,
                "select": "id,doi,title,publication_year,publication_date,authorships",
            }

            try:
                response = requests.get(self.config.WORKS_URL, params=query_params)

                if response.status_code != 200:
                    break

                data = response.json()
                works_batch = data.get("results", [])

                if not works_batch:
                    break

                all_works.extend(works_batch)
                progress_bar.update(len(works_batch))
                progress_bar.set_description(f"Page {page}, Total: {len(all_works)}")

                page += 1
                time.sleep(0.5)

                if page > 50:
                    break

            except Exception as e:
                break

        progress_bar.close()

        print(f"Total works collected: {len(all_works)}")

        eu_works = [
            work
            for work in tqdm(all_works, desc="Filtering EU")
            if self.has_eu_affiliation(work)
        ]
        print(f"EU works: {len(eu_works)}")

        filename = (
            f"eu_works_on_AI_IN_Education_between_2020_and_2025_N_{len(eu_works)}.json"
        )
        output_path = os.path.join(self.config.RAW_DATA_PATH, filename)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"all_works": all_works, "eu_works": eu_works}, f, indent=2)

        print(f"Saved to: {output_path}")

        if eu_works:
            self._print_stats(eu_works)

        return all_works, eu_works

    def _print_stats(self, works):
        print("\nSTATISTICS:")

        years = [
            work.get("publication_year")
            for work in works
            if work.get("publication_year")
        ]
        year_counts = pd.Series(years).value_counts().sort_index()
        print(f"Publication years: {dict(year_counts)}")

        eu_countries_found = set()
        for work in works:
            authorships = work.get("authorships", [])
            for authorship in authorships:
                for inst in authorship.get("institutions", []):
                    country = inst.get("country_code")
                    if country in self.config.EU_COUNTRIES:
                        eu_countries_found.add(country)
        print(f"EU countries: {sorted(eu_countries_found)}")


if __name__ == "__main__":
    collector = OpenAlexCollector()
    all_works, eu_works = collector.collect_all_works()
