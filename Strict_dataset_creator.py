import os
import time
import json
import requests
import pandas as pd
from tqdm import tqdm


def normalize_openalex_id(x):
    if pd.isna(x):
        return ""
    return str(x).replace("https://openalex.org/", "").strip()


def normalize_issn(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).replace("-", "").strip().upper()


class FinalStrictCreator:
    def __init__(self):
        from config import Config

        self.config = Config()

        self.scopus_sources = self.load_scopus_sources()
        self.scimago_data = self.load_scimago_data()
        self._openalex_cache = {}

    def load_scopus_sources(self):
        scopus_file = os.path.join(
            self.config.EXTERNAL_DATA_PATH, "Scopus Source", "scopus_sources.csv"
        )
        if not os.path.exists(scopus_file):
            print(f"Scopus file not found at: {scopus_file}")
            return set()

        try:
            df = pd.read_csv(scopus_file, encoding="latin-1", low_memory=False)
        except Exception as e:
            print(f"Failed to load Scopus file ({e})")
            return set()

        issn_set = set()
        for col in ["ISSN", "EISSN", "Issn", "issn"]:
            if col in df.columns:
                for val in df[col].dropna().astype(str):
                    clean = normalize_issn(val)
                    if len(clean) == 8:
                        issn_set.add(clean)
        print(f"Loaded Scopus sources: {len(issn_set)} valid ISSNs")
        return issn_set

    def load_scimago_data(self):
        scimago_dir = os.path.join(self.config.EXTERNAL_DATA_PATH, "SCImago")
        scimago_data = {}
        for year in range(2020, 2025):
            parts = []
            filenames = [
                f"scimagojr {year}  Subject Category - Artificial Intelligence_Eastern Europe.csv",
                f"scimagojr {year}  Subject Category - Artificial Intelligence_Western Europe.csv",
                # Add fallback names just in case
                f"scimagojr {year}  Subject Category - Artificial Intelligence_Eastern Europe .csv",
                f"scimagojr {year}  Subject Category - Artificial Intelligence_Western Europe .csv",
            ]
            for fn in filenames:
                p = os.path.join(scimago_dir, fn)
                if os.path.exists(p):
                    try:
                        df = pd.read_csv(p, sep=";")
                        scimago_cols = [c for c in df.columns]
                        df = df.rename(columns={c: c.strip() for c in scimago_cols})
                        if "Eastern" in fn:
                            df["Region"] = "Eastern Europe"
                        elif "Western" in fn:
                            df["Region"] = "Western Europe"
                        parts.append(df)
                        print(f"SCImago AI {year}: loaded {fn} ({len(df)} rows)")
                    except Exception as e:
                        print(f"Failed reading {p}: {e}")
            if parts:
                combined = pd.concat(parts, ignore_index=True)
                scimago_data[year] = combined
                print(f"SCImago AI {year} TOTAL: {len(combined)} journals")
            else:
                print(f"SCImago AI {year}: no files found in {scimago_dir}")
        return scimago_data

    def get_work_details_single(self, work_id_clean):
        if not work_id_clean:
            return None

        if work_id_clean in self._openalex_cache:
            return self._openalex_cache[work_id_clean]

        url = f"https://api.openalex.org/works/{work_id_clean}"
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                data = r.json()
                self._openalex_cache[work_id_clean] = data
                return data
            else:
                print(f"⚠️ OpenAlex status {r.status_code} for {work_id_clean}")
                return None
        except Exception as e:
            print(f"⚠️ OpenAlex request failed for {work_id_clean}: {e}")
            return None

    def get_scimago_quartile(self, source_issn, publication_year):
        if not source_issn or publication_year not in self.scimago_data:
            return None

        df_year = self.scimago_data[publication_year]
        clean_target = normalize_issn(source_issn)

        if clean_target == "":
            return None

        for idx, row in df_year.iterrows():
            issn_col_candidates = [
                c for c in row.index if c.lower() == "issn" or "issn" in c.lower()
            ]
            issn_values = []
            for c in issn_col_candidates:
                if pd.notna(row[c]):
                    issn_values.append(str(row[c]))

            for issn_raw in issn_values:
                for candidate in (s.strip() for s in issn_raw.split(",")):
                    if normalize_issn(candidate) == clean_target:
                        for qcol in [
                            "SJR Quartile",
                            "SJR quartile",
                            "Quartile",
                            "sjr quartile",
                            "SJR",
                        ]:
                            if qcol in row and pd.notna(row[qcol]):
                                q = str(row[qcol]).strip()
                                if q in ["Q1", "Q2", "Q3", "Q4"]:
                                    return q
        return None

    def is_scopus_indexed(self, source_issn_list):
        if not self.scopus_sources or not source_issn_list:
            return False
        for issn in source_issn_list:
            if normalize_issn(issn) in self.scopus_sources:
                return True
        return False

    def ensure_all_columns_exist(self, df):
        defaults = {
            "is_scopus_indexed": False,
            "scimago_quartile": None,
            "source_name": None,
            "source_type": None,
            "source_issn_l": None,
            "source_issn": [],
            "concepts_list": "",
            "venue_issn_list": "",
            "abstract": None,
            "open_access_is_oa": False,
            "open_access_oa_status": "",
            "publication_year": None,
            "cited_by_count": 0,
            "multi_institution": False,
            "multi_country": False,
            "authors_count": 0,
            "institutions_count": 0,
        }
        for k, v in defaults.items():
            if k not in df.columns:
                df[k] = v
        return df

    def enhance_all_dataset(self, df_all):
        print("🚀 Creating ENHANCED ALL dataset...")

        if "work_id" not in df_all.columns:
            raise KeyError("works_all.csv must contain 'work_id' column")

        df_all = df_all.copy()
        df_all["work_id_clean"] = df_all["work_id"].apply(normalize_openalex_id)

        work_ids = df_all["work_id_clean"].tolist()
        works_details = []

        for wid in tqdm(work_ids, desc="Fetching works"):
            detail = self.get_work_details_single(wid)
            works_details.append((wid, detail))
            time.sleep(0.15)

        print(f"Successfully attempted fetch for {len(works_details)} works")

        enhanced_rows = []
        for wid, work in tqdm(works_details, desc="Processing works"):
            if not work:
                original_rows = df_all[df_all["work_id_clean"] == wid]
                if len(original_rows) == 0:
                    continue
                orig = original_rows.iloc[0]
                row = {
                    "work_id": orig["work_id"],
                    "doi": None,
                    "title": None,
                    "abstract": None,
                    "publication_year": orig.get("publication_year", None),
                    "publication_date": orig.get("publication_date", None),
                    "type": orig.get("type", None),
                    "language": orig.get("language", None),
                    "cited_by_count": orig.get("cited_by_count", 0),
                    "open_access_is_oa": orig.get("open_access_is_oa", False),
                    "open_access_oa_status": orig.get("open_access_oa_status", ""),
                    "source_name": None,
                    "source_type": None,
                    "source_issn_l": None,
                    "source_issn": [],
                    "multi_institution": orig.get("multi_institution", False),
                    "multi_country": orig.get("multi_country", False),
                    "authors_count": orig.get("authors_count", 0),
                    "institutions_count": orig.get("institutions_count", 0),
                    "concepts_list": "",
                    "venue_issn_list": "",
                    "is_scopus_indexed": False,
                    "scimago_quartile": None,
                }
                enhanced_rows.append(row)
                continue

            original_rows = df_all[df_all["work_id_clean"] == wid]
            if len(original_rows) == 0:
                continue
            original_row = original_rows.iloc[0]

            primary_source = work.get("primary_location", {}).get("source", {}) or {}
            issn_list = []
            if primary_source.get("issn_l"):
                issn_list.append(primary_source.get("issn_l"))
            if primary_source.get("issn"):
                issn_field = primary_source.get("issn")
                if isinstance(issn_field, (list, tuple)):
                    issn_list.extend(issn_field)
                else:
                    issn_list.extend(
                        [s.strip() for s in str(issn_field).split(",") if s.strip()]
                    )
            issn_list = [s for s in set([normalize_issn(x) for x in issn_list]) if s]

            row = {
                "work_id": original_row["work_id"],
                "doi": work.get("doi"),
                "title": work.get("title"),
                "abstract": work.get("abstract"),
                "publication_year": work.get("publication_year")
                or original_row.get("publication_year"),
                "publication_date": work.get("publication_date")
                or original_row.get("publication_date"),
                "type": work.get("type") or original_row.get("type"),
                "language": work.get("language") or original_row.get("language"),
                "cited_by_count": work.get("cited_by_count", 0),
                "open_access_is_oa": work.get("open_access", {}).get("is_oa", False),
                "open_access_oa_status": work.get("open_access", {}).get(
                    "oa_status", ""
                ),
                "source_name": primary_source.get("display_name"),
                "source_type": primary_source.get("type"),
                "source_issn_l": primary_source.get("issn_l"),
                "source_issn": issn_list,
                "multi_institution": original_row.get("multi_institution", False),
                "multi_country": original_row.get("multi_country", False),
                "authors_count": original_row.get("authors_count", 0),
                "institutions_count": original_row.get("institutions_count", 0),
            }

            concepts = [
                c.get("display_name")
                for c in work.get("concepts", [])
                if c.get("display_name")
            ]
            topics = [
                t.get("display_name")
                for t in work.get("topics", [])
                if t.get("display_name")
            ]
            all_concepts = [c for c in (concepts + topics) if str(c).lower() != "other"]
            row["concepts_list"] = ";".join(all_concepts) if all_concepts else ""

            row["venue_issn_list"] = ";".join(issn_list) if issn_list else ""

            row["is_scopus_indexed"] = self.is_scopus_indexed(issn_list)

            row["scimago_quartile"] = None
            pub_year = row.get("publication_year")
            if issn_list and pub_year:
                for issn in issn_list:
                    q = self.get_scimago_quartile(issn, pub_year)
                    if q:
                        row["scimago_quartile"] = q
                        break

            enhanced_rows.append(row)

        df_enhanced = pd.DataFrame(enhanced_rows)
        df_enhanced = self.ensure_all_columns_exist(df_enhanced)
        if "publication_year" in df_enhanced.columns:
            df_enhanced["publication_year"] = pd.to_numeric(
                df_enhanced["publication_year"], errors="coerce"
            ).astype("Int64")
        if "cited_by_count" in df_enhanced.columns:
            df_enhanced["cited_by_count"] = (
                pd.to_numeric(df_enhanced["cited_by_count"], errors="coerce")
                .fillna(0)
                .astype(int)
            )
        return df_enhanced

    def create_strict_dataset(self, df_enhanced, quartile_threshold="Q3"):
        print(f"Creating STRICT dataset (Q1-{quartile_threshold})...")
        df_enhanced = self.ensure_all_columns_exist(df_enhanced)

        if self.scopus_sources:
            scopus_filtered = df_enhanced[df_enhanced["is_scopus_indexed"] == True]
            print(f"Scopus-indexed works: {len(scopus_filtered)}")
        else:
            scopus_filtered = df_enhanced
            print("Scopus list empty: skipping Scopus filter")

        valid_quartiles = (
            ["Q1", "Q2", "Q3"] if quartile_threshold == "Q3" else ["Q1", "Q2"]
        )
        strict = scopus_filtered[
            scopus_filtered["scimago_quartile"].isin(valid_quartiles)
        ]
        print(f"Works with SCImago AI Q1-{quartile_threshold}: {len(strict)}")

        if len(df_enhanced) > 0:
            pct = len(strict) / len(df_enhanced) * 100
            print(f"STRICT dataset: {len(strict)} works ({pct:.1f}% of ALL)")
        else:
            print(f"STRICT dataset: {len(strict)} works")

        return strict

    def save_datasets(self, df_all, df_strict, quartile_threshold):
        processed_dir = self.config.PROCESSED_DATA_PATH
        os.makedirs(processed_dir, exist_ok=True)

        all_csv = os.path.join(processed_dir, "dataset_all_enhanced.csv")
        all_json = os.path.join(processed_dir, "dataset_all_enhanced.json")
        strict_csv = os.path.join(
            processed_dir, f"dataset_strict_q{quartile_threshold}.csv"
        )
        strict_json = os.path.join(
            processed_dir, f"dataset_strict_q{quartile_threshold}.json"
        )

        df_all.to_csv(all_csv, index=False)
        df_all.to_json(all_json, orient="records", indent=2, force_ascii=False)
        df_strict.to_csv(strict_csv, index=False)
        df_strict.to_json(strict_json, orient="records", indent=2, force_ascii=False)

        print(f"\nALL dataset saved:\n   CSV: {all_csv}\n   JSON: {all_json}")
        print(f"\nSTRICT dataset saved:\n   CSV: {strict_csv}\n   JSON: {strict_json}")

        self.generate_final_summary(df_all, df_strict, quartile_threshold)

    def generate_final_summary(self, df_all, df_strict, quartile_threshold):
        print("\n" + "=" * 60)
        print("FINAL DATASET SUMMARY")
        print("=" * 60)
        df_all = self.ensure_all_columns_exist(df_all)
        df_strict = self.ensure_all_columns_exist(df_strict)

        print("\nALL Dataset (Enhanced):")
        print(f"   Total works: {len(df_all):,}")
        if "publication_year" in df_all.columns:
            try:
                py = df_all["publication_year"].value_counts().sort_index().to_dict()
                print(f"   Publication years: {py}")
            except Exception:
                print("   Publication years: (unavailable)")
        print(
            f"   Multi-institution: {int(df_all['multi_institution'].sum()):,} ({df_all['multi_institution'].mean()*100:.1f}%)"
        )
        print(
            f"   Multi-country: {int(df_all['multi_country'].sum()):,} ({df_all['multi_country'].mean()*100:.1f}%)"
        )
        print(
            f"   Open access: {int(df_all['open_access_is_oa'].sum()):,} ({df_all['open_access_is_oa'].mean()*100:.1f}%)"
        )
        print(f"   Average citations: {df_all['cited_by_count'].mean():.2f}")

        if "is_scopus_indexed" in df_all.columns:
            sc_count = int(df_all["is_scopus_indexed"].sum())
            print(f"   Scopus-indexed: {sc_count:,} ({sc_count/len(df_all)*100:.1f}%)")
        if "scimago_quartile" in df_all.columns:
            try:
                print(
                    f"   SCImago AI quartiles: {df_all['scimago_quartile'].value_counts().to_dict()}"
                )
            except Exception:
                pass

        print(f"\nSTRICT Dataset (AI Q1-{quartile_threshold}):")
        print(f"   Total works: {len(df_strict):,}")
        if "publication_year" in df_strict.columns:
            try:
                print(
                    f"   Publication years: {df_strict['publication_year'].value_counts().sort_index().to_dict()}"
                )
            except Exception:
                pass
        print(
            f"   Multi-institution: {int(df_strict['multi_institution'].sum()):,} ({df_strict['multi_institution'].mean()*100:.1f}%)"
        )
        print(
            f"   Multi-country: {int(df_strict['multi_country'].sum()):,} ({df_strict['multi_country'].mean()*100:.1f}%)"
        )
        print(
            f"   Open access: {int(df_strict['open_access_is_oa'].sum()):,} ({df_strict['open_access_is_oa'].mean()*100:.1f}%)"
        )
        print(f"   Average citations: {df_strict['cited_by_count'].mean():.2f}")

    def create_datasets(self, quartile_threshold="Q3"):
        print("=" * 60)
        print("🎯 CREATING ALL AND STRICT DATASETS (AI SCImago)")
        print("=" * 60)

        all_file = os.path.join(self.config.PROCESSED_DATA_PATH, "works_all.csv")
        if not os.path.exists(all_file):
            raise FileNotFoundError(f"ALL dataset not found at {all_file}")

        df_all = pd.read_csv(all_file, low_memory=False)
        print(f"📥 Loaded ALL dataset: {len(df_all)} works")

        df_all_enhanced = self.enhance_all_dataset(df_all)
        print(f"📦 Enhanced rows: {len(df_all_enhanced)}")

        df_strict = self.create_strict_dataset(df_all_enhanced, quartile_threshold)

        self.save_datasets(df_all_enhanced, df_strict, quartile_threshold)

        return df_all_enhanced, df_strict


if __name__ == "__main__":
    print("STARTING FINAL DATASET CREATION WITH AI SCImago 🚀")
    creator = FinalStrictCreator()
    df_all_enhanced, df_strict = creator.create_datasets(quartile_threshold="Q3")
    print("\n" + "=" * 60)
    print("DONE! Datasets created.")
    print("=" * 60)
