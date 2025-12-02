import os
import pandas as pd
from collections import Counter

from config import Config


def top_n_from_column(series, n=5, sep=";"):
    """
    Given a Series of strings (e.g. concepts_list or source_name),
    return a string with the top-n items and their counts.
    Example: 'AI (10); Deep learning (8); NLP (5)'
    """
    counter = Counter()
    for val in series.dropna():
        # concepts_list uses ';', source_name is a single value
        if sep:
            items = [x.strip() for x in str(val).split(sep)]
        else:
            items = [str(val).strip()]
        for item in items:
            if item:
                counter[item] += 1
    if not counter:
        return ""
    return "; ".join([f"{name} ({cnt})" for name, cnt in counter.most_common(n)])


def build_institution_table(df_strict: pd.DataFrame) -> pd.DataFrame:
    """
    Build an institution-level table from the STRICT+RAW merged dataset.
    One row per institution with aggregated statistics.
    """

    required_cols = [
        "work_id",
        "institutions",
        "publication_year",
        "cited_by_count",
        "open_access_is_oa",
        "multi_institution",
        "multi_country",
        "concepts_list",
        "source_name",
    ]
    missing = [c for c in required_cols if c not in df_strict.columns]
    if missing:
        raise KeyError(f"STRICT dataset is missing required columns: {missing}")

    # 1) Explode to (work, institution) level
    inst_rows = []
    for _, row in df_strict.iterrows():
        inst_list = row["institutions"]
        if not isinstance(inst_list, list):
            continue

        for inst in inst_list:
            if not isinstance(inst, dict):
                continue

            inst_rows.append(
                {
                    "institution_id": inst.get("institution_id"),
                    "institution_name": inst.get("institution_name"),
                    "country_code": inst.get("country_code"),
                    "type": inst.get("institution_type"),
                    "work_id": row["work_id"],
                    "publication_year": row["publication_year"],
                    "cited_by_count": row["cited_by_count"],
                    "open_access_is_oa": bool(row["open_access_is_oa"]),
                    "multi_institution": bool(row["multi_institution"]),
                    "multi_country": bool(row["multi_country"]),
                    "concepts_list": row["concepts_list"],
                    "source_name": row["source_name"],
                }
            )

    inst_df = pd.DataFrame(inst_rows)

    # Drop institutions with no ID (optional)
    inst_df = inst_df[inst_df["institution_id"].notna()]

    # 2) Basic aggregations per institution
    group = inst_df.groupby("institution_id", dropna=True)

    base = group.agg(
        institution_name=("institution_name", "first"),
        country_code=("country_code", "first"),
        type=("type", "first"),
        works_count=("work_id", "nunique"),
        citations_sum=("cited_by_count", "sum"),
        oa_share=("open_access_is_oa", "mean"),
        collaboration_rate=("multi_institution", "mean"),
        multi_country_rate=("multi_country", "mean"),
    ).reset_index()

    # 3) Publications by year since 2020 (pubs_2020, pubs_2021, ...)
    pubs_year = (
        inst_df[["institution_id", "publication_year", "work_id"]]
        .dropna(subset=["publication_year"])
        .copy()
    )
    pubs_year["publication_year"] = pd.to_numeric(
        pubs_year["publication_year"], errors="coerce"
    ).astype("Int64")

    pubs_pivot = (
        pubs_year.dropna(subset=["publication_year"])
        .groupby(["institution_id", "publication_year"])["work_id"]
        .nunique()
        .unstack(fill_value=0)
    )

    pubs_pivot.columns = [f"pubs_{int(c)}" for c in pubs_pivot.columns]

    base = base.merge(
        pubs_pivot,
        how="left",
        left_on="institution_id",
        right_index=True,
    )

    # 4) Top 5 concepts and sources per institution
    top_concepts = (
        group["concepts_list"]
        .apply(lambda s: top_n_from_column(s, n=5, sep=";"))
        .rename("top_5_concepts")
    )
    top_sources = (
        group["source_name"]
        .apply(lambda s: top_n_from_column(s, n=5, sep=None))
        .rename("top_5_sources")
    )

    base = (
        base.merge(top_concepts, on="institution_id", how="left")
        .merge(top_sources, on="institution_id", how="left")
    )

    # 5) Convert rates to percentages
    for col in ["oa_share", "collaboration_rate", "multi_country_rate"]:
        base[col] = (base[col] * 100).round(1)

    base = base.sort_values("works_count", ascending=False).reset_index(drop=True)

    return base


if __name__ == "__main__":
    cfg = Config()

    # 1) Load STRICT enhanced dataset (Q1–Q3)
    strict_path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.json")
    print(f"Loading STRICT dataset from: {strict_path}")
    df_strict = pd.read_json(strict_path)

    # 2) Load original works_all.json (this has the institutions list)
    raw_path = os.path.join(cfg.PROCESSED_DATA_PATH, "works_all.json")
    print(f"Loading RAW works with institutions from: {raw_path}")
    df_raw = pd.read_json(raw_path)

    if "institutions" not in df_raw.columns:
        raise KeyError("works_all.json does not contain 'institutions' column")

    # 3) Merge STRICT with institutions information
    df_merged = df_strict.merge(
        df_raw[["work_id", "institutions"]],
        on="work_id",
        how="left",
    )

    print(f"Merged STRICT + institutions: {len(df_merged)} rows")

    # 4) Build institution-level table
    institutions_df = build_institution_table(df_merged)

    # 5) Save result as CSV + JSON
    out_csv = os.path.join(cfg.PROCESSED_DATA_PATH, "institutions_strict.csv")
    out_json = os.path.join(cfg.PROCESSED_DATA_PATH, "institutions_strict.json")

    institutions_df.to_csv(out_csv, index=False)
    institutions_df.to_json(out_json, orient="records", indent=2, force_ascii=False)

    print(f"✅ Saved institutions table to: {out_csv}")
    print(f"✅ Saved institutions table to: {out_json}")
