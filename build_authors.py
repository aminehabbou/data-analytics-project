import os
import pandas as pd
from collections import Counter, defaultdict

from config import Config


def top_n_from_column(series, n=5, sep=";"):
    """
    Given a Series of strings (e.g. concepts_list),
    return a string with the top-n items and their counts.
    Example: 'AI (10); Deep learning (8); NLP (5)'
    """
    counter = Counter()
    for val in series.dropna():
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


def build_author_table(df: pd.DataFrame, key_threshold: int = 3) -> pd.DataFrame:
    """
    Build an author-level table from the STRICT+RAW merged dataset.
    One row per author with aggregated statistics.
    """

    required_cols = [
        "work_id",
        "authors",
        "publication_year",
        "cited_by_count",
        "open_access_is_oa",
        "multi_institution",
        "concepts_list",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"STRICT dataset is missing required columns: {missing}")

    # 1) Explode to (work, author) level
    author_rows = []
    for _, row in df.iterrows():
        auth_list = row["authors"]
        if not isinstance(auth_list, list):
            continue

        for auth in auth_list:
            if not isinstance(auth, dict):
                continue

            author_rows.append(
                {
                    "author_id": auth.get("author_id"),
                    "author_name": auth.get("author_name"),
                    "orcid": auth.get("orcid"),
                    "work_id": row["work_id"],
                    "publication_year": row["publication_year"],
                    "cited_by_count": row["cited_by_count"],
                    "open_access_is_oa": bool(row["open_access_is_oa"]),
                    "multi_institution": bool(row["multi_institution"]),
                    "concepts_list": row["concepts_list"],
                }
            )

    author_df = pd.DataFrame(author_rows)

    # Drop authors with no id (optional)
    author_df = author_df[author_df["author_id"].notna()]

    # 2) Basic aggregations per author
    group = author_df.groupby("author_id", dropna=True)

    base = group.agg(
        author_name=("author_name", "first"),
        orcid=("orcid", "first"),
        works_count=("work_id", "nunique"),
        citations_sum=("cited_by_count", "sum"),
        oa_share=("open_access_is_oa", "mean"),
        multi_institution_share=("multi_institution", "mean"),
    ).reset_index()

    # 3) Papers since 2020 (flexible if you ever change time window)
    pubs_year = (
        author_df[["author_id", "publication_year", "work_id"]]
        .dropna(subset=["publication_year"])
        .copy()
    )
    pubs_year["publication_year"] = pd.to_numeric(
        pubs_year["publication_year"], errors="coerce"
    ).astype("Int64")

    pubs_2020p = (
        pubs_year[pubs_year["publication_year"] >= 2020]
        .groupby("author_id")["work_id"]
        .nunique()
    ).rename("papers_since_2020")

    base = base.merge(pubs_2020p, on="author_id", how="left")
    base["papers_since_2020"] = base["papers_since_2020"].fillna(0).astype(int)

    # 4) Coauthors count (distinct)
    coauthor_sets = defaultdict(set)

    # build mapping: work_id -> list of author_ids
    work_authors = (
        author_df.groupby("work_id")["author_id"]
        .apply(lambda s: list({a for a in s if pd.notna(a)}))
        .to_dict()
    )

    for work_id, authors in work_authors.items():
        for a in authors:
            others = [x for x in authors if x != a]
            for o in others:
                coauthor_sets[a].add(o)

    coauthor_count_series = (
        pd.Series({aid: len(coauthors) for aid, coauthors in coauthor_sets.items()})
        .rename("coauthors_count")
    )

    base = base.merge(
        coauthor_count_series, left_on="author_id", right_index=True, how="left"
    )
    base["coauthors_count"] = base["coauthors_count"].fillna(0).astype(int)

    # 5) Top concepts per author
    top_concepts = (
        group["concepts_list"]
        .apply(lambda s: top_n_from_column(s, n=5, sep=";"))
        .rename("top_concepts")
    )

    base = base.merge(top_concepts, on="author_id", how="left")

    # 6) Convert rates to percentages
    for col in ["oa_share", "multi_institution_share"]:
        base[col] = (base[col] * 100).round(1)

    # 7) Key researcher flag
    base["is_key_researcher"] = base["papers_since_2020"] >= key_threshold

    # Sort by works_count descending
    base = base.sort_values("works_count", ascending=False).reset_index(drop=True)

    return base


if __name__ == "__main__":
    cfg = Config()

    # 1) Load STRICT enhanced dataset (Q1–Q3)
    strict_path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.json")
    print(f"Loading STRICT dataset from: {strict_path}")
    df_strict = pd.read_json(strict_path)

    # 2) Load original works_all.json (this has the authors list)
    raw_path = os.path.join(cfg.PROCESSED_DATA_PATH, "works_all.json")
    print(f"Loading RAW works with authors from: {raw_path}")
    df_raw = pd.read_json(raw_path)

    if "authors" not in df_raw.columns:
        raise KeyError("works_all.json does not contain 'authors' column")

    # 3) Merge STRICT with authors information
    df_merged = df_strict.merge(
        df_raw[["work_id", "authors"]],
        on="work_id",
        how="left",
    )

    print(f"Merged STRICT + authors: {len(df_merged)} rows")

    # 4) Build author-level table (key researchers = ≥3 papers)
    authors_df = build_author_table(df_merged, key_threshold=3)

    # 5) Save result as CSV + JSON
    out_csv = os.path.join(cfg.PROCESSED_DATA_PATH, "authors_strict.csv")
    out_json = os.path.join(cfg.PROCESSED_DATA_PATH, "authors_strict.json")

    authors_df.to_csv(out_csv, index=False)
    authors_df.to_json(out_json, orient="records", indent=2, force_ascii=False)

    print(f"✅ Saved authors table to: {out_csv}")
    print(f"✅ Saved authors table to: {out_json}")
