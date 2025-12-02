import os
import pandas as pd
from collections import Counter
from itertools import combinations

from config import Config


def build_author_edges(df_merged):
    """
    Build co-authorship edges:
    from_author_id, to_author_id, weight (# shared works)
    """
    edges = Counter()

    for _, row in df_merged.iterrows():
        authors = row["authors"]
        if not isinstance(authors, list):
            continue

        # unique author IDs for this work
        author_ids = {
            a.get("author_id")
            for a in authors
            if isinstance(a, dict) and a.get("author_id") is not None
        }

        author_ids = [a for a in author_ids if pd.notna(a)]
        if len(author_ids) < 2:
            continue

        # all pairs on this work
        for a1, a2 in combinations(sorted(author_ids), 2):
            edges[(a1, a2)] += 1

    rows = []
    for (a1, a2), w in edges.items():
        rows.append(
            {
                "from_author_id": a1,
                "to_author_id": a2,
                "weight": int(w),
            }
        )

    return pd.DataFrame(rows)


def build_institution_edges(df_merged):
    """
    Build institution collaboration edges:
    from_institution_id, to_institution_id, weight (# shared works)
    """
    edges = Counter()

    for _, row in df_merged.iterrows():
        insts = row["institutions"]
        if not isinstance(insts, list):
            continue

        inst_ids = {
            i.get("institution_id")
            for i in insts
            if isinstance(i, dict) and i.get("institution_id") is not None
        }

        inst_ids = [i for i in inst_ids if pd.notna(i)]
        if len(inst_ids) < 2:
            continue

        for i1, i2 in combinations(sorted(inst_ids), 2):
            edges[(i1, i2)] += 1

    rows = []
    for (i1, i2), w in edges.items():
        rows.append(
            {
                "from_institution_id": i1,
                "to_institution_id": i2,
                "weight": int(w),
            }
        )

    return pd.DataFrame(rows)


def build_concept_edges(df_strict):
    """
    Build concept co-occurrence edges:
    from_concept, to_concept, weight (# shared works)
    using concepts_list in STRICT (string ';' separated)
    """
    edges = Counter()

    for _, row in df_strict.iterrows():
        concepts_str = row.get("concepts_list", "")
        if not isinstance(concepts_str, str) or not concepts_str.strip():
            continue

        concepts = {
            c.strip()
            for c in concepts_str.split(";")
            if c.strip()
        }

        if len(concepts) < 2:
            continue

        for c1, c2 in combinations(sorted(concepts), 2):
            edges[(c1, c2)] += 1

    rows = []
    for (c1, c2), w in edges.items():
        rows.append(
            {
                "from_concept": c1,
                "to_concept": c2,
                "weight": int(w),
            }
        )

    return pd.DataFrame(rows)


if __name__ == "__main__":
    cfg = Config()

    # 1) Load STRICT enhanced dataset
    strict_path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.json")
    print(f"Loading STRICT dataset from: {strict_path}")
    df_strict = pd.read_json(strict_path)

    # 2) Load original works_all.json (authors + institutions)
    raw_path = os.path.join(cfg.PROCESSED_DATA_PATH, "works_all.json")
    print(f"Loading RAW works (authors + institutions) from: {raw_path}")
    df_raw = pd.read_json(raw_path)

    # 3) Merge STRICT with authors + institutions
    needed_cols = ["work_id"]
    if "authors" in df_raw.columns:
        needed_cols.append("authors")
    else:
        raise KeyError("works_all.json does not contain 'authors' column")

    if "institutions" in df_raw.columns:
        needed_cols.append("institutions")
    else:
        raise KeyError("works_all.json does not contain 'institutions' column")

    df_merged = df_strict.merge(
        df_raw[needed_cols],
        on="work_id",
        how="left",
    )

    print(f"Merged STRICT + authors + institutions: {len(df_merged)} rows")

    # === Co-authorship edges (authors) ===
    print("Building co-authorship edges (authors)...")
    author_edges = build_author_edges(df_merged)
    author_edges_csv = os.path.join(
        cfg.PROCESSED_DATA_PATH, "edges_coauthorship_authors.csv"
    )
    author_edges_json = os.path.join(
        cfg.PROCESSED_DATA_PATH, "edges_coauthorship_authors.json"
    )
    author_edges.to_csv(author_edges_csv, index=False)
    author_edges.to_json(author_edges_json, orient="records", indent=2, force_ascii=False)
    print(f"✅ Saved author co-authorship edges to: {author_edges_csv}")
    print(f"✅ Saved author co-authorship edges to: {author_edges_json}")

    # === Collaboration edges (institutions) ===
    print("Building institution collaboration edges...")
    inst_edges = build_institution_edges(df_merged)
    inst_edges_csv = os.path.join(
        cfg.PROCESSED_DATA_PATH, "edges_collaboration_institutions.csv"
    )
    inst_edges_json = os.path.join(
        cfg.PROCESSED_DATA_PATH, "edges_collaboration_institutions.json"
    )
    inst_edges.to_csv(inst_edges_csv, index=False)
    inst_edges.to_json(inst_edges_json, orient="records", indent=2, force_ascii=False)
    print(f"✅ Saved institution collaboration edges to: {inst_edges_csv}")
    print(f"✅ Saved institution collaboration edges to: {inst_edges_json}")

    # === Concept co-occurrence edges ===
    print("Building concept co-occurrence edges...")
    concept_edges = build_concept_edges(df_strict)
    concept_edges_csv = os.path.join(
        cfg.PROCESSED_DATA_PATH, "edges_cooccurrence_concepts.csv"
    )
    concept_edges_json = os.path.join(
        cfg.PROCESSED_DATA_PATH, "edges_cooccurrence_concepts.json"
    )
    concept_edges.to_csv(concept_edges_csv, index=False)
    concept_edges.to_json(concept_edges_json, orient="records", indent=2, force_ascii=False)
    print(f"✅ Saved concept co-occurrence edges to: {concept_edges_csv}")
    print(f"✅ Saved concept co-occurrence edges to: {concept_edges_json}")

    print("🎉 All VOSviewer edge lists created!")
