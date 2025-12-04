# core_part1_label_works.py
#
# Goal:
#  - Use SCImago AI subject category files (Eastern + Western Europe, 2020–2024)
#    to define which journals are "core"
#  - Label each work in the STRICT dataset as core / noncore
#  - Save a new STRICT dataset with a column "core_status"

import os
import pandas as pd
from config import Config


def load_strict_dataset(cfg: Config) -> pd.DataFrame:
    """Load STRICT dataset (Q3) from processed folder."""
    strict_path = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_qQ3.csv")
    if not os.path.exists(strict_path):
        raise FileNotFoundError(f"STRICT dataset not found at: {strict_path}")

    df = pd.read_csv(strict_path, low_memory=False)
    print(f"📥 Loaded STRICT dataset: {len(df)} rows")

    if "source_name" not in df.columns:
        raise KeyError(
            "Column 'source_name' not found in STRICT dataset. "
            "Check that Strict_dataset_creator.py created this column."
        )

    return df


def build_core_journal_list(cfg: Config) -> pd.DataFrame:
    """
    Build a list of core journals from SCImago AI subject category files.

    Core definition here:
      - Any journal that appears in ANY of the SCImago AI files
        (Artificial Intelligence, Eastern or Western Europe, 2020–2024).
    """

    scimago_dir = os.path.join(cfg.EXTERNAL_DATA_PATH, "SCImago")
    if not os.path.isdir(scimago_dir):
        raise FileNotFoundError(f"SCImago folder not found at: {scimago_dir}")

    files = sorted(
        f for f in os.listdir(scimago_dir)
        if "Subject Category - Artificial Intelligence" in f and f.lower().endswith(".csv")
    )

    if not files:
        raise FileNotFoundError(
            f"No SCImago AI csv files found in: {scimago_dir}. "
            "Expected files like 'scimagojr 2024  Subject Category - Artificial Intelligence_Western Europe.csv'."
        )

    core_frames = []

    for fname in files:
        path = os.path.join(scimago_dir, fname)
        print(f"🔹 Reading SCImago file: {fname}")
        # SCImago uses ';' as separator
        df_sc = pd.read_csv(path, sep=";")

        # SCImago AI exports usually have a 'Title' column for the journal name
        if "Title" in df_sc.columns:
            df_sc["source_norm"] = (
                df_sc["Title"]
                .astype(str)
                .str.lower()
                .str.strip()
            )
        elif "Source title" in df_sc.columns:
            df_sc["source_norm"] = (
                df_sc["Source title"]
                .astype(str)
                .str.lower()
                .str.strip()
            )
        else:
            print(f"⚠️  Could not find 'Title' or 'Source title' in {fname}, skipping.")
            continue

        core_frames.append(df_sc[["source_norm"]])

    if not core_frames:
        raise RuntimeError("No valid SCImago files could be parsed for core journals.")

    core = pd.concat(core_frames, ignore_index=True).drop_duplicates()
    core["core_journal"] = True

    print(f"✅ Core journals identified from SCImago: {len(core)} unique sources")
    return core


def label_core_status(df_strict: pd.DataFrame, core_journals: pd.DataFrame) -> pd.DataFrame:
    """
    Merge STRICT dataset with core_journals and add 'core_status' column.
    """

    # Normalize source names in STRICT dataset
    df = df_strict.copy()
    df["source_norm"] = (
        df["source_name"]
        .astype(str)
        .str.lower()
        .str.strip()
    )

    # Merge with core journal list
    df = df.merge(
        core_journals[["source_norm", "core_journal"]],
        on="source_norm",
        how="left",
    )

    # Anything that matched gets core_status='core', else 'noncore'
    df["core_status"] = df["core_journal"].apply(
        lambda x: "core" if x is True else "noncore"
    )

    # Clean up helper column
    df.drop(columns=["core_journal"], inplace=True)

    # Quick summary
    print("📊 Core status value counts:")
    print(df["core_status"].value_counts(dropna=False))

    return df


def save_labeled_strict(cfg: Config, df_labeled: pd.DataFrame) -> None:
    """Save the STRICT dataset with core_status to CSV and JSON."""
    out_csv = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_with_core.csv")
    out_json = os.path.join(cfg.PROCESSED_DATA_PATH, "dataset_strict_with_core.json")

    df_labeled.to_csv(out_csv, index=False)
    df_labeled.to_json(out_json, orient="records", indent=2, force_ascii=False)

    print(f"💾 Saved STRICT+core CSV:   {out_csv}")
    print(f"💾 Saved STRICT+core JSON:  {out_json}")


if __name__ == "__main__":
    print("=" * 60)
    print("🏷  CORE LABELING OF STRICT DATASET (SCImago AI-based)")
    print("=" * 60)

    cfg = Config()

    # 1) Load STRICT dataset
    df_strict = load_strict_dataset(cfg)

    # 2) Build core journal list from SCImago AI files
    core_journals = build_core_journal_list(cfg)

    # 3) Label works as core / noncore
    df_labeled = label_core_status(df_strict, core_journals)

    # 4) Save outputs
    save_labeled_strict(cfg, df_labeled)

    print("\n✅ Core labeling done.")
    print("=" * 60)

