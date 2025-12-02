import pandas as pd
import os
from config import Config


def write_pajek(nodes, edges, filename):
    """
    Write Pajek .net file.
    nodes: dict {node_id: label}
    edges: list of (source, target, weight)
    """
    with open(filename, "w", encoding="utf-8") as f:
        # write nodes
        f.write(f"*Vertices {len(nodes)}\n")
        for i, (node_id, label) in enumerate(nodes.items(), start=1):
            # remove any internal double quotes from labels
            safe_label = str(label).replace('"', "")
            f.write(f'{i} "{safe_label}"\n')

        # map id → index
        node_index = {node_id: i for i, node_id in enumerate(nodes.keys(), start=1)}

        # write edges
        f.write("*Edges\n")
        for src, tgt, w in edges:
            if src in node_index and tgt in node_index:
                f.write(f"{node_index[src]} {node_index[tgt]} {w}\n")


if __name__ == "__main__":
    cfg = Config()

    # === AUTHORS NETWORK ===
    print("Converting AUTHOR network to Pajek...")
    edges_auth = pd.read_csv(os.path.join(cfg.PROCESSED_DATA_PATH, "edges_coauthorship_authors.csv"))
    nodes_auth = pd.read_csv(os.path.join(cfg.PROCESSED_DATA_PATH, "authors_strict.csv"))

    author_nodes = dict(zip(nodes_auth["author_id"], nodes_auth["author_name"]))
    author_edges = list(
        zip(
            edges_auth["from_author_id"],
            edges_auth["to_author_id"],
            edges_auth["weight"],
        )
    )

    write_pajek(
        author_nodes,
        author_edges,
        os.path.join(cfg.PROCESSED_DATA_PATH, "authors.net"),
    )
    print("✅ authors.net created")

    # === INSTITUTIONS NETWORK ===
    print("Converting INSTITUTION network to Pajek...")
    edges_inst = pd.read_csv(os.path.join(cfg.PROCESSED_DATA_PATH, "edges_collaboration_institutions.csv"))
    nodes_inst = pd.read_csv(os.path.join(cfg.PROCESSED_DATA_PATH, "institutions_strict.csv"))

    inst_nodes = dict(zip(nodes_inst["institution_id"], nodes_inst["institution_name"]))
    inst_edges = list(
        zip(
            edges_inst["from_institution_id"],
            edges_inst["to_institution_id"],
            edges_inst["weight"],
        )
    )

    write_pajek(
        inst_nodes,
        inst_edges,
        os.path.join(cfg.PROCESSED_DATA_PATH, "institutions.net"),
    )
    print("✅ institutions.net created")

    # === CONCEPT NETWORK ===
    print("Converting CONCEPT network to Pajek...")
    edges_con = pd.read_csv(os.path.join(cfg.PROCESSED_DATA_PATH, "edges_cooccurrence_concepts.csv"))

    concepts = set(edges_con["from_concept"]).union(set(edges_con["to_concept"]))
    concept_nodes = {c: c for c in concepts}

    concept_edges = list(
        zip(
            edges_con["from_concept"],
            edges_con["to_concept"],
            edges_con["weight"],
        )
    )

    write_pajek(
        concept_nodes,
        concept_edges,
        os.path.join(cfg.PROCESSED_DATA_PATH, "concepts.net"),
    )
    print("✅ concepts.net created")

    print("🎉 All Pajek network files created!")

