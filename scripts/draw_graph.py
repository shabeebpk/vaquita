"""Command-line helper to draw a Phase-3 semantic graph.

Usage:
  python3 scripts/draw_graph.py --job-id 123
  python3 scripts/draw_graph.py --input-file graph.json --out out.png

This script will load a persisted semantic graph (via `get_semantic_graph`) or
load a JSON file containing the Phase-3 dict, convert it to a networkx DiGraph
using the same conversion logic as Phase-4, and draw it using matplotlib.

The layout is deterministic (spring layout with fixed seed). Node color maps to
`cluster_score` when available; otherwise a default color is used.
"""
import argparse
import json
import logging
from typing import Optional

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib

from app.path_reasoning.reasoning import _graph_to_nx
from app.graphs.persistence import get_semantic_graph

logger = logging.getLogger(__name__)


def draw_graph(
        G: nx.DiGraph,
        out_path: Optional[str] = None,
        layout: str = "spring",
        figsize=(12, 8),
        show: bool = True,
):
        """Draw the given networkx DiGraph to screen or file.

        Deterministic layout: spring_layout with fixed seed.
        Node colors reflect `cluster_score` attribute when present.
        Edge labels show either the first predicate or predicate count.
        """
        plt.figure(figsize=figsize)

        # Choose layout deterministically
        if layout == "spring":
                pos = nx.spring_layout(G, seed=42)
        elif layout == "kamada":
                pos = nx.kamada_kawai_layout(G)
        elif layout == "shell":
                pos = nx.shell_layout(G)
        else:
                pos = nx.spring_layout(G, seed=42)

        # Node colors/sizes
        cluster_scores = []
        for n in G.nodes:
                cs = G.nodes[n].get("cluster_score")
                try:
                        cluster_scores.append(float(cs) if cs is not None else None)
                except Exception:
                        cluster_scores.append(None)

        # Map cluster_score to colors if available
        if any(v is not None for v in cluster_scores):
                vals = [v if v is not None else 0.0 for v in cluster_scores]
                cmap = matplotlib.cm.get_cmap("viridis")
                node_colors = [cmap((v - min(vals)) / (max(vals) - min(vals) + 1e-9)) for v in vals]
                node_sizes = [800 + 2200 * (v if v is not None else 0.0) for v in vals]
        else:
                node_colors = "lightblue"
                node_sizes = 800

        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=node_sizes)

        # Draw edges
        nx.draw_networkx_edges(G, pos, arrowstyle="->", arrowsize=12, edge_color="#444444")

        # Labels: use node text (the node id)
        nx.draw_networkx_labels(G, pos, font_size=9)

        # Edge labels: show predicate summary (first predicate or count)
        edge_labels = {}
        for u, v, data in G.edges(data=True):
                preds = data.get("predicates", [])
                if not preds:
                        label = ""
                elif len(preds) == 1:
                        label = preds[0]
                else:
                        label = f"{preds[0]} (+{len(preds)-1})"
                edge_labels[(u, v)] = label

        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_color="#333333", font_size=7)

        plt.axis("off")
        plt.tight_layout()

        if out_path:
                plt.savefig(out_path, dpi=200)
                logger.info(f"Saved graph drawing to {out_path}")
        if show and not out_path:
                plt.show()


def main():
        parser = argparse.ArgumentParser(description="Draw Phase-3 semantic graph")
        parser.add_argument("--job-id", type=int, help="Job ID to load persisted semantic graph")
        parser.add_argument("--input-file", type=str, help="Path to semantic_graph JSON file (alternative to --job-id)")
        parser.add_argument("--out", type=str, help="Output image path (PNG). If omitted, opens interactive window.)")
        parser.add_argument("--layout", type=str, default="spring", choices=["spring", "kamada", "shell"], help="Layout algorithm")
        parser.add_argument("--no-show", dest="show", action="store_false", help="Do not open interactive window")

        args = parser.parse_args()

        semantic_graph = None
        if args.job_id:
                semantic_graph = get_semantic_graph(args.job_id)
                if semantic_graph is None:
                        parser.error(f"No persisted semantic graph found for job {args.job_id}")
        elif args.input_file:
                with open(args.input_file, "r", encoding="utf-8") as fh:
                        semantic_graph = json.load(fh)
        else:
                parser.error("Either --job-id or --input-file must be provided")

        G = _graph_to_nx(semantic_graph)
        draw_graph(G, out_path=args.out, layout=args.layout, show=args.show)


if __name__ == "__main__":
        main()
