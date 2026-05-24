# Distributed Graph Benchmark — Spark + GraphX

**ENGR-E516 · Engineering Cloud Computing · Spring 2026**  
Indiana University Bloomington

> *Where does Apache Spark + GraphX on a five-node cloud cluster start to help — or stop helping — iterative graph analytics?*

## Live Presentation

Open `index.html` in any modern browser. No build step, no dependencies, no server required.

**Keyboard shortcuts:**

| Key | Action |
|-----|--------|
| `←` / `→` | Previous / next slide |
| `Space` | Next slide |
| `Home` / `End` | First / last slide |
| `1`–`9` | Jump to slide N |
| `S` | Toggle speaker script panel |
| `O` | Slide overview grid |
| `F` | Fullscreen |
| `?` | Help |
| `Esc` | Close panels |

Touch/swipe supported on mobile.

## Project Summary

We benchmarked Apache Spark 3.5.3 + GraphX on a five-node [Jetstream2](https://jetstream-cloud.org/) cluster (NSF ACCESS allocation), running two graph algorithms across two real-world datasets and two partitioning strategies.

### Algorithms
- **PageRank** — 20-iteration power method, reset probability 0.15 (web page ranking)
- **SSSP** — Pregel bulk-synchronous shortest paths (road network routing)

### Datasets (Stanford SNAP)
| Dataset | Vertices | Edges | Topology |
|---------|----------|-------|----------|
| [web-Google](https://snap.stanford.edu/data/web-Google.html) | 875,713 | 5,105,039 | Power-law |
| [roadNet-CA](https://snap.stanford.edu/data/roadNet-CA.html) | 1,965,206 | 5,533,214 | Near-uniform |

### Experimental sweep
`2 algorithms × 2 datasets × 2 partitioners × 4 worker counts × 5 trials = 160 cluster runs`

Plus 20 scipy single-machine baseline runs and 8 diagnostic cells.

### Key Findings

1. **scipy beats Spark by 25–400×** — plain Python on one core finishes faster than the cluster for every algorithm-dataset pair (algorithm time only, excludes startup).

2. **Negative scaling** — adding workers makes Spark *slower*. SSSP on roadNet-CA shows a 3.2× slowdown at 8 workers vs 1.

3. **The bottleneck is task overhead, not GC or shuffle** — GC < 2.5%, shuffle-wait < 2.7%. A 5M-edge graph chopped into 154,926 tasks means per-task scheduling overhead dominates the trivial per-vertex work.

4. **1D vs 2D partitioning: indistinguishable** — wall times within 2%. PowerGraph's vertex-cut advantage requires hub degrees ≥ 10⁴; web-Google's max out-degree is 456.

5. **One real engineering win** — `spark.locality.wait=0` cuts wall time by 31.7% on NFS-backed deployments (where HDFS-style locality doesn't exist).

**Practitioner takeaway:** Spark pays off only when the working set exceeds single-node RAM. At ~5M edges, use a single-machine implementation.

## Repo Structure

```
distributed-graph-benchmark/
├── index.html          # Presentation deck (open this)
├── styles.css          # Styles
├── script.js           # Navigation + speaker scripts
├── figures/            # All benchmark charts (PNG)
│   ├── speedup_pagerank_web-Google.png
│   ├── speedup_sssp_roadNet-CA.png
│   ├── walltime_pagerank_roadNet-CA.png
│   ├── walltime_sssp_roadNet-CA.png
│   ├── time_decomposition.png
│   ├── shuffle_bytes.png
│   ├── comm_overhead.png
│   └── load_imbalance.png
└── README.md
```

## Team

| Name | Email |
|------|-------|
| Linthoi Laishram | lilaish@iu.edu |
| Sriya Sahoo | sssahoo@iu.edu |
| Sefunmi Akin-Olukunle | sakinolu@iu.edu |

All three authors contributed equally across infrastructure, algorithm implementation, experimental sweep, analysis, and writing.

## References

1. Malewicz et al., *Pregel: A System for Large-Scale Graph Processing*, SIGMOD 2010
2. Gonzalez et al., *PowerGraph: Distributed Graph-Parallel Computation on Natural Graphs*, OSDI 2012
3. Xin et al., *GraphX: A Resilient Distributed Graph System on Spark*, GRADES 2013
4. Gonzalez et al., *GraphX: Graph Processing in a Distributed Dataflow Framework*, OSDI 2014
5. Zaharia et al., *Resilient Distributed Datasets*, NSDI 2012
6. Page et al., *The PageRank Citation Ranking*, Stanford InfoLab 1999
7. Valiant, *A Bridging Model for Parallel Computation*, CACM 1990
8. Leskovec & Krevl, *SNAP Datasets*, snap.stanford.edu/data, 2014
9. Hancock et al., *Jetstream2: Accelerating Cloud Computing via Jetstream*, PEARC 2021

*Supported by an NSF ACCESS allocation on Jetstream2.*
