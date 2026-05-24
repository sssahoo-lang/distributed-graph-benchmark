/* ECC Final Presentation — navigation + speaker script
   Linthoi Laishram · Sriya Sahoo · Sefunmi Akin-Olukunle
   ENGR-E516 · Spring 2026 · Indiana University Bloomington */

const SCRIPTS = [
  `Good [morning / afternoon] everyone. We're presenting our ECC final project: a Distributed Graph Processing Engine benchmarking study on Apache Spark with GraphX, run on a five-node Jetstream2 cluster. I'm Linthoi Laishram, presenting work done jointly with Sriya Sahoo and Sefunmi Akin-Olukunle for ENGR-E516, Spring 2026 at Indiana University Bloomington. The three of us split every workstream equally — infrastructure, algorithm implementation, the experimental sweep, and analysis. Over the next fifteen minutes I'll walk you through our research question, the system we built, what we measured, and the practical takeaway for engineers deciding whether distributed graph processing is worth the cluster.`,
  `Our question is simple to state but surprisingly hard to answer: where does Apache Spark with GraphX, running on a cloud-provisioned five-node cluster, actually start to help iterative graph analytics — and where does it stop helping? We break this into three sub-questions. First, how does execution time scale as we add workers, from one to eight. Second, how do edge-cut versus vertex-cut partitioning strategies compare. And third, how does graph topology — power-law versus near-uniform — interact with both. Most published GraphX work measures gains on billion-edge graphs. We deliberately go the other direction and characterize the lower bound: where does distribution stop paying off.`,
  `Following early feedback from our professor, we anchored each algorithm in a concrete real-world application rather than abstract benchmarking. PageRank is the original Google algorithm for web page ranking — a page is important if other important pages link to it. We run it on the web-Google hyperlink graph, a real subgraph of the web. Single-Source Shortest Path is the foundation of road routing — given one starting depot, what's the shortest distance to every other intersection? We run it on roadNet-CA, the California road intersection network. So our two algorithm-dataset pairs map cleanly to two real engineering decisions an engineer might face.`,
  `The two datasets were chosen specifically to test contrasting graph topologies. web-Google has roughly 875 thousand vertices and 5.1 million edges with a power-law degree distribution: a few hub pages with many incoming links, most pages with few. roadNet-CA has 1.97 million vertices and 5.5 million edges, but a near-uniform degree distribution — every intersection has two to four neighbors — and a very long diameter of 726. That's characteristic of spatial networks. The critical observation, which becomes important shortly, is that both datasets fit comfortably in a single worker's 30 gigabytes of RAM. They probably fit in a modern CPU's L3 cache during execution.`,
  `Our cluster runs on five Jetstream2 virtual machines, provisioned through the NSF ACCESS allocation. The master node has sixteen vCPUs and sixty gigabytes of RAM, and runs the Spark master plus an NFS export at /srv/sparkdata. The four worker nodes each have eight vCPUs and thirty gigabytes of RAM. All five run Ubuntu 22.04, Docker 29, and Java 11. Workers mount the master's NFS share for both the dataset edge lists and the assembly JAR. We deliberately avoided standing up a full HDFS cluster — our datasets are small enough that read-once-and-cache is more than sufficient. For the eight-worker scaling point, each physical node runs two co-located Spark workers with four cores each.`,
  `The benchmark harness orchestrates a 160-cell sweep. Each cell is a tuple of algorithm, dataset, partitioner, worker count, and trial. Two algorithms times two datasets times two partitioners times four worker counts times five trials gives us 160 cluster runs. Five trials lets us compute proper 95 percent Student-t confidence intervals throughout. The harness is fully idempotent — each completed cell drops a metrics JSON file, so reruns skip what's already done. Total wall clock for the new 64-cell delta was about three hours and forty-five minutes. Our custom SparkListener writes a 1-kilobyte JSON per run and uses Welford's online variance algorithm so the listener itself doesn't OOM on the 155-thousand-task SSSP runs.`,
  `PageRank is implemented using GraphX's built-in staticPageRank, configured for twenty power iterations with a reset probability of 0.15 — the standard formulation. We force materialization with a top-five extraction so we get an honest end-to-end timing — and we sum the rank vector as a lightweight checksum. That sum should equal the vertex count, which on web-Google is exactly 875,713 — confirming convergence. We also added an environment-gated rank dump for our convergence study; that's a no-op during the main sweep.`,
  `SSSP is implemented on GraphX's Pregel API, the bulk-synchronous-parallel model from the Pregel paper. We assign unit edge weights, initialize the source vertex to distance zero and all others to positive infinity. Then in each superstep, every vertex applies vprog — take the minimum of the current distance and any incoming message — sendMsg — propagate distance-plus-one along outgoing edges if it would improve the destination — and mergeMsg — combine candidates by taking the minimum. The algorithm terminates when no vertex updates. Critically, on roadNet-CA this requires 726 supersteps because the graph diameter is 726 — and each superstep is a full Spark shuffle. The eight-worker SSSP/roadNet job emits 3,158 stages and over 154,000 tasks.`,
  `This is the single most important finding in the project. We added a single-machine baseline using scipy sparse linear algebra on the master node. Same algorithm — power iteration for PageRank, BFS for SSSP — but in one Python process on one core, no Spark, no cluster. The result: for every algorithm-dataset pair, plain scipy on a single core finishes 25 to 400 times faster than Spark on the cluster. PageRank on web-Google: half a second versus 14 seconds. SSSP on roadNet-CA: under one second versus over six minutes. We are not exaggerating these numbers. They exclude Spark startup time and exclude data load — this is algorithm time only. The cluster, on these graphs, is dramatically slower than one core.`,
  `And it gets worse: adding more workers makes Spark slower, not faster. PageRank achieves only a 1.07× speedup at two workers before declining; at four and eight workers it's below the one-worker baseline. SSSP exhibits monotonic negative scaling from one worker onward, with a 3.2× slowdown at eight workers on roadNet-CA. These curves carry five-trial 95 percent confidence intervals, and the SSSP intervals are disjoint from the no-effect line — so the negative trend is real, not noise. Adding workers should make things faster, or at worst no change. It made things two to three times slower.`,
  `The natural reaction to "Spark is slower" is to assume we configured it wrong. We took that seriously and ran a diagnostic study with extra instrumentation: per-task GC time, shuffle-fetch-wait time, the load-versus-compute split. The results contradict every obvious guess. Garbage collection is under 2.5 percent everywhere. Shuffle-fetch-wait is under 2.7 percent. The slowdown going from one worker to four lives almost entirely in the compute residual. And the smoking gun is the task count: SSSP on roadNet-CA at four workers is 154,926 tasks across 3,158 stages — a 5-million-edge graph chopped into 155 thousand tasks. Per-task fixed overhead dominates the per-vertex work, which is a single "min" operation.`,
  `On RQ2 — the partitioner question — we expected EdgePartition2D, the vertex-cut approximation, to win on the power-law web-Google graph. That's the headline claim of the PowerGraph paper. Our measurements show 1D and 2D within 2 percent on wall time and within 50 percent on shuffle bytes, with no consistent winner. PowerGraph showed gains on graphs with hub vertices of degree ten thousand or higher. web-Google's max out-degree is 456 — power-law in shape but the hubs aren't large enough for 2D's theoretical advantage to manifest. roadNet-CA is near-uniform by construction, so 2D shouldn't help there at all, and doesn't.`,
  `In digging through Spark's defaults, we found one configuration knob that genuinely moves the needle. spark.locality.wait defaults to three seconds, assuming HDFS-style data locality. We're reading from NFS, where locality doesn't exist — every read is a network read. So that three-second wait per task is pure overhead. Setting it to zero on PageRank web-Google at four workers cuts wall time from 37.6 seconds to 25.7 seconds — a 31.7 percent improvement, with disjoint confidence intervals. This is real, not noise. It doesn't change the headline that scipy still wins, but it's a concrete configuration trap to avoid on NFS-backed Spark deployments.`,
  `A finding this dramatic deserves rigorous validity discussion. We named five threats in the midterm and have closed four. Sample size: closed with five trials per cell and 95 percent Student-t confidence intervals on every metric we report. No single-machine baseline: closed with the scipy sparse comparison. PageRank convergence: we extended to thirty iterations and confirmed top-1 ordering converges by iteration ten. The 2D partitioner null result we frame as scope-bound rather than as disproof of PowerGraph. The eight-worker configuration we explicitly reframe as supplementary executor density, not a horizontal-scaling point.`,
  `To summarize: at sub-RAM graph scale, around 5 million edges, Spark's framework overhead is 25 to 400 times the single-machine algorithm time, and adding workers makes it worse, not better. The slowdown is not garbage collection and not shuffle-fetch-wait — it lives in per-task scheduling overhead that dominates when a small graph is chopped into 150 thousand tasks. The practitioner takeaway is concrete: at our datasets' scale, a single-machine implementation is the correct choice. Spark begins to pay only when the working set exceeds single-node RAM — which our datasets do not.`,
  `[Switch browser tab to dashboard.] This is our interactive Streamlit dashboard, deployed on the master node at port 8501. It lets anyone click through every cell of the 160-cell sweep without reading code. On the left, I can pick algorithm, dataset, partitioner, and worker count. On the right, you see the wall-time decomposition with confidence intervals, side-by-side 1D versus 2D shuffle bytes and load imbalance, and the speedup curves — all rendered live from the underlying summary CSV. The dashboard is part of our reproducibility story: anyone with our codebase, the SNAP datasets, and a Jetstream2 allocation can rerun the entire pipeline with one command.`,
  `These are the primary references we drew on — the foundational Pregel paper, PowerGraph, both GraphX papers, the original PageRank tech report, Valiant's BSP model, and Leskovec and Krevl's SNAP datasets. The work was supported by an NSF ACCESS allocation on Jetstream2. Thank you — questions welcome.`
];

// ── State ────────────────────────────────────────────────────────────
const slides       = document.querySelectorAll('.slide');
const totalSlides  = slides.length;
let currentSlide   = 0;
let overviewOpen   = false;

// ── DOM refs ─────────────────────────────────────────────────────────
const counter     = document.getElementById('counter');
const progress    = document.getElementById('progress');
const scriptPanel = document.getElementById('scriptPanel');
const scriptBody  = document.getElementById('scriptBody');
const scriptNum   = document.getElementById('scriptNum');
const helpOverlay = document.getElementById('helpOverlay');
const overview    = document.getElementById('overview');
const ovGrid      = document.getElementById('ovGrid');

// ── Core navigation ──────────────────────────────────────────────────
function show(idx) {
  if (idx < 0) idx = 0;
  if (idx >= totalSlides) idx = totalSlides - 1;
  slides[currentSlide].classList.remove('active');
  slides[idx].classList.add('active');
  currentSlide = idx;
  counter.textContent = `${idx + 1} / ${totalSlides}`;
  progress.style.width = `${((idx + 1) / totalSlides) * 100}%`;
  scriptNum.textContent = idx + 1;
  scriptBody.innerHTML = SCRIPTS[idx] || '<em style="opacity:.5">No script for this slide.</em>';
  scriptPanel.scrollTop = 0;
  // highlight active thumb in overview
  document.querySelectorAll('.ov-thumb').forEach((t, i) => {
    t.classList.toggle('active', i === idx);
  });
}

function next() { show(currentSlide + 1); }
function prev() { show(currentSlide - 1); }

document.getElementById('next').addEventListener('click', next);
document.getElementById('prev').addEventListener('click', prev);

// ── Overview panel ───────────────────────────────────────────────────
function buildOverview() {
  ovGrid.innerHTML = '';
  slides.forEach((sl, i) => {
    const thumb = document.createElement('button');
    thumb.className = 'ov-thumb' + (i === currentSlide ? ' active' : '');
    thumb.title = `Slide ${i + 1}`;
    // clone the slide tag text for label
    const tag = sl.querySelector('.slide-tag, .title-course');
    const label = tag ? tag.textContent.trim() : `Slide ${i + 1}`;
    thumb.innerHTML = `<span class="ov-num">${i + 1}</span><span class="ov-label">${label}</span>`;
    thumb.addEventListener('click', () => { show(i); closeOverview(); });
    ovGrid.appendChild(thumb);
  });
}

function openOverview() {
  buildOverview();
  overview.classList.add('show');
  overviewOpen = true;
}
function closeOverview() {
  overview.classList.remove('show');
  overviewOpen = false;
}

document.getElementById('ovBtn').addEventListener('click', () => {
  overviewOpen ? closeOverview() : openOverview();
});
overview.addEventListener('click', (e) => {
  if (e.target === overview) closeOverview();
});

// ── Touch / swipe ────────────────────────────────────────────────────
let touchStartX = null;
const deck = document.getElementById('deck');
deck.addEventListener('touchstart', (e) => { touchStartX = e.touches[0].clientX; }, { passive: true });
deck.addEventListener('touchend', (e) => {
  if (touchStartX === null) return;
  const dx = e.changedTouches[0].clientX - touchStartX;
  if (Math.abs(dx) > 50) { dx < 0 ? next() : prev(); }
  touchStartX = null;
});

// ── Keyboard ─────────────────────────────────────────────────────────
document.addEventListener('keydown', (e) => {
  if (overviewOpen) {
    if (e.key === 'Escape' || e.key === 'o' || e.key === 'O') { e.preventDefault(); closeOverview(); }
    return;
  }
  if (e.key === 'ArrowRight' || e.key === ' ' || e.key === 'PageDown') {
    e.preventDefault(); next();
  } else if (e.key === 'ArrowLeft' || e.key === 'PageUp') {
    e.preventDefault(); prev();
  } else if (e.key === 'Home') {
    e.preventDefault(); show(0);
  } else if (e.key === 'End') {
    e.preventDefault(); show(totalSlides - 1);
  } else if (e.key === 's' || e.key === 'S') {
    scriptPanel.classList.toggle('show');
  } else if (e.key === 'o' || e.key === 'O') {
    openOverview();
  } else if (e.key === 'f' || e.key === 'F') {
    if (!document.fullscreenElement) {
      document.documentElement.requestFullscreen?.();
    } else {
      document.exitFullscreen?.();
    }
  } else if (e.key === '?' || (e.shiftKey && e.key === '/')) {
    helpOverlay.classList.toggle('show');
  } else if (e.key === 'Escape') {
    helpOverlay.classList.remove('show');
    scriptPanel.classList.remove('show');
  } else if (/^[0-9]$/.test(e.key)) {
    const n = e.key === '0' ? 10 : parseInt(e.key, 10);
    if (n >= 1 && n <= totalSlides) show(n - 1);
  }
});

helpOverlay.addEventListener('click', () => helpOverlay.classList.remove('show'));

// ── Init ─────────────────────────────────────────────────────────────
show(0);

const params = new URLSearchParams(location.search);
const initial = parseInt(params.get('slide'), 10);
if (!isNaN(initial) && initial >= 1 && initial <= totalSlides) {
  show(initial - 1);
}
