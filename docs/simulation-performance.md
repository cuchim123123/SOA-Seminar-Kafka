# Simulation rendering verification — 2026-11-03

The final UI reduces measured React rendering work and improves typical frame pacing relative to the pre-rendering-change commit. It does not eliminate every hitch or establish a production FPS guarantee.

The comparison used the Codex in-app browser at 1280×900, Vite development builds, React StrictMode, the Full Enterprise scenario, 100 synthetic U001 events at 1× speed, builder closed and evidence collapsed. Each repeat reset the run, waited one second for warm-up, then sampled five seconds. Three valid repeats were collected per revision. Baseline: `dc97333`; candidate: the final S6 changes based on `1072fe3`. No real Purchase events or broker workload were generated.

| Metric, independent median across repeats | Baseline | Final UI |
|---|---:|---:|
| React render duration summed over five seconds | 3,346.5 ms | 1,338.1 ms |
| React render duration per commit, p50 | 3.7 ms | 0.7 ms |
| React render duration per commit, p95 | 6.0 ms | 6.3 ms |
| requestAnimationFrame callbacks per second | 95.8 | 127.6 |
| Frame interval, p50 | 7.0 ms | 7.0 ms |
| Frame interval, p95 | 20.8 ms | 13.9 ms |
| React commits per five-second sample | 925 | 826 |

React render duration decreased approximately 60%. Median p95 frame interval improved approximately 33%. This high-refresh environment is not a fixed 60 Hz device; callback rate is not a count of unique painted frames. React's `actualDuration` measures instrumented render work, not total browser CPU, layout, raster or GPU time.

The final UI's worst observed interval was **62.5 ms**, versus **41.9 ms** for the baseline. Each revision had one interval above 32 ms across its three accepted samples. Per-commit p95 render time was slightly higher in the final version. The evidence supports lower aggregate work and better typical pacing, not a claim that every frame or tail metric improved.

Raw rounded samples, protocol and independent medians are in [simulation-performance-results.json](simulation-performance-results.json). A calibration with mismatched panel state and samples accidentally instrumenting the unused `main.tsx` entry were discarded. The actual entry is `src/entry.tsx`. The third current repeat was collected after a browser-automation reconnect. Host background load was not controlled, Docker services remained running, and the sample is small. This compares the complete UI revision rather than isolating a single algorithm. The baseline also emitted ResizeObserver notices during layout changes.

Temporary instrumentation wrapped the app in React Profiler, accumulated `actualDuration` only within the sample window, and sampled native RAF timestamps in that same window. A temporary UI action reset the engine, selected 1× speed, queued the 100-event burst and started the sample. The temporary module/button/entry changes were removed afterward. The baseline server on 5174 was stopped. The diagnostic checkout and source backups were retained under `.git/simulation-diagnostics/` after automatic review blocked junction deletion; they are outside application sources and quality-check scans.

For a repeat, use an isolated checkout of the baseline and the final revision, the protocol above and a temporary root Profiler/native RAF collector. Keep panel visibility, viewport and active-tab visibility the same. Discard warm-up and samples without an active profiler; preserve all accepted repetitions, including outliers. Run without test/build jobs competing for CPU. Recheck production builds and representative presentation hardware before setting an FPS service level.

Regression evidence complements the profile: 600 idle ticks produce no model-version changes and reuse all node snapshots; changing one sink counter replaces only that node's view; particle membership publishes immediately on the frame subscription; inactive edges retain stable snapshots; and a 1,000-message workload completes without losing records while history remains bounded. Very short stages can still complete between physical frames at high speed. Use slower playback or Step to inspect every logical boundary.
