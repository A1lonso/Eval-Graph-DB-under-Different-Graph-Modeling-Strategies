#!/bin/bash
variantsExtra=("research_uplift_opt")

for variant in "${variantsExtra[@]}"; do
  echo "=== Running variant: $variant ==="
  
  # Run Memgraph
  echo "--- Running Memgraph for $variant ---"
  python3 benchmark.py vendor-docker \
    --vendor-name memgraph-docker \
    benchmarks "${variant}/*/*/*" \
    --export-results "results_${variant}_memgraph_small.json" \
    --no-authorization \
    --num-workers-for-benchmark 4 \
    --single-threaded-runtime-sec 60 \
    --warm-up hot
  
  echo "=== Completed variant: $variant ==="
  echo ""

  # Add a 2-minute pause before the next run
  echo "Sleeping for 2 minutes before next variant..."
  sleep 120
done

echo "All variants completed!"
