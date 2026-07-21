#!/bin/bash

variants=("research_denormalized_opt" "research_intermediate_opt" "research_transitive_opt" "research_uplift_opt")

for variant in "${variants[@]}"; do
  echo "=== Running variant: $variant ==="
  
  # Run Memgraph
  echo "--- Running Memgraph for $variant ---"
  py -3.13 benchmark.py vendor-docker \
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
