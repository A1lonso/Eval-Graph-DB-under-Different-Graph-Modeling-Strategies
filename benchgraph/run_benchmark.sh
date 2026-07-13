#!/bin/bash

variants=("research_base_opt" "research_denormalized_opt" "research_intermediate_opt" "research_transitive_opt" "research_uplift_opt")

for variant in "${variants[@]}"; do
  echo "=== Running variant: $variant ==="
  
  # Run Neo4j
  echo "--- Running Neo4j for $variant ---"
  python3 benchmark.py vendor-docker \
    --vendor-name neo4j-docker \
    benchmarks "${variant}/*/*/*" \
    --export-results "results_${variant}_neo4j_large.json" \
    --no-authorization \
    --num-workers-for-benchmark 4 \
    --single-threaded-runtime-sec 60 \
    --warm-up hot
  
  # Run Memgraph
  echo "--- Running Memgraph for $variant ---"
  python3 benchmark.py vendor-docker \
    --vendor-name memgraph-docker \
    benchmarks "${variant}/*/*/*" \
    --export-results "results_${variant}_memgraph_large.json" \
    --no-authorization \
    --num-workers-for-benchmark 4 \
    --single-threaded-runtime-sec 60 \
    --warm-up hot
  
  echo "=== Completed variant: $variant ==="
  echo ""
done

echo "All variants completed!"