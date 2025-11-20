# Graph Modeling Performance Benchmark

*A research implementation based on mgBench for evaluating graph modeling strategies*

## Overview

This repository contains the experimental setup for benchmarking different graph modeling approaches using Neo4j. The work extends mgBench framework to evaluate performance impact of graph modeling transformations.

## Research Focus

- **Four Graph Variants**: Property-To-Node (V1, research_uplift_opt.py), Path-To-Edge (V2, research_transitive_opt.py), Edge-To-Node (V3, research_intermediate_opt.py), Aggregation-Materialization (V4, research_denormalized_opt.py)
- **Seven Analytical Queries**: Complex traversals, aggregations, and pattern matching
- **Two Dataset Sizes**: Large (280K nodes) and Small (28K nodes)
- **Comprehensive Metrics**: Throughput, latency, CPU, memory, and storage

## Graph Variants Implementation

### V1: Property-To-Node Transformation
- **File**: `research_uplift_opt.py`
- **Description**: Elevates frequently queried properties to first-class nodes
- **Transformations**: Country properties converted to Country nodes with FROM_COUNTRY and LOCATED_IN relationships
- **Impact**: +0.004% nodes, +15% edges for explicit property representation

### V2: Path-To-Edge Transformation  
- **File**: `research_transitive_opt.py`
- **Description**: Materializes transitive paths as direct edges
- **Transformations**: COLLABORATED_WITH edges between Person nodes with shared movie collaborations
- **Impact**: Preserves node count, adds derived edges for path optimization

### V3: Edge-To-Node Transformation
- **File**: `research_intermediate_opt.py`
- **Description**: Converts relationships to intermediate role nodes
- **Transformations**: Role-specific nodes (ActorRole, DirectorRole, etc.) with two-hop Person-Role-Movie patterns
- **Impact**: Significant node increase for fine-grained relationship modeling

### V4: Aggregation-Materialization Transformation
- **File**: `research_denormalized_opt.py`
- **Description**: Pre-computes analytical properties for query optimization
- **Transformations**: Materialized aggregates on Person, Genre, Language, and Studio nodes
- **Impact**: Enhanced node properties with pre-computed metrics

## Dataset Schema

### Movie Graph Structure
- **Core Nodes**: Person, Movie, Studio, Genre, Language, Award
- **Relationship Types**: ACTED_IN, DIRECTED, PRODUCED, WROTE, COMPOSED_FOR, HAS_GENRE, IN_LANGUAGE, WON
- **Rich Properties**: Salaries, ratings, budgets, temporal data, and role-specific attributes

### Query Workload
Seven analytical queries designed to stress different database capabilities:
- Multi-dimensional aggregations and filtering
- Complex graph traversals and pattern matching  
- Relationship property analysis
- Cross-entity analytics and collaboration detection

## Experimental Methodology

### Benchmark Configuration
- **Execution Model**: 60-second time windows with 4 concurrent workers
- **Cache Conditions**: Hot runs with pre-warm queries to simulate production usage
- **Measurement**: Continuous resource monitoring via Docker container metrics

### Performance Metrics
- **Throughput**: Queries per second under concurrent load
- **Latency**: Response time percentiles (P50, P95, P99)
- **Resource Usage**: CPU utilization and peak memory consumption
- **Storage Efficiency**: Disk footprint including indexes and graph structure

## Research Infrastructure

This implementation builds upon the mgBench framework, leveraging its:
- Concurrent workload execution engine
- Bolt protocol client for Neo4j communication  
- Dataset management and indexing automation
- Result collection and aggregation pipelines

*For details on the [original benchmarking tool](https://github.com/memgraph/benchgraph.git), see the mgBench documentation and methodology.*

