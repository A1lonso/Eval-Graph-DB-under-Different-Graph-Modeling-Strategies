import json
import matplotlib.pyplot as plt
import numpy as np
import os

def detect_top_level_key(data):
    """
    Detect the top-level key that contains the test results.
    Looks for keys like 'research_base_opt', 'research_uplift_opt', etc.
    """
    # First, look for keys that start with 'research_' and end with '_opt'
    for key in data.keys():
        if key.startswith('research_') and key.endswith('_opt'):
            # Check if this key has the test data structure
            if 'default' in data[key] and 'test' in data[key]['default']:
                return key
            elif 'test' in data[key]:
                return key
    
    # If no specific key found, look for any key containing 'research'
    for key in data.keys():
        if 'research' in key.lower():
            if 'default' in data[key] and 'test' in data[key]['default']:
                return key
            elif 'test' in data[key]:
                return key
    
    # If still not found, look for the first key that contains test data
    for key in data.keys():
        if isinstance(data[key], dict):
            if 'default' in data[key] and 'test' in data[key]['default']:
                return key
            elif 'test' in data[key]:
                return key
    
    return None

def extract_query_metrics(file_path):
    """
    Extract duration, throughput, CPU, and memory for each query.
    """
    with open(file_path, 'r') as f:
        data = json.load(f)

    vendor = data.get('__run_configuration__', {}).get('vendor')
    file_name = os.path.basename(file_path).lower()
    expected_vendor = 'memgraph' if 'memgraph' in file_name else 'neo4j' if 'neo4j' in file_name else None
    if expected_vendor and (vendor is None or expected_vendor not in vendor.lower()):
        raise ValueError(
            f"Vendor mismatch in {file_path}: filename expects {expected_vendor}, recorded vendor is {vendor!r}"
        )
    
    # Detect the top-level key
    top_key = detect_top_level_key(data)
    if top_key is None:
        raise ValueError(f"Could not find research key in {file_path}")
    
    # Mapping of query names to their keys in the JSON
    query_mapping = {
        'Q1': 'complex_categorical_analytics',
        'Q2': 'complex_country_network_base',
        'Q3': 'cross_role_workforce_analysis',
        'Q4': 'denormalized_genre_performance',
        'Q5': 'relationship_property_mining',
        'Q6': 'strong_collaboration_clusters',
        'Q7': 'workforce_salary_analytics'
    }
    
    # Alternative names for some queries (especially Q2 in Intermediate)
    query_mapping_alt = {
        'Q2': ['complex_country_network_base', 'complex_country_network_optimized', 'complex_country_network_intermediate']
    }
    
    durations = {}
    throughputs = {}
    cpu_usage = {}
    memory_usage = {}
    
    # Get the test data (could be under 'default' or directly under top_key)
    test_data = None
    if 'default' in data[top_key] and 'test' in data[top_key]['default']:
        test_data = data[top_key]['default']['test']
    elif 'test' in data[top_key]:
        test_data = data[top_key]['test']
    else:
        raise ValueError(f"Could not find test data in {file_path}")
    
    # Navigate through the nested structure
    for q_key, query_name in query_mapping.items():
        try:
            # Check for alternate query names (especially for Q2)
            if q_key in query_mapping_alt:
                found = False
                for alt_name in query_mapping_alt[q_key]:
                    try:
                        query_data = test_data[alt_name]['without_fine_grained_authorization']
                        durations[q_key] = query_data['latency_stats']['mean']
                        throughputs[q_key] = query_data['throughput']
                        database = query_data.get('database', {})
                        cpu_usage[q_key] = database['cpu'] / query_data['count']
                        memory_usage[q_key] = database['memory']
                        found = True
                        break
                    except KeyError:
                        continue
                if not found:
                    raise KeyError(f"Could not find {q_key} in {file_path}")
            else:
                query_data = test_data[query_name]['without_fine_grained_authorization']
                durations[q_key] = query_data['latency_stats']['mean']
                throughputs[q_key] = query_data['throughput']
                database = query_data.get('database', {})
                cpu_usage[q_key] = database['cpu'] / query_data['count']
                memory_usage[q_key] = database['memory']
        except (KeyError, TypeError, ZeroDivisionError) as e:
            raise ValueError(
                f"Missing or invalid metrics for {q_key} ({query_name}) in {file_path}"
            ) from e
    
    return durations, throughputs, cpu_usage, memory_usage

def create_comparison_charts(baseline_small_file, baseline_large_file, 
                           variant_small_file=None, variant_large_file=None,
                           variant_name="Edge-To-Node",
                           output_dir="charts"):
    """
    Create grouped bar charts comparing query metrics across configurations.
    Generates Duration and Throughput charts.
    
    Args:
        baseline_small_file: Path to Baseline Small JSON file
        baseline_large_file: Path to Baseline Large JSON file
        variant_small_file: Path to Variant Small JSON file (optional)
        variant_large_file: Path to Variant Large JSON file (optional)
        variant_name: Name of the variant type (e.g., "Edge-To-Node")
        output_dir: Directory to save the charts
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract metrics from all files
    print(f"Reading Baseline Small: {baseline_small_file}")
    baseline_small_dur, baseline_small_tput, _, _ = extract_query_metrics(baseline_small_file)
    
    print(f"Reading Baseline Large: {baseline_large_file}")
    baseline_large_dur, baseline_large_tput, _, _ = extract_query_metrics(baseline_large_file)
    
    variant_small_dur = variant_small_tput = None
    variant_large_dur = variant_large_tput = None
    
    if variant_small_file and os.path.exists(variant_small_file):
        print(f"Reading Variant Small: {variant_small_file}")
        variant_small_dur, variant_small_tput, _, _ = extract_query_metrics(variant_small_file)
    else:
        print(f"Warning: Variant Small file not found: {variant_small_file}")
    
    if variant_large_file and os.path.exists(variant_large_file):
        print(f"Reading Variant Large: {variant_large_file}")
        variant_large_dur, variant_large_tput, _, _ = extract_query_metrics(variant_large_file)
    else:
        print(f"Warning: Variant Large file not found: {variant_large_file}")
    
    # Query labels
    queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']
    query_labels = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']
    
    # Prepare data for plotting
    x = np.arange(len(queries))
    width = 0.2  # Width of each bar
    
    # Collect metrics for each group
    def prepare_metrics(dur_dict, tput_dict):
        if dur_dict is None or tput_dict is None:
            raise ValueError('Cannot plot a comparison series without query metrics')
        dur_values = []
        tput_values = []
        for q in queries:
            dur_values.append(dur_dict[q])
            tput_values.append(tput_dict[q])
        return dur_values, tput_values
    
    baseline_small_dur_vals, baseline_small_tput_vals = prepare_metrics(baseline_small_dur, baseline_small_tput)
    baseline_large_dur_vals, baseline_large_tput_vals = prepare_metrics(baseline_large_dur, baseline_large_tput)
    variant_small_dur_vals, variant_small_tput_vals = prepare_metrics(variant_small_dur, variant_small_tput)
    variant_large_dur_vals, variant_large_tput_vals = prepare_metrics(variant_large_dur, variant_large_tput)
    
    # ==================== DURATION CHART ====================
    fig1, ax1 = plt.subplots(figsize=(14, 8))
    
    bars1 = ax1.bar(x - 1.5*width, baseline_small_dur_vals, width, 
                    label='Baseline (Small)', color='#2E86C1', edgecolor='black', linewidth=0.5)
    bars2 = ax1.bar(x - 0.5*width, variant_small_dur_vals, width, 
                    label=f'{variant_name} (Small)', color='#E74C3C', edgecolor='black', linewidth=0.5)
    bars3 = ax1.bar(x + 0.5*width, baseline_large_dur_vals, width, 
                    label='Baseline (Large)', color='#2E86C1', alpha=0.7, edgecolor='black', linewidth=0.5)
    bars4 = ax1.bar(x + 1.5*width, variant_large_dur_vals, width, 
                    label=f'{variant_name} (Large)', color='#E74C3C', alpha=0.7, edgecolor='black', linewidth=0.5)
    
    def add_value_labels(ax, bars, format_str='{:.2f}s', rotation=90):
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       format_str.format(height), ha='center', va='bottom', fontsize=8, rotation=rotation)
    
    add_value_labels(ax1, bars1)
    add_value_labels(ax1, bars2)
    add_value_labels(ax1, bars3)
    add_value_labels(ax1, bars4)
    
    ax1.set_xlabel('Query', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Duration Time (Seconds) - Logarithmic Scale', fontsize=12, fontweight='bold')
    ax1.set_title(f'Query Duration Comparison - {variant_name} vs Baseline', fontsize=14, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(query_labels)
    ax1.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=10)
    ax1.set_yscale('log')
    ax1.grid(True, axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    duration_output = os.path.join(output_dir, f'query_duration_comparison_{variant_name.lower().replace(" ", "_")}.png')
    plt.savefig(duration_output, dpi=300, bbox_inches='tight')
    print(f"Duration chart saved as: {duration_output}")
    plt.close(fig1)
    
    # ==================== THROUGHPUT CHART ====================
    fig2, ax2 = plt.subplots(figsize=(14, 8))
    
    bars5 = ax2.bar(x - 1.5*width, baseline_small_tput_vals, width, 
                    label='Baseline (Small)', color='#2E86C1', edgecolor='black', linewidth=0.5)
    bars6 = ax2.bar(x - 0.5*width, variant_small_tput_vals, width, 
                    label=f'{variant_name} (Small)', color='#E74C3C', edgecolor='black', linewidth=0.5)
    bars7 = ax2.bar(x + 0.5*width, baseline_large_tput_vals, width, 
                    label='Baseline (Large)', color='#2E86C1', alpha=0.7, edgecolor='black', linewidth=0.5)
    bars8 = ax2.bar(x + 1.5*width, variant_large_tput_vals, width, 
                    label=f'{variant_name} (Large)', color='#E74C3C', alpha=0.7, edgecolor='black', linewidth=0.5)
    
    def add_tput_labels(bars):
        for bar in bars:
            height = bar.get_height()
            if height > 0:
                # Format throughput with appropriate suffix
                if height >= 1000:
                    label = f'{height/1000:.1f}K'
                else:
                    label = f'{height:.0f}'
                ax2.text(bar.get_x() + bar.get_width()/2., height,
                        label, ha='center', va='bottom', fontsize=8, rotation=90)
    
    add_tput_labels(bars5)
    add_tput_labels(bars6)
    add_tput_labels(bars7)
    add_tput_labels(bars8)
    
    ax2.set_xlabel('Query', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Throughput (Queries Per Second) - Logarithmic Scale', fontsize=12, fontweight='bold')
    ax2.set_title(f'Query Throughput Comparison - {variant_name} vs Baseline', fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels(query_labels)
    ax2.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=10)
    ax2.set_yscale('log')
    ax2.grid(True, axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    throughput_output = os.path.join(output_dir, f'query_throughput_comparison_{variant_name.lower().replace(" ", "_")}.png')
    plt.savefig(throughput_output, dpi=300, bbox_inches='tight')
    print(f"Throughput chart saved as: {throughput_output}")
    plt.close(fig2)

    # ==================== PRINT SUMMARY ====================
    print("\n" + "="*90)
    print(f"SUMMARY - {variant_name} vs Baseline Comparison")
    print("="*90)
    
    print("\nDURATION (seconds):")
    print("-" * 90)
    print(f"{'Query':<8} {'Baseline Small':<18} {variant_name + ' Small':<20} {'Baseline Large':<18} {variant_name + ' Large':<20}")
    print("-" * 90)
    for i, q in enumerate(queries):
        print(f"{q:<8} {baseline_small_dur_vals[i]:<18.4f} {variant_small_dur_vals[i]:<20.4f} "
              f"{baseline_large_dur_vals[i]:<18.4f} {variant_large_dur_vals[i]:<20.4f}")
    
    print("\nTHROUGHPUT (queries/second):")
    print("-" * 90)
    print(f"{'Query':<8} {'Baseline Small':<18} {variant_name + ' Small':<20} {'Baseline Large':<18} {variant_name + ' Large':<20}")
    print("-" * 90)
    for i, q in enumerate(queries):
        print(f"{q:<8} {baseline_small_tput_vals[i]:<18.2f} {variant_small_tput_vals[i]:<20.2f} "
              f"{baseline_large_tput_vals[i]:<18.2f} {variant_large_tput_vals[i]:<20.2f}")
    
    # Calculate improvements
    print("\n" + "="*90)
    print("PERFORMANCE IMPROVEMENTS (Throughput increase %):")
    print("-" * 90)
    print(f"{'Query':<8} {'Small Improvement':<20} {'Large Improvement':<20}")
    print("-" * 90)
    for i, q in enumerate(queries):
        if baseline_small_tput_vals[i] > 0 and variant_small_tput_vals[i] > 0:
            small_improvement = ((variant_small_tput_vals[i] - baseline_small_tput_vals[i]) / baseline_small_tput_vals[i]) * 100
        else:
            small_improvement = 0
            
        if baseline_large_tput_vals[i] > 0 and variant_large_tput_vals[i] > 0:
            large_improvement = ((variant_large_tput_vals[i] - baseline_large_tput_vals[i]) / baseline_large_tput_vals[i]) * 100
        else:
            large_improvement = 0
            
        print(f"{q:<8} {small_improvement:<20.1f}% {large_improvement:<20.1f}%")

    return {
        'queries': queries,
        'duration': {
            'baseline_small': baseline_small_dur_vals,
            'variant_small': variant_small_dur_vals,
            'baseline_large': baseline_large_dur_vals,
            'variant_large': variant_large_dur_vals
        },
        'throughput': {
            'baseline_small': baseline_small_tput_vals,
            'variant_small': variant_small_tput_vals,
            'baseline_large': baseline_large_tput_vals,
            'variant_large': variant_large_tput_vals
        }
    }

def create_large_resource_charts(baseline_large_file, configs, output_dir="charts"):
    """Create one CPU chart and one memory chart for all large configurations."""
    os.makedirs(output_dir, exist_ok=True)
    configurations = [('Baseline', baseline_large_file)]
    configurations.extend((config['variant_name'], config['variant_large_file']) for config in configs)

    queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']
    resource_data = []
    for name, file_path in configurations:
        if not os.path.exists(file_path):
            print(f"Warning: Skipping {name}; large file not found: {file_path}")
            continue
        print(f"Reading {name} Large: {file_path}")
        _, _, cpu_usage, memory_usage = extract_query_metrics(file_path)
        resource_data.append({
            'name': name,
            'cpu': [cpu_usage[query] for query in queries],
            'memory': [memory_usage[query] / 1024**2 for query in queries]
        })

    if not resource_data:
        raise ValueError('No large configuration files found for resource charts')

    x = np.arange(len(queries))
    width = 0.8 / len(resource_data)
    colors = ['#2E86C1', '#E74C3C', '#27AE60', '#F39C12', '#8E44AD', '#16A085']

    def create_resource_chart(metric, ylabel, title, filename, formatter):
        fig, ax = plt.subplots(figsize=(14, 8))
        for index, configuration in enumerate(resource_data):
            bars = ax.bar(x - 0.4 + (index + 0.5) * width, configuration[metric], width,
                          label=configuration['name'], color=colors[index % len(colors)],
                          edgecolor='black', linewidth=0.5)
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax.text(bar.get_x() + bar.get_width() / 2, height, formatter(height),
                            ha='center', va='bottom', fontsize=7, rotation=90)
        ax.set_xlabel('Query', fontsize=12, fontweight='bold')
        ax.set_ylabel(ylabel, fontsize=12, fontweight='bold')
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(queries)
        ax.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=10)
        ax.set_yscale('log')
        ax.grid(True, axis='y', linestyle='--', alpha=0.3)
        plt.tight_layout()
        output_path = os.path.join(output_dir, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Resource chart saved as: {output_path}")
        plt.close(fig)

    create_resource_chart(
        'cpu', 'CPU Time per Query (seconds) - Logarithmic Scale',
        'CPU Time per Query Comparison',
        'cpu_usage_comparison_all_large.png', lambda value: f'{value:.4f}s')
    create_resource_chart(
        'memory', 'Memory Usage (MiB) - Logarithmic Scale',
        'Memory Usage Comparison - All Large Configurations',
        'memory_usage_comparison_all_large.png',
        lambda value: f'{value / 1024:.1f}GiB' if value >= 1024 else f'{value:.0f}MiB')

def create_dataset_charts(configurations, output_dir, filename_prefix, display_name, include_resources=False):
    """Create duration and throughput charts, plus Large-only resource charts."""
    os.makedirs(output_dir, exist_ok=True)
    queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']
    loaded = []

    for name, file_path in configurations:
        if not file_path or not os.path.exists(file_path):
            print(f"Warning: Skipping {name}; file not found: {file_path}")
            continue
        print(f"Reading {name}: {file_path}")
        duration, throughput, cpu, memory = extract_query_metrics(file_path)
        loaded.append({
            'name': name,
            'duration': [duration.get(query) or 0 for query in queries],
            'throughput': [throughput.get(query) or 0 for query in queries],
            'cpu': [cpu.get(query) or 0 for query in queries],
            'memory': [(memory.get(query) or 0) / 1024**2 for query in queries]
        })

    if not loaded:
        raise ValueError(f'No result files found for {filename_prefix}')

    x = np.arange(len(queries))
    def series_color(name):
        if name.startswith('Baseline'):
            return {'Small': '#87CEEB', 'Large': '#2E86C1', 'Extra': '#0B3D91'}.get(
                next((size for size in ('Small', 'Large', 'Extra') if size in name), 'Large'))
        return {'Small': '#F5A29A', 'Large': '#E74C3C', 'Extra': '#A61B1B'}.get(
            next((size for size in ('Small', 'Large', 'Extra') if size in name), 'Large'))

    def create_chart(metric, ylabel, title, suffix, formatter, chart_data, logarithmic=True):
        fig, ax = plt.subplots(figsize=(16, 9))
        width = min(0.8 / len(chart_data), 0.2)
        for index, configuration in enumerate(chart_data):
            group_width = 0.8 / len(chart_data)
            offset = -0.4 + (index + 0.5) * group_width
            bars = ax.bar(x + offset, configuration[metric], width,
                          label=configuration['name'], color=series_color(configuration['name']),
                          edgecolor='black', linewidth=0.5)
        ax.set_xlabel('Query', fontsize=14, fontweight='bold')
        ax.set_ylabel(ylabel, fontsize=14, fontweight='bold')
        ax.set_title(title, fontsize=18, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(queries)
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.12),
              ncol=min(len(chart_data), 5), fontsize=11, title='Variant Type',
              title_fontsize=12)
        if logarithmic:
            ax.set_yscale('log')
        ax.grid(True, axis='y', linestyle='--', alpha=0.3)
        plt.tight_layout(rect=(0, 0.08, 1, 1))
        output_path = os.path.join(output_dir, f'{filename_prefix}_{suffix}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved as: {output_path}")
        plt.close(fig)

    create_chart('duration', 'Execution Time (seconds) - Log scale',
                 display_name, 'duration', lambda value: f'{value:.2f}s', loaded)
    create_chart('throughput', 'Queries per second (log scale)',
                 display_name, 'throughput',
                 lambda value: f'{value / 1000:.1f}K' if value >= 1000 else f'{value:.0f}', loaded)
    if include_resources:
        large_loaded = [configuration for configuration in loaded if '(Large)' in configuration['name']]
        if not large_loaded:
            raise ValueError(f'No Large result files found for {filename_prefix}')
        create_chart('cpu', 'CPU Time per Query (seconds) - Logarithmic Scale',
                     display_name, 'cpu_usage',
                     lambda value: f'{value:.1f}%', large_loaded)
        create_chart('memory', 'Memory Usage (MiB) - Logarithmic Scale',
                     display_name, 'memory_usage',
                     lambda value: f'{value / 1024:.1f}GiB' if value >= 1024 else f'{value:.0f}MiB',
                     large_loaded, logarithmic=False)


def create_all_large_resource_charts(configurations, output_dir, filename_prefix):
    """Create CPU and memory charts comparing every available Large configuration."""
    os.makedirs(output_dir, exist_ok=True)
    queries = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7']
    resource_data = []

    for name, file_path in configurations:
        if not file_path or not os.path.exists(file_path):
            print(f"Warning: Skipping {name}; file not found: {file_path}")
            continue
        print(f"Reading {name}: {file_path}")
        _, _, cpu, memory = extract_query_metrics(file_path)
        resource_data.append({
            'name': name,
            'cpu': [cpu[query] for query in queries],
            'memory': [memory[query] / 1024**2 for query in queries]
        })

    if not resource_data:
        raise ValueError(f'No Large result files found for {filename_prefix}')

    x = np.arange(len(queries))
    group_width = 0.8 / len(resource_data)
    width = min(group_width, 0.2)
    resource_colors = ['#2E86C1', '#E74C3C', '#27AE60', '#F39C12', '#8E44AD', '#16A085']
    def create_resource_chart(metric, ylabel, title, suffix, formatter, logarithmic=True):
        fig, ax = plt.subplots(figsize=(16, 9))
        for index, configuration in enumerate(resource_data):
            offset = -0.4 + (index + 0.5) * group_width
            bars = ax.bar(x + offset, configuration[metric], width,
                          label=configuration['name'], color=resource_colors[index % len(resource_colors)],
                          edgecolor='black', linewidth=0.5)
        ax.set_xlabel('Query', fontsize=14, fontweight='bold')
        ax.set_ylabel(ylabel, fontsize=14, fontweight='bold')
        ax.set_title(title, fontsize=18, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(queries)
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.12),
              ncol=min(len(resource_data), 5), fontsize=11, title='Variant Type',
              title_fontsize=12)
        if logarithmic:
            ax.set_yscale('log')
        ax.grid(True, axis='y', linestyle='--', alpha=0.3)
        plt.tight_layout(rect=(0, 0.08, 1, 1))
        output_path = os.path.join(output_dir, f'{filename_prefix}_{suffix}.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"Chart saved as: {output_path}")
        plt.close(fig)

    create_resource_chart('cpu', 'CPU Time per Query (seconds)',
                          'CPU Time per Query Comparison', 'cpu_time_per_query',
                          lambda value: f'{value:.4f}s')
    create_resource_chart('memory', 'Memory Usage (MiB)',
                          'Memory Usage Comparison', 'memory_usage',
                          lambda value: f'{value / 1024:.1f}GiB' if value >= 1024 else f'{value:.0f}MiB',
                          logarithmic=False)


def generate_all_comparisons(configs, output_dir="charts"):
    """
    Generate comparison charts for multiple variants in one go.
    
    Args:
        configs: List of dictionaries, each containing:
            - variant_small_file: Path to variant small JSON
            - variant_large_file: Path to variant large JSON
            - variant_name: Name of the variant
    """
    baseline = 'results_research_base_opt'
    variant_display_names = {
        'Denormalized': 'Aggregation-Materialization',
        'Transitive': 'Path-to-Edge',
        'Intermediate': 'Edge-To-Node',
        'Uplift': 'Property-To-Node'
    }
    neo4j_large_configurations = [('Baseline (Large)', f'{baseline}_neo4j_large.json')]
    memgraph_large_configurations = [('Baseline (Large)', f'{baseline}_memgraph_large.json')]
    for config in configs:
        variant_slug = config['variant_name'].lower()
        neo4j_configurations = [
            ('Baseline (Small)', f'{baseline}_neo4j_small.json'),
            (f"{config['variant_name']} (Small)", config['variant_small_file_neo4j']),
            ('Baseline (Large)', f'{baseline}_neo4j_large.json'),
            (f"{config['variant_name']} (Large)", config['variant_large_file_neo4j']),
            ('Baseline (Extra)', f'{baseline}_neo4j_extra.json'),
            (f"{config['variant_name']} (Extra)", config.get('variant_extra_file'))
        ]
        display_name = variant_display_names[config['variant_name']]
        neo4j_configurations[1] = (f'{display_name} (Small)', neo4j_configurations[1][1])
        neo4j_configurations[3] = (f'{display_name} (Large)', neo4j_configurations[3][1])
        neo4j_configurations[5] = (f'{display_name} (Extra)', neo4j_configurations[5][1])
        create_dataset_charts(neo4j_configurations,
                              os.path.join(output_dir, 'neo4j', variant_slug), variant_slug,
                              display_name)
        neo4j_large_configurations.append(
            (display_name, config['variant_large_file_neo4j']))

        memgraph_configurations = [
            ('Baseline (Small)', f'{baseline}_memgraph_small.json'),
            (f"{config['variant_name']} (Small)", config['variant_small_file_memgraph']),
            ('Baseline (Large)', f'{baseline}_memgraph_large.json'),
            (f"{config['variant_name']} (Large)", config['variant_large_file_memgraph'])
        ]
        memgraph_configurations[1] = (f'{display_name} (Small)', memgraph_configurations[1][1])
        memgraph_configurations[3] = (f'{display_name} (Large)', memgraph_configurations[3][1])
        create_dataset_charts(memgraph_configurations,
                              os.path.join(output_dir, 'memgraph', variant_slug), variant_slug,
                              display_name)
        memgraph_large_configurations.append(
            (display_name, config['variant_large_file_memgraph']))

    create_all_large_resource_charts(
        neo4j_large_configurations, os.path.join(output_dir, 'neo4j'), 'neo4j_all_large')
    create_all_large_resource_charts(
        memgraph_large_configurations, os.path.join(output_dir, 'memgraph'), 'memgraph_all_large')

if __name__ == "__main__":
    # Check if baseline files exist
    baseline_small_file = "results_research_base_opt_neo4j_small.json"
    baseline_large_file = "results_research_base_opt_neo4j_large.json"
    
    if not os.path.exists(baseline_small_file) or not os.path.exists(baseline_large_file):
        print("Error: Baseline files not found. Please ensure the following files exist:")
        print(f"  - {baseline_small_file}")
        print(f"  - {baseline_large_file}")
        exit(1)
    
    # Define configurations for different variants
    configs = [
        {
            'variant_name': 'Transitive',
            'variant_small_file_neo4j': 'results_research_transitive_opt_neo4j_small.json',
            'variant_large_file_neo4j': 'results_research_transitive_opt_neo4j_large.json',
            'variant_small_file_memgraph': 'results_research_transitive_opt_memgraph_small.json',
            'variant_large_file_memgraph': 'results_research_transitive_opt_memgraph_large.json',
            'variant_extra_file': 'results_research_transitive_opt_neo4j_extra.json'
        },
        {
            'variant_name': 'Uplift',
            'variant_small_file_neo4j': 'results_research_uplift_opt_neo4j_small.json',
            'variant_large_file_neo4j': 'results_research_uplift_opt_neo4j_large.json',
            'variant_small_file_memgraph': 'results_research_uplift_opt_memgraph_small.json',
            'variant_large_file_memgraph': 'results_research_uplift_opt_memgraph_large.json'
        },
        {
            'variant_name': 'Denormalized',
            'variant_small_file_neo4j': 'results_research_denormalized_opt_neo4j_small.json',
            'variant_large_file_neo4j': 'results_research_denormalized_opt_neo4j_large.json',
            'variant_small_file_memgraph': 'results_research_denormalized_opt_memgraph_small.json',
            'variant_large_file_memgraph': 'results_research_denormalized_opt_memgraph_large.json',
            'variant_extra_file': 'results_research_denormalized_opt_neo4j_extra.json'
        },
        {
            'variant_name': 'Intermediate',
            'variant_small_file_neo4j': 'results_research_intermediate_opt_neo4j_small.json',
            'variant_large_file_neo4j': 'results_research_intermediate_opt_neo4j_large.json',
            'variant_small_file_memgraph': 'results_research_intermediate_opt_memgraph_small.json',
            'variant_large_file_memgraph': 'results_research_intermediate_opt_memgraph_large.json',
            'variant_extra_file': 'results_research_intermediate_opt_neo4j_extra.json'
        }
    ]
    
    # Check which variant files exist
    valid_configs = []
    for config in configs:
        required_files = [
            config['variant_small_file_neo4j'], config['variant_large_file_neo4j'],
            config['variant_small_file_memgraph'], config['variant_large_file_memgraph']
        ]
        missing_files = [file_path for file_path in required_files if not os.path.exists(file_path)]
        
        if not missing_files:
            valid_configs.append(config)
            print(f"✓ Found {config['variant_name']} files")
        else:
            print(f"✗ Skipping {config['variant_name']} - files not found")
            for file_path in missing_files:
                print(f"    Missing: {file_path}")
    
    if not valid_configs:
        print("\nNo valid variant configurations found. Please check your file paths.")
        exit(1)
    
    # Generate all charts
    print(f"\nGenerating charts for {len(valid_configs)} variant(s)...")
    generate_all_comparisons(valid_configs, output_dir="charts")
    
    print("\n" + "="*90)
    print("All charts generated successfully!")
    print(f"Charts are saved in the 'charts' directory.")
    print("="*90)