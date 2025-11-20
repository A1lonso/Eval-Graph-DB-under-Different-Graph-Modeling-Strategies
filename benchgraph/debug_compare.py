# debug_comparison.py
import json
import sys

def load_and_compare(file1, file2):
    with open(file1, 'r') as f:
        data1 = json.load(f)
    with open(file2, 'r') as f:
        data2 = json.load(f)
    
    print("=== PERFORMANCE COMPARISON ===")
    print(f"{'Query':<20} {'Base QPS':<10} {'Variant QPS':<12} {'Change':<10}")
    print("=" * 60)
    
    # Extract test results
    base_tests = data1["ic_base"]["default"]["test"]
    variant_tests = data2["ic_intermediate_nodes"]["default"]["test"]
    
    for test_name in base_tests.keys():
        if test_name in variant_tests:
            base_qps = base_tests[test_name]["without_fine_grained_authorization"]["throughput"]
            variant_qps = variant_tests[test_name]["without_fine_grained_authorization"]["throughput"]
            change_pct = ((variant_qps - base_qps) / base_qps) * 100
            
            print(f"{test_name:<20} {base_qps:<10.1f} {variant_qps:<12.1f} {change_pct:+.1f}%")

if __name__ == "__main__":
    load_and_compare("results_ic_base.json", "results_ic_intermediate_nodes.json")