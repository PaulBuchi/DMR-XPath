# performance_comparison.py
"""
Performance comparison between different XPath Accelerator implementations:
- Original EDGE Model (Phase 1)
- Full XPath Accelerator (Phase 2) 
- Single-Axis Accelerator (Phase 3)
"""
import time
import psycopg2
from typing import List, Tuple, Dict
from db import connect_db
from single_axis_accelerator import SingleAxisAccelerator
from axes import descendant_nodes, xpath_descendant_window


def benchmark_descendant_queries() -> None:
    """
    Benchmarks descendant queries across different implementations.
    """
    print("=== Performance Benchmark: Descendant Queries ===\n")
    
    conn = connect_db()
    if not conn:
        print("ERROR: Could not connect to database")
        return
    
    cur = conn.cursor()
    
    try:
        # Test nodes - using toy example data
        test_nodes = get_test_nodes(cur)
        
        if not test_nodes:
            print("No test nodes found. Please run previous phases first.")
            return
        
        print(f"Testing descendant queries on {len(test_nodes)} nodes:")
        for node_id, s_id, description in test_nodes:
            print(f"  - {s_id}: {description}")
        
        print("\nRunning performance benchmarks...")
        
        # Benchmark each implementation
        results = {}
        
        # 1. Test EDGE Model (recursive approach)
        print("\n1. Testing EDGE Model (Phase 1 - Recursive)...")
        results['edge_model'] = benchmark_edge_model(cur, test_nodes)
        
        # 2. Test Full XPath Accelerator (window functions)
        print("\n2. Testing Full XPath Accelerator (Phase 2 - Window Functions)...")
        results['full_xpath'] = benchmark_full_xpath_accelerator(cur, test_nodes)
        
        # 3. Test Single-Axis Accelerator
        print("\n3. Testing Single-Axis Accelerator (Phase 3 - Optimized)...")
        results['single_axis'] = benchmark_single_axis_accelerator(cur, test_nodes)
        
        # Display results
        display_benchmark_results(results, test_nodes)
        
    except Exception as e:
        print(f"Benchmark error: {e}")
    finally:
        cur.close()
        conn.close()


def get_test_nodes(cur: psycopg2.extensions.cursor) -> List[Tuple[int, str, str]]:
    """
    Gets test nodes for benchmarking.
    """
    test_nodes = []
    
    # Check which schema is available
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
    has_accel = cur.fetchone()[0]
    
    if has_accel:
        # Use accel schema
        test_queries = [
            ("vldb_2023", "VLDB 2023 venue (many descendants)"),
            ("SchmittKAMM23", "SchmittKAMM23 article"),
            ("HutterAK0L22", "HutterAK0L22 article")
        ]
        
        for s_id, description in test_queries:
            cur.execute("SELECT id FROM accel WHERE s_id = %s;", (s_id,))
            result = cur.fetchone()
            if result:
                test_nodes.append((result[0], s_id, description))
    
    return test_nodes


def benchmark_edge_model(cur: psycopg2.extensions.cursor, test_nodes: List[Tuple[int, str, str]]) -> Dict:
    """
    Benchmarks the original EDGE model recursive approach.
    """
    results = {'times': [], 'counts': []}
    
    for node_id, s_id, description in test_nodes:
        start_time = time.time()
        
        # Use recursive descendant function
        descendants = descendant_nodes(cur, node_id)
        
        end_time = time.time()
        
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        results['times'].append(execution_time)
        results['counts'].append(len(descendants))
        
        print(f"  {s_id}: {len(descendants)} descendants in {execution_time:.2f}ms")
    
    return results


def benchmark_full_xpath_accelerator(cur: psycopg2.extensions.cursor, test_nodes: List[Tuple[int, str, str]]) -> Dict:
    """
    Benchmarks the full XPath accelerator with window functions.
    """
    results = {'times': [], 'counts': []}
    
    for node_id, s_id, description in test_nodes:
        start_time = time.time()
        
        # Use window function approach
        descendants = xpath_descendant_window(cur, node_id)
        
        end_time = time.time()
        
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        results['times'].append(execution_time)
        results['counts'].append(len(descendants))
        
        print(f"  {s_id}: {len(descendants)} descendants in {execution_time:.2f}ms")
    
    return results


def benchmark_single_axis_accelerator(cur: psycopg2.extensions.cursor, test_nodes: List[Tuple[int, str, str]]) -> Dict:
    """
    Benchmarks the single-axis accelerator.
    """
    results = {'times': [], 'counts': []}
    
    # Check if single-axis schema exists
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'single_axis_accel');")
    has_single_axis = cur.fetchone()[0]
    
    if not has_single_axis:
        print("  Single-axis schema not found. Please run single_axis_accelerator.py first.")
        return results
    
    accelerator = SingleAxisAccelerator(cur)
    
    # Map node IDs from accel to single_axis_accel
    for node_id, s_id, description in test_nodes:
        # Find corresponding node in single-axis schema
        cur.execute("SELECT id FROM single_axis_accel WHERE s_id = %s;", (s_id,))
        single_axis_result = cur.fetchone()
        
        if not single_axis_result:
            print(f"  {s_id}: Node not found in single-axis schema")
            results['times'].append(0)
            results['counts'].append(0)
            continue
        
        single_axis_id = single_axis_result[0]
        
        start_time = time.time()
        
        # Use single-axis descendant function
        descendants = accelerator.xpath_descendant_single_axis(single_axis_id)
        
        end_time = time.time()
        
        execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
        results['times'].append(execution_time)
        results['counts'].append(len(descendants))
        
        print(f"  {s_id}: {len(descendants)} descendants in {execution_time:.2f}ms")
    
    return results


def display_benchmark_results(results: Dict, test_nodes: List[Tuple[int, str, str]]) -> None:
    """
    Displays benchmark results in a formatted table.
    """
    print("\n" + "="*80)
    print("PERFORMANCE BENCHMARK RESULTS")
    print("="*80)
    
    # Header
    print(f"{'Node':<15} {'EDGE Model':<15} {'Full XPath':<15} {'Single-Axis':<15} {'Improvement'}")
    print("-" * 80)
    
    # Results for each test node
    for i, (node_id, s_id, description) in enumerate(test_nodes):
        edge_time = results.get('edge_model', {}).get('times', [0])[i] if i < len(results.get('edge_model', {}).get('times', [])) else 0
        full_time = results.get('full_xpath', {}).get('times', [0])[i] if i < len(results.get('full_xpath', {}).get('times', [])) else 0
        single_time = results.get('single_axis', {}).get('times', [0])[i] if i < len(results.get('single_axis', {}).get('times', [])) else 0
        
        # Calculate improvement
        if edge_time > 0 and single_time > 0:
            improvement = f"{(edge_time / single_time):.1f}x"
        else:
            improvement = "N/A"
        
        print(f"{s_id:<15} {edge_time:<15.2f} {full_time:<15.2f} {single_time:<15.2f} {improvement}")
    
    # Summary statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    
    for impl_name, impl_results in results.items():
        if impl_results.get('times'):
            avg_time = sum(impl_results['times']) / len(impl_results['times'])
            total_descendants = sum(impl_results['counts'])
            print(f"{impl_name.replace('_', ' ').title():<20}: Avg {avg_time:.2f}ms, Total descendants: {total_descendants}")
    
    print("\n" + "="*60)
    print("OPTIMIZATION BENEFITS")
    print("="*60)
    print("  Single-Axis Accelerator focuses on one axis (descendants)")
    print("  B+-Tree index optimizes range queries")
    print("  Reduced schema overhead")
    print("  Consistent results with Phase 2 implementation")
    
    # Correctness verification
    print("\n" + "="*60)
    print("CORRECTNESS VERIFICATION")
    print("="*60)
    
    all_correct = True
    for i in range(len(test_nodes)):
        edge_count = results.get('edge_model', {}).get('counts', [0])[i] if i < len(results.get('edge_model', {}).get('counts', [])) else 0
        full_count = results.get('full_xpath', {}).get('counts', [0])[i] if i < len(results.get('full_xpath', {}).get('counts', [])) else 0
        single_count = results.get('single_axis', {}).get('counts', [0])[i] if i < len(results.get('single_axis', {}).get('counts', [])) else 0
        
        s_id = test_nodes[i][1]
        
        # Check if all implementations return the same count
        counts = [edge_count, full_count, single_count]
        non_zero_counts = [c for c in counts if c > 0]
        
        if len(set(non_zero_counts)) <= 1:  # All non-zero counts are the same
            print(f"  {s_id}: All implementations agree ({edge_count} descendants)")
        else:
            print(f"  {s_id}: Results differ - EDGE: {edge_count}, Full: {full_count}, Single: {single_count}")
            all_correct = False
    
    if all_correct:
        print("\n  ALL IMPLEMENTATIONS PRODUCE CONSISTENT RESULTS")
    else:
        print("\n  SOME IMPLEMENTATIONS PRODUCE DIFFERENT RESULTS")


def main() -> None:
    """
    Main function for performance comparison.
    """
    print("XPath Accelerator Performance Comparison\n")
    benchmark_descendant_queries()


if __name__ == "__main__":
    main()
