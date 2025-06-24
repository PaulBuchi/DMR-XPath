# window_performance_analysis.py
"""
Detaillierte Performance-Analyse der Window-Optimierungen.
Vergleicht Standard Window Functions mit optimierten Window Functions.
"""
import time
import psycopg2
from typing import List, Tuple, Dict
from db import connect_db
from window_optimization import OptimizedWindowAccelerator
from axes import xpath_descendant_window, xpath_ancestor_window


def analyze_window_performance() -> None:
    """
    Analysiert die Performance-Verbesserungen durch Window-Verkleinerung.
    """
    print("Detailed Window Performance Analysis:")
    
    conn = connect_db()
    if not conn:
        print("  ERROR: Could not connect to database")
        return
    
    cur = conn.cursor()
    
    try:
        # Check if both schemas exist
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
        has_standard = cur.fetchone()[0]
        
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'optimized_accel');")
        has_optimized = cur.fetchone()[0]
        
        if not has_standard or not has_optimized:
            print("  ERROR: Both standard and optimized schemas must exist")
            return
        
        accelerator = OptimizedWindowAccelerator(cur)
        
        # Performance tests
        test_descendant_performance(cur, accelerator)
        test_ancestor_performance(cur, accelerator)
        test_sibling_performance(cur, accelerator)
        analyze_window_size_reduction(cur, accelerator)
        #analyze_memory_io_benefits(cur)
        
    except Exception as e:
        print(f"  ERROR: {e}")
    finally:
        cur.close()
        conn.close()


def test_descendant_performance(cur: psycopg2.extensions.cursor, accelerator: OptimizedWindowAccelerator) -> None:
    """
    Testet die Performance der descendant-Achse.
    """
    print("\n  Descendant Axis Performance:")
    
    test_nodes = [
        ("vldb_2023", "Large subtree"),
        ("SchmittKAMM23", "Medium subtree"),
        ("HutterAK0L22", "Medium subtree")
    ]
    
    total_improvement = 0
    test_count = 0
    
    for s_id, description in test_nodes:
        # Get node IDs
        cur.execute("SELECT id FROM accel WHERE s_id = %s;", (s_id,))
        standard_result = cur.fetchone()
        cur.execute("SELECT id FROM optimized_accel WHERE s_id = %s;", (s_id,))
        optimized_result = cur.fetchone()
        
        if not standard_result or not optimized_result:
            continue
        
        standard_id = standard_result[0]
        optimized_id = optimized_result[0]
        
        # Benchmark standard implementation (simplified timing)
        start_time = time.time()
        for _ in range(50):
            standard_results = xpath_descendant_window(cur, standard_id)
        standard_time = (time.time() - start_time) * 20
        
        # Benchmark optimized implementation
        start_time = time.time()
        for _ in range(50):
            optimized_results = accelerator.xpath_descendant_optimized(optimized_id)
        optimized_time = (time.time() - start_time) * 20
        
        # Calculate improvement
        if optimized_time > 0:
            improvement = standard_time / optimized_time
            total_improvement += improvement
            test_count += 1
            print(f"    {s_id}: {improvement:.1f}x faster")
    
    if test_count > 0:
        avg_improvement = total_improvement / test_count
        print(f"    -> Average improvement: {avg_improvement:.1f}x")


def test_ancestor_performance(cur: psycopg2.extensions.cursor, accelerator: OptimizedWindowAccelerator) -> None:
    """
    Testet die Performance der ancestor-Achse.
    """
    print("\n  Ancestor Axis Performance:")
    
    # Test with author nodes
    cur.execute("""
        SELECT a.id, c.text FROM accel a 
        JOIN content c ON a.id = c.id 
        WHERE a.type = 'author' LIMIT 2;
    """)
    
    author_nodes = cur.fetchall()
    improvements = []
    
    for author_id, author_name in author_nodes:
        # Get corresponding optimized node
        cur.execute("""
            SELECT a.id FROM optimized_accel a 
            JOIN optimized_content c ON a.id = c.id 
            WHERE c.text = %s;
        """, (author_name,))
        
        optimized_result = cur.fetchone()
        if not optimized_result:
            continue
        
        optimized_id = optimized_result[0]
        
        # Simplified benchmark
        start_time = time.time()
        for _ in range(30):
            standard_results = xpath_ancestor_window(cur, author_id)
        standard_time = (time.time() - start_time) * 33.3
        
        start_time = time.time()
        for _ in range(30):
            optimized_results = accelerator.xpath_ancestor_optimized(optimized_id)
        optimized_time = (time.time() - start_time) * 33.3
        
        if optimized_time > 0:
            improvement = standard_time / optimized_time
            improvements.append(improvement)
            author_short = author_name[:15] + "..." if len(author_name) > 15 else author_name
            print(f"    {author_short}: {improvement:.1f}x faster")
    
    if improvements:
        avg_improvement = sum(improvements) / len(improvements)
        print(f"    -> Average improvement: {avg_improvement:.1f}x")


def test_sibling_performance(cur: psycopg2.extensions.cursor, accelerator: OptimizedWindowAccelerator) -> None:
    """
    Testet die Performance der sibling-Achse.
    """
    print("\n  Sibling Axis Performance:")
    
    # Test with article nodes
    test_cases = [
        ("SchmittKAMM23", "following"),
        ("SchalerHS23", "preceding")
    ]
    
    improvements = []
    
    for s_id, direction in test_cases:
        # Get node IDs
        cur.execute("SELECT id FROM accel WHERE s_id = %s;", (s_id,))
        standard_id = cur.fetchone()[0]
        
        cur.execute("SELECT id FROM optimized_accel WHERE s_id = %s;", (s_id,))
        optimized_id = cur.fetchone()[0]
        
        # Import sibling functions
        from axes import xpath_following_sibling_window, xpath_preceding_sibling_window
        
        # Simplified benchmark
        start_time = time.time()
        for _ in range(50):
            if direction == "following":
                standard_results = xpath_following_sibling_window(cur, standard_id)
            else:
                standard_results = xpath_preceding_sibling_window(cur, standard_id)
        standard_time = (time.time() - start_time) * 20
        
        start_time = time.time()
        for _ in range(50):
            optimized_results = accelerator.xpath_sibling_optimized(optimized_id, direction)
        optimized_time = (time.time() - start_time) * 20
        
        if optimized_time > 0:
            improvement = standard_time / optimized_time
            improvements.append(improvement)
            print(f"    {s_id} ({direction}): {improvement:.1f}x faster")
    
    if improvements:
        avg_improvement = sum(improvements) / len(improvements)
        print(f"    -> Average improvement: {avg_improvement:.1f}x")


def analyze_window_size_reduction(cur: psycopg2.extensions.cursor, accelerator: OptimizedWindowAccelerator) -> None:
    """
    Analysiert die tatsächliche Verkleinerung der Window-Größen.
    """
    print("\n  Window Size Reduction Analysis:")
    
    # Get basic statistics
    cur.execute("SELECT COUNT(*) FROM optimized_accel WHERE subtree_size <= 1;")
    leaf_nodes = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM optimized_accel;")
    total_nodes = cur.fetchone()[0]
    
    cur.execute("SELECT AVG(subtree_size), MAX(subtree_size) FROM optimized_accel WHERE subtree_size > 1;")
    avg_subtree, max_subtree = cur.fetchone()
    
    leaf_reduction = (leaf_nodes / total_nodes) * 100 if total_nodes > 0 else 0
    
    print(f"    Leaf node optimization: {leaf_nodes}/{total_nodes} nodes ({leaf_reduction:.1f}% skipped)")
    print(f"    Large subtree optimization: Max size {max_subtree} -> limited depth")
    print(f"    Average subtree size: {avg_subtree:.1f}")

#Not sure if this works as intendd. Ergibt teils falsche ergebnisse. Ich verwende es deshabl erstmal nicht.
def analyze_memory_io_benefits(cur: psycopg2.extensions.cursor) -> None:
    """
    Analysiert die Memory- und I/O-Vorteile der Optimierungen.
    """
    print("\n  Memory and I/O Benefits:")
    
    # Get basic table sizes
    cur.execute("SELECT COUNT(*) FROM accel;")
    standard_count = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM optimized_accel;")
    optimized_count = cur.fetchone()[0]
    
    print(f"    Standard accelerator: {standard_count} nodes")
    print(f"    Optimized accelerator: {optimized_count} nodes")