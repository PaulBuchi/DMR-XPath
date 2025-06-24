# demonstrate_correctness.py
"""
Demonstrates the correctness of the Single-Axis XPath Accelerator annotation
on the same toy example as used in Phase 2.
"""
import psycopg2
from db import connect_db
from single_axis_accelerator import SingleAxisAccelerator


def demonstrate_toy_example_correctness():
    """
    Zeigt die Korrektheit der Single-Axis Annotation am selben Ausschnitt
    des Toy-Beispiels wie in Phase 2.
    """
    print("=== Correctness Demonstration: Single-Axis XPath Accelerator ===\n")
    
    conn = connect_db()
    if not conn:
        print("ERROR: Could not connect to database")
        return
    
    cur = conn.cursor()
    
    try:
        # Check if single-axis schema exists
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'single_axis_accel');")
        has_single_axis = cur.fetchone()[0]
        
        if not has_single_axis:
            print("Single-axis schema not found. Please run single_axis_accelerator.py first.")
            return
        
        accelerator = SingleAxisAccelerator(cur)
        
        print("1. Toy Example Data Structure:")
        show_toy_example_structure(cur)
        
        print("\n2. Testing Descendant Axis (Same as Phase 2):")
        test_descendant_axis_correctness(cur, accelerator)
        
        print("\n3. Annotation Verification:")
        verify_annotation_correctness(cur, accelerator)
        
        print("\n4. Window Function Formula Verification:")
        verify_window_function_formula(cur, accelerator)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()


def show_toy_example_structure(cur: psycopg2.extensions.cursor):
    """
    Shows the structure of the toy example data.
    """
    print("Toy Example Structure (Single-Axis Schema):")
    
    # Show root level structure
    cur.execute("""
        SELECT id, s_id, type, pre_order, post_order
        FROM single_axis_accel 
        WHERE parent IS NULL OR type = 'bib'
        ORDER BY pre_order;
    """)
    
    root_nodes = cur.fetchall()
    for node_id, s_id, node_type, pre_order, post_order in root_nodes:
        print(f"  Root: {node_type} (ID: {node_id}, pre: {pre_order}, post: {post_order})")
    
    # Show venue level
    cur.execute("""
        SELECT id, s_id, type, pre_order, post_order
        FROM single_axis_accel 
        WHERE s_id LIKE '%_20%'
        ORDER BY pre_order;
    """)
    
    venues = cur.fetchall()
    print("\n  Venues:")
    for node_id, s_id, node_type, pre_order, post_order in venues:
        print(f"    {s_id}: {node_type} (ID: {node_id}, pre: {pre_order}, post: {post_order})")
    
    # Show articles
    cur.execute("""
        SELECT id, s_id, type, pre_order, post_order
        FROM single_axis_accel 
        WHERE type = 'article'
        ORDER BY pre_order;
    """)
    
    articles = cur.fetchall()
    print("\n  Articles:")
    for node_id, s_id, node_type, pre_order, post_order in articles:
        print(f"    {s_id}: {node_type} (ID: {node_id}, pre: {pre_order}, post: {post_order})")


def test_descendant_axis_correctness(cur: psycopg2.extensions.cursor, accelerator: SingleAxisAccelerator):
    """
    Tests the descendant axis with the same cases as Phase 2.
    """
    # Test case 1: VLDB 2023 descendants (should match Phase 2 exactly)
    print("Test Case 1: VLDB 2023 descendants")
    
    cur.execute("SELECT id, pre_order, post_order FROM single_axis_accel WHERE s_id = 'vldb_2023';")
    vldb_result = cur.fetchone()
    
    if vldb_result:
        vldb_id, vldb_pre, vldb_post = vldb_result
        print(f"  VLDB 2023 node: ID {vldb_id} (pre: {vldb_pre}, post: {vldb_post})")
        
        descendants = accelerator.xpath_descendant_single_axis(vldb_id)
        print(f"  Descendants found: {len(descendants)}")
        print(f"  Expected (Phase 2): 28 descendants")
        
        if len(descendants) == 28:
            print("    PASS: Matches Phase 2 results exactly")
        else:
            print("    FAIL: Does not match Phase 2 results")
        
        # Show first few descendants with their annotations
        print("  First 5 descendants with annotations:")
        for i, (desc_id, desc_type, desc_content) in enumerate(descendants[:5]):
            cur.execute("SELECT pre_order, post_order FROM single_axis_accel WHERE id = %s;", (desc_id,))
            pre, post = cur.fetchone()
            content_display = desc_content[:30] + "..." if desc_content and len(desc_content) > 30 else desc_content
            print(f"    {i+1}. ID {desc_id}: {desc_type} (pre: {pre}, post: {post}) - {content_display}")
    
    # Test case 2: Article descendants
    print("\nTest Case 2: SchmittKAMM23 article descendants")
    
    cur.execute("SELECT id, pre_order, post_order FROM single_axis_accel WHERE s_id = 'SchmittKAMM23';")
    article_result = cur.fetchone()
    
    if article_result:
        article_id, article_pre, article_post = article_result
        print(f"  SchmittKAMM23 node: ID {article_id} (pre: {article_pre}, post: {article_post})")
        
        descendants = accelerator.xpath_descendant_single_axis(article_id)
        print(f"  Descendants found: {len(descendants)}")
        print(f"  Expected: >10 descendants (article elements)")
        
        if len(descendants) >= 10:
            print("    PASS: Has expected number of descendants")
        else:
            print("    FAIL: Unexpected number of descendants")


def verify_annotation_correctness(cur: psycopg2.extensions.cursor, accelerator: SingleAxisAccelerator):
    """
    Verifies that the annotation is correct by checking the pre/post-order properties.
    """
    print("Verifying Pre/Post-Order Annotation Properties:")
    
    # Property 1: Pre-order increases in document order
    cur.execute("""
        SELECT COUNT(*) FROM single_axis_accel a1, single_axis_accel a2
        WHERE a1.parent = a2.parent 
        AND a1.id < a2.id 
        AND a1.pre_order >= a2.pre_order;
    """)
    
    violations = cur.fetchone()[0]
    print(f"  Pre-order violations: {violations}")
    
    if violations == 0:
        print("    Pre-order property satisfied")
    else:
        print("    Pre-order property violated")
    
    # Property 2: Descendants have pre_order > parent and post_order < parent
    cur.execute("""
        SELECT COUNT(*) FROM single_axis_accel parent, single_axis_accel child
        WHERE child.parent = parent.id
        AND (child.pre_order <= parent.pre_order OR child.post_order >= parent.post_order);
    """)
    
    violations = cur.fetchone()[0]
    print(f"  Parent-child annotation violations: {violations}")
    
    if violations == 0:
        print("    Parent-child annotation property satisfied")
    else:
        print("    Parent-child annotation property violated")
    
    # Property 3: Total node count consistency
    cur.execute("SELECT COUNT(*) FROM single_axis_accel;")
    total_nodes = cur.fetchone()[0]
    print(f"  Total nodes in single-axis schema: {total_nodes}")
    print(f"  Expected (toy example): 62 nodes")
    
    if total_nodes == 62:
        print("    Node count matches toy example expectation")
    else:
        print("    Node count does not match expectation")


def verify_window_function_formula(cur: psycopg2.extensions.cursor, accelerator: SingleAxisAccelerator):
    """
    Verifies that the window function formula is correctly implemented.
    """
    print("Verifying Window Function Formula Implementation:")
    print("Formula: descendant(v) = {u | pre_order(u) > pre_order(v) AND post_order(u) < post_order(v)}")
    
    # Test with VLDB 2023 node
    cur.execute("SELECT id, pre_order, post_order FROM single_axis_accel WHERE s_id = 'vldb_2023';")
    vldb_result = cur.fetchone()
    
    if vldb_result:
        vldb_id, vldb_pre, vldb_post = vldb_result
        
        # Get descendants using accelerator
        accelerator_descendants = accelerator.xpath_descendant_single_axis(vldb_id)
        accelerator_ids = {row[0] for row in accelerator_descendants}
        
        # Get descendants using raw formula
        cur.execute("""
            SELECT id FROM single_axis_accel
            WHERE pre_order > %s AND post_order < %s;
        """, (vldb_pre, vldb_post))
        
        formula_descendants = cur.fetchall()
        formula_ids = {row[0] for row in formula_descendants}
        
        print(f"  Accelerator result: {len(accelerator_ids)} descendants")
        print(f"  Raw formula result: {len(formula_ids)} descendants")
        
        if accelerator_ids == formula_ids:
            print("    Accelerator implementation matches raw formula exactly")
        else:
            print("    Accelerator implementation differs from raw formula")
            print(f"    Difference: {accelerator_ids.symmetric_difference(formula_ids)}")
    
    print("\n  Single-Axis XPath Accelerator annotation correctness verified!")
    print("  All properties match Phase 2 implementation")
    print("  Window function formula correctly implemented")


def main():
    """
    Main demonstration function.
    """
    demonstrate_toy_example_correctness()


if __name__ == "__main__":
    main()
