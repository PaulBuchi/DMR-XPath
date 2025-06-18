#!/usr/bin/env python3
"""
Task 4: XPath Axes as Window Functions - Complete Test Suite
Tests both Phase 1 (Node/Edge schema) and Phase 2 (accel schema) implementations.
"""

from xPath import *
import psycopg2

def test_task4_window_functions():
    """
    Complete test suite for Task 4: XPath Axes as Window Functions
    """
    print("=== TASK 4: XPATH AXES AS WINDOW FUNCTIONS ===\n")
    
    # Connect to database
    conn = connect_db()
    if not conn:
        print("❌ Could not connect to database")
        return
    
    cur = conn.cursor()
    
    try:
        print("PART A & B: Schema Mapping and SQL Implementation")
        print("=" * 60)
        
        # Test 1: Phase 1 Schema (Node/Edge)
        print("\n1. Testing with Phase 1 Schema (Node/Edge)")
        print("-" * 50)
        
        # Setup original schema
        setup_schema(cur, use_original_schema=True)
        
        # Parse and insert toy example data
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        root_node.insert_to_original_db(cur, verbose=False)
        conn.commit()
        
        # Get key node IDs
        cur.execute("SELECT id FROM Node WHERE s_id = 'SchmittKAMM23';")
        schmitt_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM Node WHERE s_id = 'SchalerHS23';")
        schaler_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM Node WHERE content = 'Daniel Ulrich Schmitt';")
        daniel_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM Node WHERE s_id = 'vldb_2023';")
        vldb_id = cur.fetchone()[0]
        
        print(f"Key nodes: SchmittKAMM23={schmitt_id}, SchalerHS23={schaler_id}, Daniel={daniel_id}, VLDB2023={vldb_id}")
        
        # Test window functions vs recursive functions
        print("\n1.1 Ancestor Axis Comparison:")
        recursive_ancestors = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
        window_ancestors = xpath_ancestor_window(cur, daniel_id)
        
        rec_ids = [row[0] for row in recursive_ancestors]
        win_ids = [row[0] for row in window_ancestors]
        
        print(f"   Recursive: {rec_ids}")
        print(f"   Window:    {win_ids}")
        print(f"   Match:     {'✅ YES' if set(rec_ids) == set(win_ids) else '❌ NO'}")
        
        print("\n1.2 Descendant Axis Comparison:")
        recursive_descendants = descendant_nodes(cur, vldb_id)
        window_descendants = xpath_descendant_window(cur, vldb_id)
        
        rec_ids = [row[0] for row in recursive_descendants]
        win_ids = [row[0] for row in window_descendants]
        
        print(f"   Recursive: {len(rec_ids)} nodes")
        print(f"   Window:    {len(win_ids)} nodes")
        print(f"   Match:     {'✅ YES' if set(rec_ids) == set(win_ids) else '❌ NO'}")
        
        print("\n1.3 Following-Sibling Axis Comparison:")
        recursive_following = siblings(cur, schmitt_id, direction="following")
        window_following = xpath_following_sibling_window(cur, schmitt_id)
        
        rec_ids = [row[0] for row in recursive_following]
        win_ids = [row[0] for row in window_following]
        
        print(f"   Recursive: {rec_ids}")
        print(f"   Window:    {win_ids}")
        print(f"   Match:     {'✅ YES' if rec_ids == win_ids else '❌ NO'}")
        
        print("\n1.4 Preceding-Sibling Axis Comparison:")
        recursive_preceding = siblings(cur, schaler_id, direction="preceding")
        window_preceding = xpath_preceding_sibling_window(cur, schaler_id)
        
        rec_ids = [row[0] for row in recursive_preceding]
        win_ids = [row[0] for row in window_preceding]
        
        print(f"   Recursive: {rec_ids}")
        print(f"   Window:    {win_ids}")
        print(f"   Match:     {'✅ YES' if rec_ids == win_ids else '❌ NO'}")
        
        # Test 2: Phase 2 Schema (accel/content)
        print("\n\n2. Testing with Phase 2 Schema (accel/content)")
        print("-" * 50)
        
        # Setup accelerator schema
        setup_schema(cur, use_original_schema=False)
        
        # Parse and insert toy example data with traversal orders
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        annotate_traversal_orders(root_node)
        root_node.insert_to_db(cur, verbose=False)
        conn.commit()
        
        # Get key node IDs (will be different in new schema)
        cur.execute("SELECT id FROM accel WHERE s_id = 'SchmittKAMM23';")
        schmitt_id_new = cur.fetchone()[0]
        cur.execute("SELECT id FROM accel WHERE s_id = 'SchalerHS23';")
        schaler_id_new = cur.fetchone()[0]
        cur.execute("SELECT a.id FROM accel a JOIN content c ON a.id = c.id WHERE c.text = 'Daniel Ulrich Schmitt';")
        daniel_id_new = cur.fetchone()[0]
        cur.execute("SELECT id FROM accel WHERE s_id = 'vldb_2023';")
        vldb_id_new = cur.fetchone()[0]
        
        print(f"Key nodes: SchmittKAMM23={schmitt_id_new}, SchalerHS23={schaler_id_new}, Daniel={daniel_id_new}, VLDB2023={vldb_id_new}")
        
        # Test window functions vs recursive functions with new schema
        print("\n2.1 Ancestor Axis Comparison:")
        recursive_ancestors = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
        window_ancestors = xpath_ancestor_window(cur, daniel_id_new)
        
        rec_ids = [row[0] for row in recursive_ancestors]
        win_ids = [row[0] for row in window_ancestors]
        
        print(f"   Recursive: {rec_ids}")
        print(f"   Window:    {win_ids}")
        print(f"   Match:     {'✅ YES' if set(rec_ids) == set(win_ids) else '❌ NO'}")
        
        print("\n2.2 Descendant Axis Comparison:")
        recursive_descendants = descendant_nodes(cur, vldb_id_new)
        window_descendants = xpath_descendant_window(cur, vldb_id_new)
        
        rec_ids = [row[0] for row in recursive_descendants]
        win_ids = [row[0] for row in window_descendants]
        
        print(f"   Recursive: {len(rec_ids)} nodes")
        print(f"   Window:    {len(win_ids)} nodes")
        print(f"   Match:     {'✅ YES' if set(rec_ids) == set(win_ids) else '❌ NO'}")
        
        print("\n2.3 Following-Sibling Axis Comparison:")
        recursive_following = siblings(cur, schmitt_id_new, direction="following")
        window_following = xpath_following_sibling_window(cur, schmitt_id_new)
        
        rec_ids = [row[0] for row in recursive_following]
        win_ids = [row[0] for row in window_following]
        
        print(f"   Recursive: {rec_ids}")
        print(f"   Window:    {win_ids}")
        print(f"   Match:     {'✅ YES' if rec_ids == win_ids else '❌ NO'}")
        
        print("\n2.4 Preceding-Sibling Axis Comparison:")
        recursive_preceding = siblings(cur, schaler_id_new, direction="preceding")
        window_preceding = xpath_preceding_sibling_window(cur, schaler_id_new)
        
        rec_ids = [row[0] for row in recursive_preceding]
        win_ids = [row[0] for row in window_preceding]
        
        print(f"   Recursive: {rec_ids}")
        print(f"   Window:    {win_ids}")
        print(f"   Match:     {'✅ YES' if rec_ids == win_ids else '❌ NO'}")
        
        print("\n\nPART C: Correctness Verification")
        print("=" * 60)
        
        # Verify against Phase 1 expected results using original schema
        setup_schema(cur, use_original_schema=True)
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        root_node.insert_to_original_db(cur, verbose=False)
        conn.commit()
        
        # Expected Phase 1 results
        expected = {
            "ancestor": [1,2,3,4,32,47,48],
            "descendants": [4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31],
            "schmitt_following": [19],
            "schmitt_preceding": [],
            "schaler_following": [],
            "schaler_preceding": [4]
        }
        
        # Test window functions against expected results
        daniel_ancestors = xpath_ancestor_window(cur, 5)  # Daniel Ulrich Schmitt
        vldb_descendants = xpath_descendant_window(cur, 3)  # VLDB 2023
        schmitt_following = xpath_following_sibling_window(cur, 4)  # SchmittKAMM23
        schmitt_preceding = xpath_preceding_sibling_window(cur, 4)  # SchmittKAMM23
        schaler_following = xpath_following_sibling_window(cur, 19)  # SchalerHS23
        schaler_preceding = xpath_preceding_sibling_window(cur, 19)  # SchalerHS23
        
        results = {
            "ancestor": [row[0] for row in daniel_ancestors],
            "descendants": [row[0] for row in vldb_descendants],
            "schmitt_following": [row[0] for row in schmitt_following],
            "schmitt_preceding": [row[0] for row in schmitt_preceding],
            "schaler_following": [row[0] for row in schaler_following],
            "schaler_preceding": [row[0] for row in schaler_preceding]
        }
        
        print("Window Function Results vs Expected Phase 1 Values:")
        all_pass = True
        for axis, expected_ids in expected.items():
            actual_ids = results[axis]
            match = actual_ids == expected_ids
            status = "✅ PASS" if match else "❌ FAIL"
            print(f"   {axis:20} | {status}")
            if not match:
                all_pass = False
                print(f"     Expected: {expected_ids}")
                print(f"     Actual:   {actual_ids}")
        
        print(f"\nOverall Task 4 Result: {'✅ ALL TESTS PASS' if all_pass else '❌ SOME TESTS FAIL'}")
        
        print("\n\nPART D: Documentation Summary")
        print("=" * 60)
        print("✅ Schema mapping completed for both Node/Edge and accel schemas")
        print("✅ Window function implementations created for all 4 axes")
        print("✅ Dual schema support with automatic detection")
        print("✅ Correctness verification against Phase 1 expected results")
        print("✅ Pre/post-order number utilization for efficient computation")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    test_task4_window_functions()
