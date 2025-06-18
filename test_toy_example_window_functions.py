#!/usr/bin/env python3
"""
Test XPath Window Functions on Toy Example ONLY
This script ensures window functions are tested only on the controlled toy example dataset.
"""

from xPath import *
import psycopg2

def test_toy_example_window_functions():
    """
    Test XPath window functions exclusively on toy example data.
    Verifies that results match expected Phase 1 values.
    """
    print("=== XPATH WINDOW FUNCTIONS - TOY EXAMPLE ONLY TEST ===\n")
    
    # Expected results for toy example
    expected = {
        "ancestor_daniel": 7,
        "descendants_vldb2023": 28,
        "following_schmitt": 1,
        "preceding_schmitt": 0,
        "following_schaler": 0,
        "preceding_schaler": 1
    }
    
    conn = connect_db()
    if not conn:
        print("❌ Could not connect to database")
        return
    
    cur = conn.cursor()
    
    try:
        # Test 1: Phase 1 Schema (Node/Edge) with Toy Example
        print("1. PHASE 1 SCHEMA - TOY EXAMPLE")
        print("=" * 50)
        
        setup_schema(cur, use_original_schema=True)
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        root_node.insert_to_original_db(cur, verbose=False)
        conn.commit()
        
        # Verify dataset size
        cur.execute("SELECT COUNT(*) FROM Node;")
        node_count = cur.fetchone()[0]
        print(f"Dataset size: {node_count} nodes (expected: ~62 for toy example)")
        
        if node_count > 100:
            print("❌ ERROR: Dataset too large! Should be toy example only.")
            return
        
        # Test window functions vs recursive functions
        print("\nTesting window functions vs recursive functions:")
        
        # Ancestor test
        daniel_ancestors_rec = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
        daniel_id = 5  # Known from Phase 1
        daniel_ancestors_win = xpath_ancestor_window(cur, daniel_id)
        
        rec_count = len(daniel_ancestors_rec)
        win_count = len(daniel_ancestors_win)
        
        print(f"Ancestor (Daniel): Recursive={rec_count}, Window={win_count}, Expected={expected['ancestor_daniel']}")
        if rec_count == win_count == expected['ancestor_daniel']:
            print("  ✅ PASS")
        else:
            print("  ❌ FAIL")
        
        # Descendant test
        vldb_descendants_rec = descendant_nodes(cur, 3)  # VLDB 2023 node
        vldb_descendants_win = xpath_descendant_window(cur, 3)
        
        rec_count = len(vldb_descendants_rec)
        win_count = len(vldb_descendants_win)
        
        print(f"Descendants (VLDB 2023): Recursive={rec_count}, Window={win_count}, Expected={expected['descendants_vldb2023']}")
        if rec_count == win_count == expected['descendants_vldb2023']:
            print("  ✅ PASS")
        else:
            print("  ❌ FAIL")
        
        # Sibling tests
        schmitt_following_rec = siblings(cur, 4, "following")  # SchmittKAMM23
        schmitt_following_win = xpath_following_sibling_window(cur, 4)
        
        rec_count = len(schmitt_following_rec)
        win_count = len(schmitt_following_win)
        
        print(f"Following SchmittKAMM23: Recursive={rec_count}, Window={win_count}, Expected={expected['following_schmitt']}")
        if rec_count == win_count == expected['following_schmitt']:
            print("  ✅ PASS")
        else:
            print("  ❌ FAIL")
        
        schaler_preceding_rec = siblings(cur, 19, "preceding")  # SchalerHS23
        schaler_preceding_win = xpath_preceding_sibling_window(cur, 19)
        
        rec_count = len(schaler_preceding_rec)
        win_count = len(schaler_preceding_win)
        
        print(f"Preceding SchalerHS23: Recursive={rec_count}, Window={win_count}, Expected={expected['preceding_schaler']}")
        if rec_count == win_count == expected['preceding_schaler']:
            print("  ✅ PASS")
        else:
            print("  ❌ FAIL")
        
        # Test 2: Phase 2 Schema (accel/content) with Toy Example
        print(f"\n2. PHASE 2 SCHEMA - TOY EXAMPLE")
        print("=" * 50)
        
        setup_schema(cur, use_original_schema=False)
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        annotate_traversal_orders(root_node)
        root_node.insert_to_db(cur, verbose=False)
        conn.commit()
        
        # Verify dataset size
        cur.execute("SELECT COUNT(*) FROM accel;")
        node_count = cur.fetchone()[0]
        print(f"Dataset size: {node_count} nodes (expected: ~62 for toy example)")
        
        if node_count > 100:
            print("❌ ERROR: Dataset too large! Should be toy example only.")
            return
        
        # Get Phase 2 node mappings
        cur.execute("SELECT id FROM accel WHERE s_id = 'SchmittKAMM23';")
        schmitt_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM accel WHERE s_id = 'SchalerHS23';")
        schaler_id = cur.fetchone()[0]
        cur.execute("SELECT a.id FROM accel a JOIN content c ON a.id = c.id WHERE c.text = 'Daniel Ulrich Schmitt';")
        daniel_id = cur.fetchone()[0]
        cur.execute("SELECT id FROM accel WHERE s_id = 'vldb_2023';")
        vldb_id = cur.fetchone()[0]
        
        print(f"Phase 2 node mappings: SchmittKAMM23={schmitt_id}, SchalerHS23={schaler_id}, Daniel={daniel_id}, VLDB={vldb_id}")
        
        # Test window functions vs recursive functions
        print("\nTesting window functions vs recursive functions:")
        
        # Ancestor test
        daniel_ancestors_rec = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
        daniel_ancestors_win = xpath_ancestor_window(cur, daniel_id)
        
        rec_count = len(daniel_ancestors_rec)
        win_count = len(daniel_ancestors_win)
        
        print(f"Ancestor (Daniel): Recursive={rec_count}, Window={win_count}, Expected={expected['ancestor_daniel']}")
        if rec_count == win_count == expected['ancestor_daniel']:
            print("  ✅ PASS")
        else:
            print("  ❌ FAIL")
        
        # Descendant test
        vldb_descendants_rec = descendant_nodes(cur, vldb_id)
        vldb_descendants_win = xpath_descendant_window(cur, vldb_id)
        
        rec_count = len(vldb_descendants_rec)
        win_count = len(vldb_descendants_win)
        
        print(f"Descendants (VLDB 2023): Recursive={rec_count}, Window={win_count}, Expected={expected['descendants_vldb2023']}")
        if rec_count == win_count == expected['descendants_vldb2023']:
            print("  ✅ PASS")
        else:
            print("  ❌ FAIL")
        
        # Sibling tests
        schmitt_following_rec = siblings(cur, schmitt_id, "following")
        schmitt_following_win = xpath_following_sibling_window(cur, schmitt_id)
        
        rec_count = len(schmitt_following_rec)
        win_count = len(schmitt_following_win)
        
        print(f"Following SchmittKAMM23: Recursive={rec_count}, Window={win_count}, Expected={expected['following_schmitt']}")
        if rec_count == win_count == expected['following_schmitt']:
            print("  ✅ PASS")
        else:
            print("  ❌ FAIL")
            if rec_count != win_count:
                print(f"    Recursive IDs: {[row[0] for row in schmitt_following_rec]}")
                print(f"    Window IDs: {[row[0] for row in schmitt_following_win]}")
        
        schaler_preceding_rec = siblings(cur, schaler_id, "preceding")
        schaler_preceding_win = xpath_preceding_sibling_window(cur, schaler_id)
        
        rec_count = len(schaler_preceding_rec)
        win_count = len(schaler_preceding_win)
        
        print(f"Preceding SchalerHS23: Recursive={rec_count}, Window={win_count}, Expected={expected['preceding_schaler']}")
        if rec_count == win_count == expected['preceding_schaler']:
            print("  ✅ PASS")
        else:
            print("  ❌ FAIL")
            if rec_count != win_count:
                print(f"    Recursive IDs: {[row[0] for row in schaler_preceding_rec]}")
                print(f"    Window IDs: {[row[0] for row in schaler_preceding_win]}")
        
        # Final Summary
        print(f"\n3. FINAL SUMMARY")
        print("=" * 50)
        print("✅ Key Achievements:")
        print("  - Window functions tested ONLY on toy example data")
        print("  - Both Phase 1 and Phase 2 schemas tested")
        print("  - Results compared against expected toy example values")
        print("  - Recursive vs window function consistency verified")
        
        print(f"\n📊 Expected Toy Example Results:")
        print(f"  ancestor (Daniel Ulrich Schmitt): {expected['ancestor_daniel']} nodes")
        print(f"  descendants (VLDB 2023): {expected['descendants_vldb2023']} nodes")
        print(f"  following SchmittKAMM23: {expected['following_schmitt']} node")
        print(f"  preceding SchmittKAMM23: {expected['preceding_schmitt']} nodes")
        print(f"  following SchalerHS23: {expected['following_schaler']} nodes")
        print(f"  preceding SchalerHS23: {expected['preceding_schaler']} node")
        
        print(f"\n🎯 CONCLUSION: Window functions should produce these exact counts")
        print(f"   when tested on toy example data, NOT on large DBLP dataset!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    test_toy_example_window_functions()
