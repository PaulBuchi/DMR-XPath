#!/usr/bin/env python3
"""
Debug Phase 2 Window Functions - Comprehensive Analysis
This script analyzes the Phase 2 window function results and maps them to Phase 1 equivalents.
"""

from xPath import *
import psycopg2

def debug_phase2_window_functions():
    """
    Debug and verify Phase 2 window function implementations.
    """
    print("=== PHASE 2 WINDOW FUNCTIONS DEBUG ===\n")
    
    # Connect to database
    conn = connect_db()
    if not conn:
        print("❌ Could not connect to database")
        return
    
    cur = conn.cursor()
    
    try:
        # Step 1: Test Phase 1 to get baseline expected results
        print("1. PHASE 1 BASELINE RESULTS")
        print("-" * 50)
        
        setup_schema(cur, use_original_schema=True)
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        root_node.insert_to_original_db(cur, verbose=False)
        conn.commit()
        
        # Get Phase 1 results
        phase1_results = {}
        
        # Ancestors
        ancestors = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
        phase1_results["ancestors"] = [row[0] for row in ancestors]
        
        # Descendants
        descendants = descendant_nodes(cur, 3)  # VLDB 2023 year node
        phase1_results["descendants"] = [row[0] for row in descendants]
        
        # Siblings
        schmitt_following = siblings(cur, 4, direction="following")  # SchmittKAMM23
        phase1_results["schmitt_following"] = [row[0] for row in schmitt_following]
        
        schmitt_preceding = siblings(cur, 4, direction="preceding")
        phase1_results["schmitt_preceding"] = [row[0] for row in schmitt_preceding]
        
        schaler_following = siblings(cur, 19, direction="following")  # SchalerHS23
        phase1_results["schaler_following"] = [row[0] for row in schaler_following]
        
        schaler_preceding = siblings(cur, 19, direction="preceding")
        phase1_results["schaler_preceding"] = [row[0] for row in schaler_preceding]
        
        print("Phase 1 Results:")
        for key, value in phase1_results.items():
            print(f"  {key}: {value} (count: {len(value)})")
        
        # Step 2: Test Phase 2 with same logical queries
        print("\n2. PHASE 2 RESULTS")
        print("-" * 50)
        
        setup_schema(cur, use_original_schema=False)
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        annotate_traversal_orders(root_node)
        root_node.insert_to_db(cur, verbose=False)
        conn.commit()
        
        # Get Phase 2 node mappings
        cur.execute("SELECT id FROM accel WHERE s_id = 'SchmittKAMM23';")
        schmitt_id_p2 = cur.fetchone()[0]
        
        cur.execute("SELECT id FROM accel WHERE s_id = 'SchalerHS23';")
        schaler_id_p2 = cur.fetchone()[0]
        
        cur.execute("SELECT a.id FROM accel a JOIN content c ON a.id = c.id WHERE c.text = 'Daniel Ulrich Schmitt';")
        daniel_id_p2 = cur.fetchone()[0]
        
        cur.execute("SELECT id FROM accel WHERE s_id = 'vldb_2023';")
        vldb_id_p2 = cur.fetchone()[0]
        
        print(f"Phase 2 Node Mappings:")
        print(f"  SchmittKAMM23: {schmitt_id_p2} (Phase 1: 4)")
        print(f"  SchalerHS23: {schaler_id_p2} (Phase 1: 19)")
        print(f"  Daniel Ulrich Schmitt: {daniel_id_p2} (Phase 1: 5)")
        print(f"  VLDB 2023: {vldb_id_p2} (Phase 1: 3)")
        
        # Test Phase 2 results
        phase2_results = {}
        
        # Ancestors
        ancestors_p2 = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
        phase2_results["ancestors"] = [row[0] for row in ancestors_p2]
        
        # Descendants
        descendants_p2 = descendant_nodes(cur, vldb_id_p2)
        phase2_results["descendants"] = [row[0] for row in descendants_p2]
        
        # Siblings
        schmitt_following_p2 = siblings(cur, schmitt_id_p2, direction="following")
        phase2_results["schmitt_following"] = [row[0] for row in schmitt_following_p2]
        
        schmitt_preceding_p2 = siblings(cur, schmitt_id_p2, direction="preceding")
        phase2_results["schmitt_preceding"] = [row[0] for row in schmitt_preceding_p2]
        
        schaler_following_p2 = siblings(cur, schaler_id_p2, direction="following")
        phase2_results["schaler_following"] = [row[0] for row in schaler_following_p2]
        
        schaler_preceding_p2 = siblings(cur, schaler_id_p2, direction="preceding")
        phase2_results["schaler_preceding"] = [row[0] for row in schaler_preceding_p2]
        
        print("\nPhase 2 Recursive Results:")
        for key, value in phase2_results.items():
            print(f"  {key}: {value} (count: {len(value)})")
        
        # Step 3: Test Phase 2 Window Functions
        print("\n3. PHASE 2 WINDOW FUNCTION RESULTS")
        print("-" * 50)
        
        # Window function results
        window_results = {}
        
        # Ancestors
        ancestors_win = xpath_ancestor_window(cur, daniel_id_p2)
        window_results["ancestors"] = [row[0] for row in ancestors_win]
        
        # Descendants
        descendants_win = xpath_descendant_window(cur, vldb_id_p2)
        window_results["descendants"] = [row[0] for row in descendants_win]
        
        # Siblings
        schmitt_following_win = xpath_following_sibling_window(cur, schmitt_id_p2)
        window_results["schmitt_following"] = [row[0] for row in schmitt_following_win]
        
        schmitt_preceding_win = xpath_preceding_sibling_window(cur, schmitt_id_p2)
        window_results["schmitt_preceding"] = [row[0] for row in schmitt_preceding_win]
        
        schaler_following_win = xpath_following_sibling_window(cur, schaler_id_p2)
        window_results["schaler_following"] = [row[0] for row in schaler_following_win]
        
        schaler_preceding_win = xpath_preceding_sibling_window(cur, schaler_id_p2)
        window_results["schaler_preceding"] = [row[0] for row in schaler_preceding_win]
        
        print("Phase 2 Window Function Results:")
        for key, value in window_results.items():
            print(f"  {key}: {value} (count: {len(value)})")
        
        # Step 4: Comparison and Verification
        print("\n4. COMPARISON AND VERIFICATION")
        print("-" * 50)
        
        # Expected counts from Phase 1
        expected_counts = {
            "ancestors": 7,
            "descendants": 28,
            "schmitt_following": 1,
            "schmitt_preceding": 0,
            "schaler_following": 0,
            "schaler_preceding": 1
        }
        
        print("Count Comparison:")
        print("Axis                    | Phase 1 | Phase 2 Rec | Phase 2 Win | Expected | Status")
        print("-" * 85)
        
        all_correct = True
        for axis in expected_counts.keys():
            p1_count = len(phase1_results[axis])
            p2_rec_count = len(phase2_results[axis])
            p2_win_count = len(window_results[axis])
            expected = expected_counts[axis]
            
            # Check if all counts match expected
            counts_match = (p1_count == expected and p2_rec_count == expected and p2_win_count == expected)
            status = "✅ PASS" if counts_match else "❌ FAIL"
            
            if not counts_match:
                all_correct = False
            
            print(f"{axis:22} | {p1_count:7} | {p2_rec_count:11} | {p2_win_count:11} | {expected:8} | {status}")
        
        # Check if Phase 2 recursive and window functions match
        print("\nPhase 2 Recursive vs Window Function Comparison:")
        for axis in expected_counts.keys():
            rec_set = set(phase2_results[axis])
            win_set = set(window_results[axis])
            match = rec_set == win_set
            status = "✅ MATCH" if match else "❌ DIFFER"
            print(f"  {axis:22} | {status}")
            if not match:
                print(f"    Recursive: {sorted(rec_set)}")
                print(f"    Window:    {sorted(win_set)}")
        
        print(f"\nOverall Result: {'✅ ALL TESTS PASS' if all_correct else '❌ SOME TESTS FAIL'}")
        
        # Step 5: Detailed Analysis if there are issues
        if not all_correct:
            print("\n5. DETAILED ISSUE ANALYSIS")
            print("-" * 50)
            
            for axis in expected_counts.keys():
                if len(phase2_results[axis]) != expected_counts[axis] or len(window_results[axis]) != expected_counts[axis]:
                    print(f"\n{axis.upper()} ISSUE ANALYSIS:")
                    print(f"  Expected count: {expected_counts[axis]}")
                    print(f"  Phase 1 result: {phase1_results[axis]}")
                    print(f"  Phase 2 recursive: {phase2_results[axis]}")
                    print(f"  Phase 2 window: {window_results[axis]}")
        
    except Exception as e:
        print(f"❌ Debug failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    debug_phase2_window_functions()
