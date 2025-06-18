#!/usr/bin/env python3
"""
Detailed test script for XPath window function implementations.
This script provides comprehensive verification of the window function approach.
"""

from xPath import *
import psycopg2

def detailed_window_function_test():
    """
    Performs detailed testing of window function implementations.
    """
    print("=== Detailed XPath Window Function Verification ===\n")
    
    # Connect to database
    conn = connect_db()
    if not conn:
        print("❌ Could not connect to database")
        return
    
    cur = conn.cursor()
    
    try:
        # Setup schema and data
        setup_schema(cur)
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        annotate_traversal_orders(root_node)
        root_node.insert_to_db(cur, verbose=False)
        conn.commit()
        
        print("✅ Database setup complete\n")
        
        # Test specific nodes with known relationships
        test_cases = [
            {
                "name": "SchmittKAMM23 Publication",
                "s_id": "SchmittKAMM23",
                "expected_descendants": 14,  # All child elements
                "expected_following_siblings": 1,  # SchalerHS23
                "expected_preceding_siblings": 0   # First article in VLDB 2023
            },
            {
                "name": "SchalerHS23 Publication", 
                "s_id": "SchalerHS23",
                "expected_descendants": 12,  # All child elements
                "expected_following_siblings": 0,  # Last article in VLDB 2023
                "expected_preceding_siblings": 1   # SchmittKAMM23
            },
            {
                "name": "VLDB 2023 Year Node",
                "s_id": "vldb_2023",
                "expected_descendants": 28,  # 2 articles + their children
                "expected_following_siblings": 0,  # Only year in VLDB venue
                "expected_preceding_siblings": 0   # Only year in VLDB venue
            }
        ]
        
        for test_case in test_cases:
            print(f"Testing: {test_case['name']}")
            print("-" * 60)
            
            # Get node ID
            cur.execute("SELECT id, pre_order, post_order, parent FROM accel WHERE s_id = %s;", 
                       (test_case['s_id'],))
            result = cur.fetchone()
            
            if not result:
                print(f"❌ Node {test_case['s_id']} not found!")
                continue
                
            node_id, pre_order, post_order, parent_id = result
            print(f"Node ID: {node_id}, Pre: {pre_order}, Post: {post_order}, Parent: {parent_id}")
            
            # Test 1: Ancestor Axis
            print("\n1. Ancestor Axis:")
            ancestors = xpath_ancestor_window(cur, node_id)
            print(f"   Found {len(ancestors)} ancestors")
            
            # Verify ancestor properties
            for ancestor_id, ancestor_type, ancestor_content in ancestors:
                cur.execute("SELECT pre_order, post_order FROM accel WHERE id = %s;", (ancestor_id,))
                anc_pre, anc_post = cur.fetchone()
                
                # Check ancestor property: pre(ancestor) < pre(node) < post(node) < post(ancestor)
                if not (anc_pre < pre_order and post_order < anc_post):
                    print(f"   ❌ Ancestor property violation: {ancestor_id}")
                    break
            else:
                print("   ✅ All ancestor properties verified")
            
            # Test 2: Descendant Axis
            print("\n2. Descendant Axis:")
            descendants = xpath_descendant_window(cur, node_id)
            actual_desc = len(descendants)
            expected_desc = test_case['expected_descendants']
            
            print(f"   Expected: {expected_desc}, Actual: {actual_desc}")
            
            if actual_desc == expected_desc:
                print("   ✅ Descendant count matches expected")
            else:
                print("   ⚠️  Descendant count differs from expected")
            
            # Verify descendant properties
            for desc_id, desc_type, desc_content in descendants:
                cur.execute("SELECT pre_order, post_order FROM accel WHERE id = %s;", (desc_id,))
                desc_pre, desc_post = cur.fetchone()
                
                # Check descendant property: pre(node) < pre(desc) < post(desc) < post(node)
                if not (pre_order < desc_pre and desc_post < post_order):
                    print(f"   ❌ Descendant property violation: {desc_id}")
                    break
            else:
                print("   ✅ All descendant properties verified")
            
            # Test 3: Following-Sibling Axis
            print("\n3. Following-Sibling Axis:")
            following_siblings = xpath_following_sibling_window(cur, node_id)
            actual_following = len(following_siblings)
            expected_following = test_case['expected_following_siblings']
            
            print(f"   Expected: {expected_following}, Actual: {actual_following}")
            
            if actual_following == expected_following:
                print("   ✅ Following-sibling count matches expected")
            else:
                print("   ⚠️  Following-sibling count differs from expected")
            
            # Verify sibling properties
            for sib_id, sib_type, sib_content in following_siblings:
                cur.execute("SELECT parent, pre_order FROM accel WHERE id = %s;", (sib_id,))
                sib_parent, sib_pre = cur.fetchone()
                
                # Check sibling property: same parent and pre(node) < pre(sibling)
                if sib_parent != parent_id or sib_pre <= pre_order:
                    print(f"   ❌ Following-sibling property violation: {sib_id}")
                    break
            else:
                print("   ✅ All following-sibling properties verified")
            
            # Test 4: Preceding-Sibling Axis
            print("\n4. Preceding-Sibling Axis:")
            preceding_siblings = xpath_preceding_sibling_window(cur, node_id)
            actual_preceding = len(preceding_siblings)
            expected_preceding = test_case['expected_preceding_siblings']
            
            print(f"   Expected: {expected_preceding}, Actual: {actual_preceding}")
            
            if actual_preceding == expected_preceding:
                print("   ✅ Preceding-sibling count matches expected")
            else:
                print("   ⚠️  Preceding-sibling count differs from expected")
            
            # Verify sibling properties
            for sib_id, sib_type, sib_content in preceding_siblings:
                cur.execute("SELECT parent, pre_order FROM accel WHERE id = %s;", (sib_id,))
                sib_parent, sib_pre = cur.fetchone()
                
                # Check sibling property: same parent and pre(sibling) < pre(node)
                if sib_parent != parent_id or sib_pre >= pre_order:
                    print(f"   ❌ Preceding-sibling property violation: {sib_id}")
                    break
            else:
                print("   ✅ All preceding-sibling properties verified")
            
            print("\n" + "="*60 + "\n")
        
        # Performance comparison test
        print("Performance Comparison Test:")
        print("-" * 30)
        
        import time
        
        # Test with VLDB 2023 node (has many descendants)
        cur.execute("SELECT id FROM accel WHERE s_id = 'vldb_2023';")
        vldb_node_id = cur.fetchone()[0]
        
        # Time window function approach
        start_time = time.time()
        for _ in range(10):
            xpath_descendant_window(cur, vldb_node_id)
        window_time = time.time() - start_time
        
        # Time recursive approach
        start_time = time.time()
        for _ in range(10):
            descendant_nodes(cur, vldb_node_id)
        recursive_time = time.time() - start_time
        
        print(f"Window function (10 runs): {window_time:.4f} seconds")
        print(f"Recursive method (10 runs): {recursive_time:.4f} seconds")
        print(f"Speedup: {recursive_time/window_time:.2f}x")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    detailed_window_function_test()
