#!/usr/bin/env python3
"""
Clean Phase 1 test that shows exact output for all XPath axis queries.
This is the definitive test for Phase 1 requirements.
"""

from xPath import *
import psycopg2

def phase1_clean_test():
    """
    Clean test that produces exact Phase 1 output.
    """
    print("=== PHASE 1 XPATH AXIS RESULTS ===\n")
    
    # Connect to database
    conn = connect_db()
    if not conn:
        print("❌ Could not connect to database")
        return
    
    cur = conn.cursor()
    
    try:
        # Setup original Node/Edge schema for Phase 1 compatibility
        setup_schema(cur, use_original_schema=True)
        
        # Parse and insert toy example data
        venues = parse_toy_example("toy_example.txt")
        root_node = build_edge_model(venues)
        root_node.insert_to_original_db(cur, verbose=False)
        conn.commit()
        
        print("Database setup complete with original Node/Edge schema.\n")
        
        # Show key node mappings
        print("Key Node Mappings:")
        print("-" * 30)
        cur.execute("""
            SELECT id, type, s_id, content 
            FROM Node 
            WHERE s_id IS NOT NULL OR content = 'Daniel Ulrich Schmitt'
            ORDER BY id;
        """)
        key_nodes = cur.fetchall()
        for node_id, node_type, s_id, content in key_nodes:
            if s_id:
                print(f"  {s_id}: Node ID = {node_id}")
            elif content == 'Daniel Ulrich Schmitt':
                print(f"  Daniel Ulrich Schmitt: Node ID = {node_id}")
        
        print("\n" + "="*60)
        print("XPATH AXIS QUERY RESULTS")
        print("="*60)
        
        # 1. Ancestor axis - Daniel Ulrich Schmitt
        print("\n1. ANCESTOR AXIS")
        print("Context: Daniel Ulrich Schmitt (author)")
        ancestors = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
        ancestor_ids = [row[0] for row in ancestors]
        print(f"Result Node IDs: {ancestor_ids}")
        print(f"Result Size: {len(ancestor_ids)}")
        
        # Show details of ancestors
        print("Ancestor Details:")
        for node_id, s_id, node_type, content in ancestors:
            s_id_str = s_id if s_id else "None"
            content_str = content if content else "None"
            print(f"  ID={node_id}: Type={node_type}, S_ID={s_id_str}, Content={content_str}")
        
        # 2. Descendant axis - VLDB 2023 year node
        print("\n2. DESCENDANT AXIS")
        cur.execute("SELECT id FROM Node WHERE s_id = 'vldb_2023';")
        vldb_node = cur.fetchone()
        if vldb_node:
            vldb_id = vldb_node[0]
            print(f"Context: VLDB 2023 year node (ID={vldb_id})")
            descendants = descendant_nodes(cur, vldb_id)
            descendant_ids = [row[0] for row in descendants]
            print(f"Result Node IDs: {descendant_ids}")
            print(f"Result Size: {len(descendant_ids)}")
        
        # 3. Following siblings - SchmittKAMM23
        print("\n3. FOLLOWING-SIBLING AXIS")
        cur.execute("SELECT id FROM Node WHERE s_id = 'SchmittKAMM23';")
        schmitt_node = cur.fetchone()
        if schmitt_node:
            schmitt_id = schmitt_node[0]
            print(f"Context: SchmittKAMM23 (ID={schmitt_id})")
            following = siblings(cur, schmitt_id, direction="following")
            following_ids = [row[0] for row in following]
            print(f"Result Node IDs: {following_ids}")
            print(f"Result Size: {len(following_ids)}")
            
            # Show details
            if following:
                for node_id, node_type, content in following:
                    cur.execute("SELECT s_id FROM Node WHERE id = %s;", (node_id,))
                    s_id = cur.fetchone()[0]
                    print(f"  ID={node_id}: Type={node_type}, S_ID={s_id}")
        
        # 4. Preceding siblings - SchmittKAMM23
        print("\n4. PRECEDING-SIBLING AXIS")
        if schmitt_node:
            print(f"Context: SchmittKAMM23 (ID={schmitt_id})")
            preceding = siblings(cur, schmitt_id, direction="preceding")
            preceding_ids = [row[0] for row in preceding]
            print(f"Result Node IDs: {preceding_ids}")
            print(f"Result Size: {len(preceding_ids)}")
        
        # 5. Following siblings - SchalerHS23
        print("\n5. FOLLOWING-SIBLING AXIS")
        cur.execute("SELECT id FROM Node WHERE s_id = 'SchalerHS23';")
        schaler_node = cur.fetchone()
        if schaler_node:
            schaler_id = schaler_node[0]
            print(f"Context: SchalerHS23 (ID={schaler_id})")
            following = siblings(cur, schaler_id, direction="following")
            following_ids = [row[0] for row in following]
            print(f"Result Node IDs: {following_ids}")
            print(f"Result Size: {len(following_ids)}")
        
        # 6. Preceding siblings - SchalerHS23
        print("\n6. PRECEDING-SIBLING AXIS")
        if schaler_node:
            print(f"Context: SchalerHS23 (ID={schaler_id})")
            preceding = siblings(cur, schaler_id, direction="preceding")
            preceding_ids = [row[0] for row in preceding]
            print(f"Result Node IDs: {preceding_ids}")
            print(f"Result Size: {len(preceding_ids)}")
            
            # Show details
            if preceding:
                for node_id, node_type, content in preceding:
                    cur.execute("SELECT s_id FROM Node WHERE id = %s;", (node_id,))
                    s_id = cur.fetchone()[0]
                    print(f"  ID={node_id}: Type={node_type}, S_ID={s_id}")
        
        print("\n" + "="*60)
        print("SUMMARY TABLE")
        print("="*60)
        print("Axis                    | Result Node IDs                                    | Size")
        print("-" * 80)
        print(f"ancestor                | {','.join(map(str, ancestor_ids)):50} | {len(ancestor_ids)}")
        print(f"descendants             | {','.join(map(str, descendant_ids)):50} | {len(descendant_ids)}")
        
        # Get all sibling results for summary
        schmitt_following_ids = [row[0] for row in siblings(cur, schmitt_id, direction="following")]
        schmitt_preceding_ids = [row[0] for row in siblings(cur, schmitt_id, direction="preceding")]
        schaler_following_ids = [row[0] for row in siblings(cur, schaler_id, direction="following")]
        schaler_preceding_ids = [row[0] for row in siblings(cur, schaler_id, direction="preceding")]
        
        schmitt_following_str = ','.join(map(str, schmitt_following_ids)) if schmitt_following_ids else "-"
        schmitt_preceding_str = ','.join(map(str, schmitt_preceding_ids)) if schmitt_preceding_ids else "-"
        schaler_following_str = ','.join(map(str, schaler_following_ids)) if schaler_following_ids else "-"
        schaler_preceding_str = ','.join(map(str, schaler_preceding_ids)) if schaler_preceding_ids else "-"
        
        print(f"following SchmittKAMM23 | {schmitt_following_str:50} | {len(schmitt_following_ids)}")
        print(f"preceding SchmittKAMM23 | {schmitt_preceding_str:50} | {len(schmitt_preceding_ids)}")
        print(f"following SchalerHS23   | {schaler_following_str:50} | {len(schaler_following_ids)}")
        print(f"preceding SchalerHS23   | {schaler_preceding_str:50} | {len(schaler_preceding_ids)}")
        
        # Verification against expected values
        expected = {
            "ancestor": [1,2,3,4,32,47,48],
            "descendants": [4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31],
            "schmitt_following": [19],
            "schmitt_preceding": [],
            "schaler_following": [],
            "schaler_preceding": [4]
        }
        
        print("\n" + "="*60)
        print("VERIFICATION")
        print("="*60)
        
        checks = [
            ("ancestor", ancestor_ids, expected["ancestor"]),
            ("descendants", descendant_ids, expected["descendants"]),
            ("following SchmittKAMM23", schmitt_following_ids, expected["schmitt_following"]),
            ("preceding SchmittKAMM23", schmitt_preceding_ids, expected["schmitt_preceding"]),
            ("following SchalerHS23", schaler_following_ids, expected["schaler_following"]),
            ("preceding SchalerHS23", schaler_preceding_ids, expected["schaler_preceding"])
        ]
        
        all_pass = True
        for name, actual, expected_val in checks:
            match = actual == expected_val
            status = "✅ PASS" if match else "❌ FAIL"
            print(f"{name:25} | {status}")
            if not match:
                all_pass = False
                print(f"  Expected: {expected_val}")
                print(f"  Actual:   {actual}")
        
        print(f"\nOverall Result: {'✅ ALL TESTS PASS' if all_pass else '❌ SOME TESTS FAIL'}")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    phase1_clean_test()
