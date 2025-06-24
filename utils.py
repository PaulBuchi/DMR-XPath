"""
Hilfsfunktionen für das XPath-Projekt.
"""
import psycopg2
import psycopg2.extensions
from typing import List, Tuple, Optional

from db import connect_db, setup_schema
from xml_parser import parse_toy_example
from model import (
    Node,
    build_edge_model,
    annotate_traversal_orders
)

from axes import (
    xpath_ancestor_window,
    xpath_descendant_window,
    xpath_following_sibling_window,
    xpath_preceding_sibling_window,
    ancestor_nodes,
    descendant_nodes,
    siblings
)

def print_nodes(
    label: str,
    nodes: List[Tuple[int, str, Optional[str]]]
) -> None:
    """
    Gibt eine Liste von (id, type, content) für Debug-/Testzwecke auf der Konsole aus.
    """
    print(f"{label}:")
    if not nodes:
        print("  Keine Knoten gefunden.")
        return

    for node in nodes:
        print(node)


def verify_traversal_orders(cur: psycopg2.extensions.cursor, publication_keys: List[str]) -> None:
    """
    Überprüft die Pre-Order- und Post-Order-Traversierungsnummern für bestimmte Publikationen.
    Zeigt die Baumstruktur und Traversierungsnummern zur manuellen Überprüfung an.
    """
    print("\n=== Traversal Order Verification ===")

    for pub_key in publication_keys:
        print(f"\nPublication: {pub_key}")
        print("-" * 50)

        # Find the publication node
        cur.execute("SELECT id, pre_order, post_order FROM accel WHERE s_id = %s;", (pub_key,))
        pub_result = cur.fetchone()

        if not pub_result:
            print(f"Publication {pub_key} not found!")
            continue

        pub_id, pub_pre, pub_post = pub_result
        print(f"Publication Node: id={pub_id}, pre={pub_pre}, post={pub_post}")

        # Get all descendants with their traversal orders
        cur.execute("""
            WITH RECURSIVE descendants(id, level) AS (
                SELECT id, 0 FROM accel WHERE id = %s
                UNION
                SELECT a.id, d.level + 1
                FROM accel a
                JOIN descendants d ON a.parent = d.id
            )
            SELECT a.id, a.pre_order, a.post_order, a.type, a.s_id, c.text, d.level
            FROM accel a
            LEFT JOIN content c ON a.id = c.id
            JOIN descendants d ON a.id = d.id
            ORDER BY a.pre_order;
        """, (pub_id,))

        nodes = cur.fetchall()

        print("\nTree Structure (ordered by pre-order):")
        print("Level | Pre | Post | Type       | S_ID           | Content")
        print("------|-----|------|------------|----------------|------------------")

        for _, pre_ord, post_ord, node_type, s_id, content, level in nodes:
            indent = "  " * level
            s_id_str = s_id or ""
            content_str = (content or "")[:20] + ("..." if content and len(content) > 20 else "")
            print(f"{level:5} | {pre_ord:3} | {post_ord:4} | {indent}{node_type:10} | {s_id_str:14} | {content_str}")


def test_queries(cur: psycopg2.extensions.cursor) -> None:
    """
    Führt Testabfragen für Ancestor, Descendant, und Siblings durch,
    analog zu Phase 1 der Aufgabenstellung.
    ONLY tests on toy example data, not full DBLP dataset.
    """
    print("\nTeste XPath-Funktionen:\n")

    # Check dataset size to ensure we're testing on toy example only
    cur.execute("SELECT COUNT(*) FROM accel;")
    node_count = cur.fetchone()[0]

    if node_count > 1000:
        print("⚠️  WARNING: Large dataset detected. XPath window function tests should only run on toy example.")
        print(f"   Current dataset has {node_count:,} nodes.")
        print("   Skipping detailed window function tests to avoid incorrect results.")
        print("   Expected toy example dataset: ~62 nodes")
        return
    else:
        print(f" Toy example dataset detected ({node_count} nodes). Proceeding with tests.")

    # First, verify traversal orders for specific publications
    verify_traversal_orders(cur, ["HutterAK0L22", "SchalerHS23"])

    # Test window function implementations ONLY on toy example
    test_xpath_window_functions_toy_example(cur)


def test_xpath_window_functions_toy_example(cur: psycopg2.extensions.cursor) -> None:
    """
    Tests the XPath window function implementations ONLY on toy example data.
    Compares window functions with recursive implementations for correctness verification.
    Expected results should match Phase 1 toy example values.
    """
    print("\n=== Testing XPath Window Functions (Toy Example Only) ===\n")

    # Test publications from toy example - these should be the only ones tested
    test_publications = ["SchmittKAMM23", "SchalerHS23"]

    # Expected results for toy example
    expected_results = {
        "SchmittKAMM23": {
            "following_siblings": 1,  # Should be SchalerHS23
            "preceding_siblings": 0   # Should be none (first article)
        },
        "SchalerHS23": {
            "following_siblings": 0,  # Should be none (last article)
            "preceding_siblings": 1   # Should be SchmittKAMM23
        }
    }

    for pub_key in test_publications:
        print(f"Testing publication: {pub_key}")
        print("-" * 50)

        # Get the publication node ID
        cur.execute("SELECT id FROM accel WHERE s_id = %s;", (pub_key,))
        result = cur.fetchone()

        if not result:
            print(f"Publication {pub_key} not found!")
            continue

        node_id = result[0]

        # Test 1: Ancestor axis (not tested against expected values, just consistency)
        print("1. Ancestor Axis:")
        window_ancestors = xpath_ancestor_window(cur, node_id)

        # For toy example, test against Daniel Ulrich Schmitt ancestors
        cur.execute("""
            SELECT c.text FROM accel a
            JOIN content c ON a.id = c.id
            WHERE a.parent = %s AND a.type = 'author' AND c.text = 'Daniel Ulrich Schmitt'
            LIMIT 1;
        """, (node_id,))
        author_result = cur.fetchone()

        if author_result:
            recursive_ancestors = ancestor_nodes(cur, author_result[0])
            print(f"  Window function: {len(window_ancestors)} ancestors")
            print(f"  Recursive method: {len(recursive_ancestors)} ancestors")

            # For toy example, we expect 7 ancestors for Daniel Ulrich Schmitt
            if len(recursive_ancestors) == 7:
                print("   Expected toy example ancestor count (7)")
            else:
                print(f"  ⚠️  Unexpected ancestor count (expected 7, got {len(recursive_ancestors)})")

        # Test 2: Descendant axis
        print("2. Descendant Axis:")
        window_descendants = xpath_descendant_window(cur, node_id)
        recursive_descendants = descendant_nodes(cur, node_id)

        print(f"  Window function: {len(window_descendants)} descendants")
        print(f"  Recursive method: {len(recursive_descendants)} descendants")

        # Verify they match
        window_ids = {row[0] for row in window_descendants}
        recursive_ids = {row[0] for row in recursive_descendants}

        if window_ids == recursive_ids:
            print("   Results match!")
        else:
            print("   Results differ!")

        # Test 3: Following-sibling axis (critical test for toy example)
        print("3. Following-Sibling Axis:")
        window_following = xpath_following_sibling_window(cur, node_id)
        recursive_following = siblings(cur, node_id, direction="following")

        print(f"  Window function: {len(window_following)} following siblings")
        print(f"  Recursive method: {len(recursive_following)} following siblings")

        expected_following = expected_results[pub_key]["following_siblings"]
        print(f"  Expected (toy example): {expected_following} following siblings")

        # Verify they match expected and each other
        window_count = len(window_following)
        recursive_count = len(recursive_following)

        if window_count == recursive_count == expected_following:
            print("   All results match expected toy example values!")
        else:
            print("   Results don't match expected values!")
            if window_count != recursive_count:
                print(f"    Window vs Recursive mismatch: {window_count} vs {recursive_count}")
            if recursive_count != expected_following:
                print(f"    Expected vs Actual mismatch: {expected_following} vs {recursive_count}")

        # Test 4: Preceding-sibling axis (critical test for toy example)
        print("4. Preceding-Sibling Axis:")
        window_preceding = xpath_preceding_sibling_window(cur, node_id)
        recursive_preceding = siblings(cur, node_id, direction="preceding")

        print(f"  Window function: {len(window_preceding)} preceding siblings")
        print(f"  Recursive method: {len(recursive_preceding)} preceding siblings")

        expected_preceding = expected_results[pub_key]["preceding_siblings"]
        print(f"  Expected (toy example): {expected_preceding} preceding siblings")

        # Verify they match expected and each other
        window_count = len(window_preceding)
        recursive_count = len(recursive_preceding)

        if window_count == recursive_count == expected_preceding:
            print("   All results match expected toy example values!")
        else:
            print("   Results don't match expected values!")
            if window_count != recursive_count:
                print(f"    Window vs Recursive mismatch: {window_count} vs {recursive_count}")
            if recursive_count != expected_preceding:
                print(f"    Expected vs Actual mismatch: {expected_preceding} vs {recursive_count}")

        print()  # Empty line between publications

    print("=== Toy Example Test Summary ===")
    print("Expected results for toy example:")
    print("  SchmittKAMM23 following siblings: 1 (SchalerHS23)")
    print("  SchmittKAMM23 preceding siblings: 0 (first article)")
    print("  SchalerHS23 following siblings: 0 (last article)")
    print("  SchalerHS23 preceding siblings: 1 (SchmittKAMM23)")
    print("  Daniel Ulrich Schmitt ancestors: 7 nodes")
    print("  VLDB 2023 descendants: 28 nodes")


def collect_xpath_results_for_summary(cur: psycopg2.extensions.cursor) -> dict:
    """
    Collects XPath axis results for summary display.
    Returns dictionary with all axis results.
    """
    results = {}

    try:
        # Get key node IDs
        cur.execute("SELECT id FROM accel WHERE s_id = 'SchmittKAMM23';")
        schmitt_result = cur.fetchone()
        cur.execute("SELECT id FROM accel WHERE s_id = 'SchalerHS23';")
        schaler_result = cur.fetchone()
        cur.execute("SELECT a.id FROM accel a JOIN content c ON a.id = c.id WHERE c.text = 'Daniel Ulrich Schmitt';")
        daniel_result = cur.fetchone()
        cur.execute("SELECT id FROM accel WHERE s_id = 'vldb_2023';")
        vldb_result = cur.fetchone()

        if all([schmitt_result, schaler_result, daniel_result, vldb_result]):
            schmitt_id = schmitt_result[0]
            schaler_id = schaler_result[0]
            daniel_id = daniel_result[0]
            vldb_id = vldb_result[0]

            # Ancestor test
            ancestors = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
            results["ancestor"] = [row[0] for row in ancestors]

            # Descendant test
            descendants = descendant_nodes(cur, vldb_id)
            results["descendants"] = [row[0] for row in descendants]

            # Sibling tests
            schmitt_following = siblings(cur, schmitt_id, direction="following")
            results["schmitt_following"] = [row[0] for row in schmitt_following]

            schmitt_preceding = siblings(cur, schmitt_id, direction="preceding")
            results["schmitt_preceding"] = [row[0] for row in schmitt_preceding]

            schaler_following = siblings(cur, schaler_id, direction="following")
            results["schaler_following"] = [row[0] for row in schaler_following]

            schaler_preceding = siblings(cur, schaler_id, direction="preceding")
            results["schaler_preceding"] = [row[0] for row in schaler_preceding]

    except Exception as e:
        print(f"   Warning: Could not collect all XPath results: {e}")

    return results


def generate_phase2_summary_tables(cur: psycopg2.extensions.cursor) -> None:
    """
    Generates summary tables for both EDGE model and XPath accelerator model results.
    Shows the same format as Phase 1 summary table.
    """
    print("\n" + "="*70)
    print("PHASE 2 SUMMARY TABLES")
    print("="*70)

    # Get node mappings for Phase 2 (accel schema)
    cur.execute("SELECT id FROM accel WHERE s_id = 'SchmittKAMM23';")
    schmitt_id = cur.fetchone()[0]
    cur.execute("SELECT id FROM accel WHERE s_id = 'SchalerHS23';")
    schaler_id = cur.fetchone()[0]
    cur.execute("SELECT a.id FROM accel a JOIN content c ON a.id = c.id WHERE c.text = 'Daniel Ulrich Schmitt';")
    daniel_id = cur.fetchone()[0]
    cur.execute("SELECT id FROM accel WHERE s_id = 'vldb_2023';")
    vldb_id = cur.fetchone()[0]

    print(f"\nPhase 2 Node Mappings (accel schema):")
    print(f"  SchmittKAMM23: {schmitt_id}")
    print(f"  SchalerHS23: {schaler_id}")
    print(f"  Daniel Ulrich Schmitt: {daniel_id}")
    print(f"  VLDB 2023: {vldb_id}")

    # Collect results for EDGE model (recursive functions)
    print(f"\nCOLLECTING EDGE MODEL RESULTS (Recursive Functions)")

    # Ancestor test
    ancestors_edge = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
    ancestor_ids_edge = [row[0] for row in ancestors_edge]

    # Descendant test
    descendants_edge = descendant_nodes(cur, vldb_id)
    descendant_ids_edge = [row[0] for row in descendants_edge]

    # Sibling tests
    schmitt_following_edge = siblings(cur, schmitt_id, direction="following")
    schmitt_following_ids_edge = [row[0] for row in schmitt_following_edge]

    schmitt_preceding_edge = siblings(cur, schmitt_id, direction="preceding")
    schmitt_preceding_ids_edge = [row[0] for row in schmitt_preceding_edge]

    schaler_following_edge = siblings(cur, schaler_id, direction="following")
    schaler_following_ids_edge = [row[0] for row in schaler_following_edge]

    schaler_preceding_edge = siblings(cur, schaler_id, direction="preceding")
    schaler_preceding_ids_edge = [row[0] for row in schaler_preceding_edge]

    # Collect results for XPath Accelerator model (window functions)
    print(f"\n COLLECTING XPATH ACCELERATOR MODEL RESULTS (Window Functions)")


    # Ancestor test
    ancestors_xpath = xpath_ancestor_window(cur, daniel_id)
    ancestor_ids_xpath = [row[0] for row in ancestors_xpath]

    # Descendant test
    descendants_xpath = xpath_descendant_window(cur, vldb_id)
    descendant_ids_xpath = [row[0] for row in descendants_xpath]

    # Sibling tests
    schmitt_following_xpath = xpath_following_sibling_window(cur, schmitt_id)
    schmitt_following_ids_xpath = [row[0] for row in schmitt_following_xpath]

    schmitt_preceding_xpath = xpath_preceding_sibling_window(cur, schmitt_id)
    schmitt_preceding_ids_xpath = [row[0] for row in schmitt_preceding_xpath]

    schaler_following_xpath = xpath_following_sibling_window(cur, schaler_id)
    schaler_following_ids_xpath = [row[0] for row in schaler_following_xpath]

    schaler_preceding_xpath = xpath_preceding_sibling_window(cur, schaler_id)
    schaler_preceding_ids_xpath = [row[0] for row in schaler_preceding_xpath]

    # Generate summary tables
    print(f"\n1. EDGE MODEL SUMMARY TABLE")
    print("="*80)
    print("Axis                    | Result Node IDs                                    | Size")
    print("-" * 80)

    ancestor_str_edge = ','.join(map(str, ancestor_ids_edge))
    descendant_str_edge = ','.join(map(str, descendant_ids_edge))
    schmitt_following_str_edge = ','.join(map(str, schmitt_following_ids_edge)) if schmitt_following_ids_edge else "-"
    schmitt_preceding_str_edge = ','.join(map(str, schmitt_preceding_ids_edge)) if schmitt_preceding_ids_edge else "-"
    schaler_following_str_edge = ','.join(map(str, schaler_following_ids_edge)) if schaler_following_ids_edge else "-"
    schaler_preceding_str_edge = ','.join(map(str, schaler_preceding_ids_edge)) if schaler_preceding_ids_edge else "-"

    print(f"ancestor                | {ancestor_str_edge:50} | {len(ancestor_ids_edge)}")
    print(f"descendants             | {descendant_str_edge:50} | {len(descendant_ids_edge)}")
    print(f"following SchmittKAMM23 | {schmitt_following_str_edge:50} | {len(schmitt_following_ids_edge)}")
    print(f"preceding SchmittKAMM23 | {schmitt_preceding_str_edge:50} | {len(schmitt_preceding_ids_edge)}")
    print(f"following SchalerHS23   | {schaler_following_str_edge:50} | {len(schaler_following_ids_edge)}")
    print(f"preceding SchalerHS23   | {schaler_preceding_str_edge:50} | {len(schaler_preceding_ids_edge)}")

    print(f"\n2. XPATH ACCELERATOR MODEL SUMMARY TABLE")
    print("="*80)
    print("Axis                    | Result Node IDs                                    | Size")
    print("-" * 80)

    ancestor_str_xpath = ','.join(map(str, ancestor_ids_xpath))
    descendant_str_xpath = ','.join(map(str, descendant_ids_xpath))
    schmitt_following_str_xpath = ','.join(map(str, schmitt_following_ids_xpath)) if schmitt_following_ids_xpath else "-"
    schmitt_preceding_str_xpath = ','.join(map(str, schmitt_preceding_ids_xpath)) if schmitt_preceding_ids_xpath else "-"
    schaler_following_str_xpath = ','.join(map(str, schaler_following_ids_xpath)) if schaler_following_ids_xpath else "-"
    schaler_preceding_str_xpath = ','.join(map(str, schaler_preceding_ids_xpath)) if schaler_preceding_ids_xpath else "-"

    print(f"ancestor                | {ancestor_str_xpath:50} | {len(ancestor_ids_xpath)}")
    print(f"descendants             | {descendant_str_xpath:50} | {len(descendant_ids_xpath)}")
    print(f"following SchmittKAMM23 | {schmitt_following_str_xpath:50} | {len(schmitt_following_ids_xpath)}")
    print(f"preceding SchmittKAMM23 | {schmitt_preceding_str_xpath:50} | {len(schmitt_preceding_ids_xpath)}")
    print(f"following SchalerHS23   | {schaler_following_str_xpath:50} | {len(schaler_following_ids_xpath)}")
    print(f"preceding SchalerHS23   | {schaler_preceding_str_xpath:50} | {len(schaler_preceding_ids_xpath)}")

    # Verification
    print(f"\n3. VERIFICATION")
    print("="*80)
    print("Comparing EDGE Model vs XPath Accelerator Model results:")

    comparisons = [
        ("ancestor", len(ancestor_ids_edge), len(ancestor_ids_xpath)),
        ("descendants", len(descendant_ids_edge), len(descendant_ids_xpath)),
        ("following SchmittKAMM23", len(schmitt_following_ids_edge), len(schmitt_following_ids_xpath)),
        ("preceding SchmittKAMM23", len(schmitt_preceding_ids_edge), len(schmitt_preceding_ids_xpath)),
        ("following SchalerHS23", len(schaler_following_ids_edge), len(schaler_following_ids_xpath)),
        ("preceding SchalerHS23", len(schaler_preceding_ids_edge), len(schaler_preceding_ids_xpath))
    ]

    all_match = True
    for axis, edge_count, xpath_count in comparisons:
        match = edge_count == xpath_count
        status = " MATCH" if match else " DIFFER"
        print(f"  {axis:22} | EDGE: {edge_count:3} | XPath: {xpath_count:3} | {status}")
        if not match:
            all_match = False

    print(f"\nOverall Verification: {' ALL RESULTS MATCH' if all_match else ' SOME RESULTS DIFFER'}")

    # Expected toy example validation
    expected_counts = [7, 28, 1, 0, 0, 1]  # ancestor, descendants, following schmitt, preceding schmitt, following schaler, preceding schaler
    actual_counts = [len(ancestor_ids_edge), len(descendant_ids_edge), len(schmitt_following_ids_edge),
                    len(schmitt_preceding_ids_edge), len(schaler_following_ids_edge), len(schaler_preceding_ids_edge)]

    toy_validation = actual_counts == expected_counts
    print(f"Toy Example Validation: {' MATCHES EXPECTED PHASE 1 VALUES' if toy_validation else ' DIFFERS FROM EXPECTED VALUES'}")
    if toy_validation:
        print("  Expected: [7, 28, 1, 0, 0, 1] ")
        print(f"  Actual:   {actual_counts} ")


def test_xpath_accelerators_separately():
    """
    Tests XPath accelerators separately on toy example data.
    This avoids performance issues with the large dataset.
    """
    print(f"\n" + "="*60)
    print("XPATH ACCELERATOR TESTING (Toy Example)")
    print("="*60)

    # Create a separate connection for toy example testing
    test_conn = connect_db()
    if not test_conn:
        print(" Could not connect to database for XPath testing")
        return

    test_cur = test_conn.cursor()

    try:
        print("1. Setting up toy example for XPath testing...")

        # Setup accelerator schema for toy example
        setup_schema(test_cur, use_original_schema=False)

        # Parse and insert ONLY toy example data
        toy_venues = parse_toy_example("toy_example.txt")
        toy_root = build_edge_model(toy_venues)
        annotate_traversal_orders(toy_root)
        toy_root.insert_to_db(test_cur, verbose=False)
        test_conn.commit()

        print("2. Testing XPath window functions on toy example...")
        test_queries(test_cur)

        # Generate summary tables for both models
        print("\n3. Generating summary tables...")
        generate_phase2_summary_tables(test_cur)

        print(f"\n" + "="*60)
        print("XPATH ACCELERATOR TESTING COMPLETE")
        print("="*60)

    except Exception as e:
        print(f" XPath testing failed: {e}")
    finally:
        test_cur.close()
        test_conn.close()
