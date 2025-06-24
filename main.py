# main.py
"""
Steuert Phase 1 (Original-Schema + Toy-Beispiel)
und Phase 2 (accel-Schema + SMALL_BIB + Demo-Queries).
"""
import os
import re
import sys
from typing import Dict, List, Optional

from db import (
    connect_db,
    setup_schema,
    clear_db
)
from xml_parser import (
    parse_toy_example
)
from axes import (
    ancestor_nodes,
    descendant_nodes,
    siblings,
)
from model import (
    build_edge_model,
    annotate_traversal_orders,
)
from db import (
    get_database_statistics,
    
    )
from xml_parser import (
    extract_venue_publications,
    validate_toy_example_inclusion,
    count_nikolaus_augsten_publications,
    find_toy_example_positions,
    parse_extracted_data
)

from utils import test_xpath_accelerators_separately
from config import TOY_XML, SMALL_BIB
from single_axis_accelerator import verify_single_axis_correctness
from performance_comparison import benchmark_descendant_queries
from window_optimization import verify_window_optimization_equivalence
from window_performance_analysis import analyze_window_performance


def main_phase1() -> None:
    """
    Phase 1: Toy Example Processing mit Original Node/Edge Schema.
    Produziert die erwarteten Phase 1 Ergebnisse.
    """
    print("=== Phase 1: Toy Example Processing ===")

    conn = connect_db()
    if not conn:
        return

    cur = conn.cursor()

    # Use original Node/Edge schema for Phase 1 compatibility
    setup_schema(cur, use_original_schema=True)

    print("1. Parsing toy example...")
    venues = parse_toy_example("toy_example.txt")
    root_node = build_edge_model(venues)

    print("2. Inserting into database...")
    root_node.insert_to_original_db(cur, verbose=False)
    conn.commit()

    print("3. Key Node Mappings:")
    cur.execute("""
        SELECT id, type, s_id, content
        FROM Node
        WHERE s_id IS NOT NULL OR content = 'Daniel Ulrich Schmitt'
        ORDER BY id;
    """)
    key_nodes = cur.fetchall()
    for node_id, _, s_id, content in key_nodes:
        if s_id:
            print(f"   {s_id}: Node ID = {node_id}")
        elif content == 'Daniel Ulrich Schmitt':
            print(f"   Daniel Ulrich Schmitt: Node ID = {node_id}")

    print("\n4. Testing XPath accelerator...")

    # Ancestor test
    print("\n4.1 Ancestor axis (Daniel Ulrich Schmitt):")
    ancestors = ancestor_nodes(cur, "Daniel Ulrich Schmitt")
    ancestor_ids = [row[0] for row in ancestors]
    print(f"   Result: {ancestor_ids} (Count: {len(ancestor_ids)})")

    # Descendant test
    print("\n4.2 Descendant axis (VLDB 2023):")
    cur.execute("SELECT id FROM Node WHERE s_id = 'vldb_2023';")
    vldb_id = cur.fetchone()[0]
    descendants = descendant_nodes(cur, vldb_id)
    descendant_ids = [row[0] for row in descendants]
    print(f"   Result: {descendant_ids} (Count: {len(descendant_ids)})")

    # Sibling tests
    print("\n4.3 Sibling axes:")
    cur.execute("SELECT id FROM Node WHERE s_id = 'SchmittKAMM23';")
    schmitt_id = cur.fetchone()[0]
    cur.execute("SELECT id FROM Node WHERE s_id = 'SchalerHS23';")
    schaler_id = cur.fetchone()[0]

    schmitt_following = siblings(cur, schmitt_id, direction="following")
    schmitt_following_ids = [row[0] for row in schmitt_following]
    print(f"   Following SchmittKAMM23: {schmitt_following_ids} (Count: {len(schmitt_following_ids)})")

    schmitt_preceding = siblings(cur, schmitt_id, direction="preceding")
    schmitt_preceding_ids = [row[0] for row in schmitt_preceding]
    print(f"   Preceding SchmittKAMM23: {schmitt_preceding_ids} (Count: {len(schmitt_preceding_ids)})")

    schaler_following = siblings(cur, schaler_id, direction="following")
    schaler_following_ids = [row[0] for row in schaler_following]
    print(f"   Following SchalerHS23: {schaler_following_ids} (Count: {len(schaler_following_ids)})")

    schaler_preceding = siblings(cur, schaler_id, direction="preceding")
    schaler_preceding_ids = [row[0] for row in schaler_preceding]
    print(f"   Preceding SchalerHS23: {schaler_preceding_ids} (Count: {len(schaler_preceding_ids)})")

    # Summary table
    print("\n5. Summary Table:")
    print("   Axis                    | Result Node IDs                                    | Size")
    print("   " + "-" * 80)
    print(f"   ancestor                | {','.join(map(str, ancestor_ids)):50} | {len(ancestor_ids)}")
    print(f"   descendants             | {','.join(map(str, descendant_ids)):50} | {len(descendant_ids)}")

    schmitt_following_str = ','.join(map(str, schmitt_following_ids)) if schmitt_following_ids else "-"
    schmitt_preceding_str = ','.join(map(str, schmitt_preceding_ids)) if schmitt_preceding_ids else "-"
    schaler_following_str = ','.join(map(str, schaler_following_ids)) if schaler_following_ids else "-"
    schaler_preceding_str = ','.join(map(str, schaler_preceding_ids)) if schaler_preceding_ids else "-"

    print(f"   following SchmittKAMM23 | {schmitt_following_str:50} | {len(schmitt_following_ids)}")
    print(f"   preceding SchmittKAMM23 | {schmitt_preceding_str:50} | {len(schmitt_preceding_ids)}")
    print(f"   following SchalerHS23   | {schaler_following_str:50} | {len(schaler_following_ids)}")
    print(f"   preceding SchalerHS23   | {schaler_preceding_str:50} | {len(schaler_preceding_ids)}")

    cur.close()
    conn.close()

    print("\n=== Phase 1 Complete ===")
    print("Toy example processed and XPath functions tested successfully!")

def main_phase2(force_extraction: bool = False) -> None:
    """
    Hauptprogramm für Phase 2: DBLP Data Processing und XPath Accelerator.
    """
    print("=== Phase 2: DBLP Data Processing ===\n")

    # 1. Extrahiere venue-spezifische Publikationen
    output_file = "my_small_bib.xml"
    if force_extraction or not os.path.exists(output_file):
        print("1. Extracting venue-specific publications...")
        venue_counts = extract_venue_publications("dblp.xml", output_file)
    else:
        print("1. Using existing my_small_bib.xml file...")
        # Count publications in existing file
        venue_counts = {'vldb': 0, 'sigmod': 0, 'icde': 0}
        venue_patterns = {
            'vldb': re.compile(r'key="(conf/vldb/|journals/pvldb/)'),
            'sigmod': re.compile(r'key="(conf/sigmod/|journals/pacmmod/)'),
            'icde': re.compile(r'key="(conf/icde/)')
        }
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip().startswith('<article ') or line.strip().startswith('<inproceedings '):
                    for venue, pattern in venue_patterns.items():
                        if pattern.search(line):
                            venue_counts[venue] += 1
                            break

    # 2. Validiere Toy-Beispiel-Einschluss
    print("\n2. Validating toy example inclusion...")
    validation_success = validate_toy_example_inclusion(output_file)

    # 3. Zähle Nikolaus Augsten Publikationen
    print("\n3. Counting Nikolaus Augsten publications...")
    augsten_counts = count_nikolaus_augsten_publications(output_file)

    # 3.5. Finde Toy-Beispiel-Positionen
    print("\n3.5. Finding toy example publication positions...")
    toy_positions = find_toy_example_positions(output_file)

    # 4. File metrics
    print("\n4. File metrics:")
    file_size_kb = os.path.getsize(output_file) / 1024
    with open(output_file, 'r', encoding='utf-8') as f:
        line_count = sum(1 for _ in f)
    print(f"  File size: {file_size_kb:.1f} KB")
    print(f"  Line count: {line_count:,}")

    # 5. Importiere Daten in die Datenbank
    print("\n5. Importing data into database...")
    conn = connect_db()
    if not conn:
        print("ERROR: Could not connect to database")
        return

    cur = conn.cursor()
    setup_schema(cur)

    # Parse extrahierte Daten und baue EDGE Model
    print("  Parsing extracted data...")
    venues = parse_extracted_data(output_file)
    print("  Building EDGE model...")
    root_node = build_edge_model(venues)
    print("  Annotating nodes with traversal orders...")
    annotate_traversal_orders(root_node)
    print("  Inserting into database...")
    root_node.insert_to_db(cur, verbose=False)

    conn.commit()

    # 6. Datenbankstatistiken
    accel_count, content_count, attribute_count = get_database_statistics(cur)
    print(f"  Database import completed.")

    cur.close()
    conn.close()

    print("\n=== Phase 2 Summary ===")
    print("Venue publication counts:")
    for venue, count in venue_counts.items():
        print(f"  {venue.upper()}: {count:,}")

    print(f"\nNikolaus Augsten publications:")
    for venue, count in augsten_counts.items():
        print(f"  {venue.upper()}: {count}")

    print(f"\nDatabase statistics:")
    print(f"  accel table: {accel_count:,} tuples")
    print(f"  content table: {content_count:,} tuples")
    print(f"  attribute table: {attribute_count:,} tuples")

    print(f"\nValidation status:")
    print(f"  Toy example inclusion: {'  PASS' if validation_success else '  FAIL'}")

    print(f"\nToy example publication positions:")
    for pub, pos in toy_positions.items():
        print(f"  {pub}: {pos}")

    print(f"\nFile metrics:")
    print(f"  Size: {file_size_kb:.1f} KB")
    print(f"  Lines: {line_count:,}")

    print(f"\n" + "="*60)
    print("PHASE 2 COMPLETE - Large dataset processing finished!")
    print("="*60)

    # Ask user if they want to test XPath accelerators
    print(f"\nXPath Accelerator Testing:")
    print(f"  The large dataset ({accel_count:,} nodes) is now loaded.")
    print(f"  XPath window function tests should be run on toy example data for validation.")
    print(f"  Would you like to run XPath accelerator tests now? (y/n): ", end="")

    try:
        user_input = input().strip().lower()
        if user_input in ['y', 'yes']:
            test_xpath_accelerators_separately()
        else:
            print(f"  Skipping XPath accelerator tests.")
            print(f"  You can run them later with: python -c \"from xPath import test_xpath_accelerators_separately; test_xpath_accelerators_separately()\"")
    except (KeyboardInterrupt, EOFError):
        print(f"\n  Skipping XPath accelerator tests.")

def main_phase3() -> None:
    """
    Phase 3: Single-Axis XPath Accelerator Implementation und Window-Optimierungen.
    Implementiert eine optimierte Variante mit nur einer Achse (descendants) und
    Window-Verkleinerungen für effizientere Anfragen.
    """
    print("=== Phase 3: XPath Accelerator Optimizations ===\n")
    
    print("1. Single-Axis XPath Accelerator (descendants only)...")
    verify_single_axis_correctness()
    
    print("\n2. Window Size Reduction Optimizations...")
    verify_window_optimization_equivalence()
    
    print("\n3. Performance Benchmark...")
    benchmark_descendant_queries()
    
    # Interactive prompt for detailed performance analysis
    print(f"\nOptional: Detailed Performance Analysis")
    print(f"  Run optimization metrics (window)? (y/n): ", end="")

    user_input = input().strip().lower()
    if user_input in ['y', 'yes']:
        print("\n4. Running Detailed Performance Analysis...")
        analyze_window_performance()
    else:
        print("  Skipping detailed analysis.")

    print("\n" + "="*60)
    print("PHASE 3 COMPLETE - XPath Accelerator optimizations finished!")
    print("="*60)


def select_phase() -> int:
    """
    Allows user to select which phase to run.
    Returns 1 for Phase 1 (toy example), 2 for Phase 2 (DBLP processing), or 3 for Phase 3 (optimizations).
    """

    # Cleanup the DB
    clear_db()

    # Check for command line argument
    if len(sys.argv) > 1:
        try:
            phase = int(sys.argv[1])
            if phase in [1, 2, 3]:
                return phase
        except ValueError:
            pass

    # Check environment variable
    env_phase = os.environ.get('XPATH_PHASE')
    if env_phase:
        try:
            phase = int(env_phase)
            if phase in [1, 2, 3]:
                return phase
        except ValueError:
            pass

    # Interactive prompt
    while True:
        try:
            print("XPath Accelerator - Phase Selection")
            print("1. Phase 1: Toy Example Processing")
            print("2. Phase 2: DBLP Data Processing")
            print("3. Phase 3: Optimizations & Window Reduction")
            choice = input("Select phase (1, 2, or 3): ").strip()

            if choice in ['1', '2', '3']:
                return int(choice)
            else:
                print("Invalid choice. Please enter 1, 2, or 3.")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting...")
            sys.exit(0)


if __name__ == "__main__":
    phase = select_phase()

    if phase == 1:
        print("Running Phase 1: Toy Example Processing\n")
        main_phase1()
    elif phase == 2:
        print("Running Phase 2: DBLP Data Processing\n")
        main_phase2()
    elif phase == 3:
        print("Running Phase 3: Optimizations & Window Reduction\n")
        main_phase3()
    else:
        print("Invalid phase selected.")
        sys.exit(1)