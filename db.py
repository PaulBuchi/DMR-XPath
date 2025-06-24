# db.py
"""
Datenbank-Utilities:
 - connect_db: Verbindung aufbauen
 - clear_db:    Datenbank leeren
 - setup_schema: Tabellen anlegen
"""

import psycopg2
from psycopg2.extensions import cursor as PsycoCursor
from typing import Optional, Tuple, Any
from config import DB_PARAMS


def connect_db():
    """Stellt die Verbindung zur Datenbank her."""
    return psycopg2.connect(**DB_PARAMS)


def clear_db() -> None:
    """
    Löscht alle Tabellen und Sequenzen in der Datenbank.
    """
    conn = connect_db()
    cur = conn.cursor()
    # Drop all tables
    cur.execute("DROP TABLE IF EXISTS attribute CASCADE;")
    cur.execute("DROP TABLE IF EXISTS content CASCADE;")
    cur.execute("DROP TABLE IF EXISTS accel CASCADE;")
    cur.execute("DROP TABLE IF EXISTS Edge CASCADE;")
    cur.execute("DROP TABLE IF EXISTS Node CASCADE;")
    cur.execute("DROP TABLE IF EXISTS single_axis_accel CASCADE;")
    cur.execute("DROP TABLE IF EXISTS single_axis_content CASCADE;")
    cur.execute("DROP TABLE IF EXISTS optimized_accel CASCADE;")
    #Drop all sequences
    cur.execute("DROP SEQUENCE IF EXISTS noe_id_seq;")
    cur.execute("DROP SEQUENCE IF EXISTS edge_id_seq;")
    cur.execute("DROP SEQUENCE IF EXISTS optimized_accel_id_seq;")
    cur.execute("DrOP SEQUENCE IF EXISTS single_axis_accel_id_seq;")
    conn.commit()
    cur.close()
    print("Datenbank geleert: Alle Tabellen und Sequences gelöscht.")


def setup_schema(cur: psycopg2.extensions.cursor, use_original_schema: bool = False) -> None:
    """
    Legt die Tabellen für das XPath Accelerator System an.

    Args:
        use_original_schema: Wenn True, wird das originale Node/Edge-Schema für Phase 1 Kompatibilität verwendet.
                            Wenn False, wird das neue accel/content/attribute-Schema für Window-Functions verwendet.
    """
    if use_original_schema:
        print("Richte Original Node/Edge Schema ein (Phase 1 Kompatibilität)...")

        # Drop existing tables
        cur.execute("DROP TABLE IF EXISTS attribute;")
        cur.execute("DROP TABLE IF EXISTS content;")
        cur.execute("DROP TABLE IF EXISTS accel;")
        cur.execute("DROP TABLE IF EXISTS Edge;")
        cur.execute("DROP TABLE IF EXISTS Node;")
        print("Alte Tabellen gelöscht (falls vorhanden).")

        # Create original Node/Edge schema
        cur.execute("""
            CREATE TABLE Node (
                id SERIAL PRIMARY KEY,
                s_id TEXT,
                type TEXT,
                content TEXT
            );
        """)
        cur.execute("""
            CREATE TABLE Edge (
                id SERIAL PRIMARY KEY,
                from_node INTEGER REFERENCES Node(id),
                to_node INTEGER REFERENCES Node(id),
                position INTEGER
            );
        """)

        print("Original Schema Tabellen erstellt:")
        print("  - Node: Core node table with SERIAL IDs")
        print("  - Edge: Parent-child relationships")
    else:
        print("Richte XPath Accelerator Datenbankschema ein...")

        # Drop existing tables in correct order (respecting foreign keys)
        cur.execute("DROP TABLE IF EXISTS attribute;")
        cur.execute("DROP TABLE IF EXISTS content;")
        cur.execute("DROP TABLE IF EXISTS accel;")
        # Legacy tables cleanup
        cur.execute("DROP TABLE IF EXISTS Edge;")
        cur.execute("DROP TABLE IF EXISTS Node;")
        print("Alte Tabellen gelöscht (falls vorhanden).")

        # Create accel table - core node table with EDGE model structure
        cur.execute("""
            CREATE TABLE accel (
                id INT PRIMARY KEY,
                pre_order INT NOT NULL,
                post_order INT NOT NULL,
                s_id VARCHAR(255),
                parent INT,
                type VARCHAR(50),
                FOREIGN KEY (parent) REFERENCES accel(id)
            );
        """)

        # Create content table - stores textual content of nodes
        cur.execute("""
            CREATE TABLE content (
                id INT PRIMARY KEY,
                text TEXT,
                FOREIGN KEY (id) REFERENCES accel(id)
            );
        """)

        # Create attribute table - stores XML attributes as key-value pairs
        cur.execute("""
            CREATE TABLE attribute (
                id INT,
                text TEXT,
                PRIMARY KEY (id, text),
                FOREIGN KEY (id) REFERENCES accel(id)
            );
        """)

        print("XPath Accelerator Tabellen erstellt:")
        print("  - accel: Core node table with post-order numbering")
        print("  - content: Node content storage")
        print("  - attribute: Node attributes storage")


def get_database_statistics(cur: psycopg2.extensions.cursor) -> Tuple[int, int, int]:
    """
    Gibt die Anzahl der Tupel in den XPath Accelerator Tabellen zurück.
    Returns: (accel_count, content_count, attribute_count)
    """
    cur.execute("SELECT COUNT(*) FROM accel;")
    accel_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM content;")
    content_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM attribute;")
    attribute_count = cur.fetchone()[0]

    return accel_count, content_count, attribute_count