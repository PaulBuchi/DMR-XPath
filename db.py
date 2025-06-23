# db.py
"""
Datenbank-Utilities:
 - connect_db: Verbindung aufbauen
 - close_db:   Cursor und Connection schließen
 - setup_schema: Tabellen anlegen
"""

import psycopg2
from psycopg2.extensions import cursor as PsycoCursor
from typing import Optional, Tuple, Any
from config import DB_PARAMS


def connect_db():
    """Stellt die Verbindung zur Datenbank her."""
    return psycopg2.connect(**DB_PARAMS)


def setup_schema(cur: psycopg2.extensions.cursor, use_original_schema: bool = False) -> None:
    """
    Legt die Tabellen für das XPath Accelerator System an.

    Args:
        use_original_schema: If True, uses original Node/Edge schema for Phase 1 compatibility
                           If False, uses new accel/content/attribute schema for window functions
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