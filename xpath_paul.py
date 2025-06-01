import psycopg2
from lxml import etree
from collections import defaultdict
from typing import Dict, List, Optional, Tuple


# Datenbank-Verbindungsparameter
DB_PARAMS = {
    'host': 'localhost',
    'dbname': 'DMR_XPath',
    'user': 'postgres',
    'password': 'Science_city',
    'port': '5432'
}


def connect_db():
    """Stellt die Verbindung zur Datenbank her."""
    return psycopg2.connect(**DB_PARAMS)


def setup_schema(cur: psycopg2.extensions.cursor) -> None:
    """
    Legt die Tabellen Node und Edge an.
    Alte Versionen werden vorher gelöscht.
    """
    print("Richte Datenbankschema ein...")
    cur.execute("DROP TABLE IF EXISTS Edge;")
    cur.execute("DROP TABLE IF EXISTS Node;")
    print("Alte Tabellen gelöscht (falls vorhanden).")

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
    print("Tabellen Node und Edge erstellt.")


def parse_toy_example(
    file_path: str
) -> Dict[str, Dict[str, List[etree._Element]]]:
    """
    Liest das Toy-Beispiel (XML) ein und gruppiert nach venue und Jahr.
    Ignoriert dabei die Tags 'mdate' und 'orcid'.
    """
    parser = etree.XMLParser(
        load_dtd=True,
        no_network=False,
        resolve_entities=True
    )
    tree = etree.parse(file_path, parser)
    venues: Dict[str, Dict[str, List[etree._Element]]] = defaultdict(
        lambda: defaultdict(list)
    )
    root = tree.getroot()  # z.B. <dblp>

    bib = root.find("bib")
    if bib is None:
        print("Kein <bib>-Element gefunden!")
        return venues

    for pub in bib:
        if pub.tag not in ("article", "inproceedings"):
            continue

        year = pub.findtext("year")
        key = pub.get("key")
        venue: Optional[str] = None

        if key:
            if key.startswith("conf/sigmod") or key.startswith("journals/pacmmod"):
                venue = "sigmod"
            elif key.startswith("conf/vldb") or key.startswith("journals/pvldb"):
                venue = "vldb"
            elif key.startswith("conf/icde"):
                venue = "icde"

        if venue and year:
            venues[venue][year].append(pub)

    return venues


class Node:
    """
    Repräsentiert einen Knoten im Edge Model mit beliebig vielen Kindern.
    Nach dem Einfügen in die DB speichert 'db_id' die generierte ID.
    """

    def __init__(
        self,
        type_: str,
        content: Optional[str] = None,
        s_id: Optional[str] = None
    ) -> None:
        self.type: str = type_
        self.content: Optional[str] = content
        self.children: List["Node"] = []
        self.db_id: Optional[int] = None
        self.s_id: Optional[str] = s_id

    def add_child(self, child: "Node") -> None:
        """Fügt diesem Knoten ein Kind hinzu."""
        self.children.append(child)

    def insert_to_db(
        self,
        cur: psycopg2.extensions.cursor,
        parent_id: Optional[int] = None,
        position: int = 0
    ) -> None:
        """
        Fügt diesen Knoten in die Tabelle Node ein.
        Falls 'parent_id' übergeben, wird auch der entsprechende Edge-Eintrag erzeugt.
        """
        cur.execute(
            "INSERT INTO Node (s_id, type, content) VALUES (%s, %s, %s) "
            "RETURNING id;",
            (self.s_id, self.type, self.content)
        )
        self.db_id = cur.fetchone()[0]

        if parent_id is not None:
            cur.execute(
                "INSERT INTO Edge (from_node, to_node, position) "
                "VALUES (%s, %s, %s);",
                (parent_id, self.db_id, position)
            )

        for idx, child in enumerate(self.children):
            child.insert_to_db(cur, self.db_id, idx)


def build_edge_model(
    venues: Dict[str, Dict[str, List[etree._Element]]]
) -> Node:
    """
    Baut den Baum nach dem EDGE Model auf:
    bib → venue → year → Publikationen → Kinder (author, title, ...).
    Gibt den Wurzelknoten 'bib' zurück.
    """
    root_node = Node("bib")

    for venue, years in venues.items():
        venue_node = Node("venue", content=venue)
        for year, pubs in years.items():
            year_node = Node("year", content=year, s_id=f"{venue}_{year}")
            for pub in pubs:
                full_key = pub.get("key")
                short_key = full_key.split("/")[-1] if full_key else None
                pub_node = Node(pub.tag, s_id=short_key)

                for child in pub:
                    if child.tag in ("mdate", "orcid"):
                        continue
                    pub_node.add_child(Node(child.tag, content=child.text))

                year_node.add_child(pub_node)

            venue_node.add_child(year_node)

        root_node.add_child(venue_node)

    return root_node


def ancestor_nodes(
    cur: psycopg2.extensions.cursor,
    node_id: int
) -> List[Tuple[int, str, Optional[str]]]:
    """
    Berechnet alle ancestor-Knoten eines gegebenen Knotens in der DB.
    Gibt eine Liste von Tupeln (id, type, content) zurück.
    """
    cur.execute(
        """
        WITH RECURSIVE Ancestors(from_node, to_node) AS (
            SELECT from_node, to_node FROM Edge WHERE to_node = %s
            UNION
            SELECT e.from_node, e.to_node
            FROM Edge e
            JOIN Ancestors a ON e.to_node = a.from_node
        )
        SELECT DISTINCT Node.id, Node.type, Node.content
        FROM Node
        JOIN Ancestors ON Node.id = Ancestors.from_node;
        """,
        (node_id,)
    )
    return cur.fetchall()


def descendant_nodes(
    cur: psycopg2.extensions.cursor,
    node_id: int
) -> List[Tuple[int, str, Optional[str]]]:
    """
    Berechnet alle descendant-Knoten eines gegebenen Knotens in der DB.
    Rückgabe als Liste von Tupeln (id, type, content).
    """
    cur.execute(
        """
        WITH RECURSIVE Descendants(from_node, to_node) AS (
            SELECT from_node, to_node FROM Edge WHERE from_node = %s
            UNION
            SELECT e.from_node, e.to_node
            FROM Edge e
            JOIN Descendants d ON e.from_node = d.to_node
        )
        SELECT DISTINCT Node.id, Node.type, Node.content
        FROM Node
        JOIN Descendants ON Node.id = Descendants.to_node;
        """,
        (node_id,)
    )
    return cur.fetchall()

def siblings(
    cur: psycopg2.extensions.cursor,
    node_id: int,
    direction: str = "following"
) -> List[Tuple[int,str,Optional[str]]]:
    """
    Berechnet die following- oder preceding-sibling-Knoten eines Knotens
    vom Typ <article> (im Toy-Beispiel sind nur <article>-Knoten
    direkte Siblings unter demselben parent = <year>-Knoten).
    direction muss 'following' oder 'preceding' sein.
    """
    #Stelle sicher, dass dieser node_id-Tupel wirklich wirklich ein <article> ist:
    cur.execute("SELECT type FROM Node WHERE id = %s;", (node_id,))
    row = cur.fetchone()
    if row is None or row[0] != "article":
        return []

    #Hole das Parent-Level. Muss der year-Knoten sein
    cur.execute("SELECT from_node FROM Edge WHERE to_node = %s;", (node_id,))
    parent = cur.fetchone()
    if not parent:
        return []
    parent_id = parent[0]

    # Hole Position dieses <article>-Knotens
    cur.execute("SELECT position FROM Edge WHERE to_node = %s;", (node_id,))
    pos_row = cur.fetchone()
    if not pos_row:
        return []
    my_position = pos_row[0]

    # Baue die siblings-Query korrekt auf
    if direction == "following":
        cur.execute(
            """
            SELECT n.id, n.type, n.content
            FROM Edge e
            JOIN Node n ON e.to_node = n.id
            WHERE e.from_node = %s
              AND e.position > %s
              AND n.type = 'article'
            ORDER BY e.position;
            """,
            (parent_id, my_position)
        )
    else:  # preceding
        cur.execute(
            """
            SELECT n.id, n.type, n.content
            FROM Edge e
            JOIN Node n ON e.to_node = n.id
            WHERE e.from_node = %s
              AND e.position < %s
              AND n.type = 'article'
            ORDER BY e.position DESC;
            """,
            (parent_id, my_position)
        )

    return cur.fetchall()


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

    for _id, _type, _content in nodes:
        print(f"  id={_id}, type={_type}, content={_content}")


def test_queries(cur: psycopg2.extensions.cursor) -> None:
    """
    Führt Testabfragen für Ancestor, Descendant, und Siblings durch,
    analog zu Phase 1 der Aufgabenstellung.
    """
    print("\nTeste XPath-Funktionen:\n")

    # Ancestors von "Daniel Ulrich Schmitt"
    print("Ancestors von 'Daniel Ulrich Schmitt':")
    cur.execute(
        "SELECT id FROM Node WHERE content = 'Daniel Ulrich Schmitt';"
    )
    result = cur.fetchone()
    if result:
        nodes = ancestor_nodes(cur, result[0])
        print_nodes("Ancestors", nodes)
    else:
        print("Knoten mit Inhalt 'Daniel Ulrich Schmitt' nicht gefunden")

    # Descendants von VLDB 2023 (s_id='vldb_2023')
    print("\nDescendants von VLDB 2023 (s_id='vldb_2023'):")
    cur.execute("SELECT id FROM Node WHERE s_id = 'vldb_2023';")
    result = cur.fetchone()
    if result:
        descendants = descendant_nodes(cur, result[0])
        print_nodes("Descendants", descendants)
    else:
        print("Knoten mit s_id 'vldb_2023' nicht gefunden")

    # Siblings von "SchmittKAMM23"
    print("\nSiblings von 'SchmittKAMM23':")
    cur.execute("SELECT id FROM Node WHERE s_id = 'SchmittKAMM23';")
    sid_result = cur.fetchone()
    if sid_result:
        following = siblings(cur, sid_result[0], direction="following")
        print_nodes("Following siblings", following)
        preceding = siblings(cur, sid_result[0], direction="preceding")
        print_nodes("Preceding siblings", preceding)
    else:
        print("Knoten mit s_id 'SchmittKAMM23' nicht gefunden")


def main() -> None:
    """
    Hauptprogramm: Verbindet zur DB, erstellt Schema, parst Toy-Beispiel und
    füllt die Tabellen. Anschließend Testabfragen.
    """
    conn = connect_db()
    if not conn:
        return

    cur = conn.cursor()
    setup_schema(cur)

    venues = parse_toy_example("toy_example.txt")
    root_node = build_edge_model(venues)
    root_node.insert_to_db(cur)

    print("Datenbank erfolgreich mit Knoten und Kanten gefüllt.")
    conn.commit()

    test_queries(cur)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
