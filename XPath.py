import psycopg2
from lxml import etree
from collections import defaultdict


def connect_db():
    try:
        conn = psycopg2.connect(user='postgres',
                                password='Science_city',
                                host='localhost',
                                port='5432',
                                database='DMR_XPath')
        print("Erfolgreich mit Datenbank verbunden.")
        return conn
    except Exception as e:
        print(f"Fehler bei Verbindung zur Datenbank: {e}")
        return None


def setup_schema(cur):
    print("Richte Datenbankschema ein...")
    cur.execute("DROP TABLE IF EXISTS Edge")
    cur.execute("DROP TABLE IF EXISTS Node")
    print("Alte Tabellen gelöscht (falls vorhanden).")
    cur.execute("""
        CREATE TABLE Node (
            id SERIAL PRIMARY KEY,
            s_id TEXT,
            type TEXT,
            content TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE Edge (
            id SERIAL PRIMARY KEY,
            from_node INTEGER REFERENCES Node(id),
            to_node INTEGER REFERENCES Node(id),
            position INTEGER
        )
    """)
    print("Tabellen Node und Edge erstellt.")


def parse_toy_example(file_path):
    parser = etree.XMLParser(load_dtd=True, no_network=False, resolve_entities=True)
    tree = etree.parse(file_path, parser)
    root = tree.getroot()

    # Falls die Wurzel selbst <bib> ist, verwenden wir sie direkt
    if root.tag == "bib":
        bib = root
    else:
        bib = root.find("bib")

    if bib is None:
        print("Kein <bib>-Element gefunden!")
        return {}

    venues = defaultdict(lambda: defaultdict(list))
    for pub in bib:
        if pub.tag not in ["article", "inproceedings"]:
            continue
        year = pub.findtext("year")
        key = pub.get("key")

        venue = None
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
    def __init__(self, type_, content=None, s_id=None):
        self.type = type_
        self.content = content
        self.children = []
        self.db_id = None
        self.s_id = s_id

    def add_child(self, child):
        self.children.append(child)

    def insert_to_db(self, cur, parent_id=None, position=0):
        cur.execute(
            "INSERT INTO Node (s_id, type, content) VALUES (%s, %s, %s) RETURNING id",
            (self.s_id, self.type, self.content)
        )
        self.db_id = cur.fetchone()[0]
        print(f"Node inserted: id={self.db_id}, type={self.type}, s_id={self.s_id}, content={self.content}")
        if parent_id is not None:
            cur.execute(
                "INSERT INTO Edge (from_node, to_node, position) VALUES (%s, %s, %s)",
                (parent_id, self.db_id, position)
            )
            print(f"Edge inserted: from {parent_id} to {self.db_id} at position {position}")
        for idx, child in enumerate(self.children):
            child.insert_to_db(cur, self.db_id, idx)


def build_edge_model(venues):
    root_node = Node("bib")  # Wurzelknoten

    for venue, years in venues.items():
        # Venue-Knoten: type="venue", content=None, s_id=venue
        venue_node = Node(type_="venue", content=None, s_id=venue)

        for year, pubs in years.items():
            # Year-Knoten: type="year", content=None, s_id="sigmod_2023" etc.
            year_node = Node(type_="year", content=None, s_id=f"{venue}_{year}")

            for pub in pubs:
                # Ermittlung des Kurzschlüssels (short_key) aus dem "key"-Attribut
                full_key = pub.get("key")
                short_key = full_key.split("/")[-1] if full_key else None

                # Publication-Knoten: type="article" oder "inproceedings", content=None, s_id=short_key
                pub_node = Node(type_=pub.tag, content=None, s_id=short_key)

                # Alle Kindelemente (z.B. author, title, pages, year, volume, journal, number, ee, url)
                for child in pub:
                    if child.tag in ["mdate", "orcid"]:
                        continue
                    # content=child.text, type=child.tag, s_id bleibt None
                    pub_node.add_child(Node(type_=child.tag, content=child.text))

                year_node.add_child(pub_node)

            venue_node.add_child(year_node)

        root_node.add_child(venue_node)

    return root_node


def ancestor_nodes(cur, node_content):
    cur.execute(
        """WITH RECURSIVE ancestors(id) AS (
            SELECT e.from_node FROM Node n JOIN Edge e ON n.id = e.to_node
            WHERE n.type = 'author' AND n.content = %s
            UNION
            SELECT e.from_node FROM ancestors a JOIN Edge e ON a.id = e.to_node
            ) SELECT n.* FROM Node n WHERE n.id IN (SELECT id FROM ancestors);""",
        (node_content, )
    )
    return cur.fetchall()


def descendant_nodes(cur, node_id):
    cur.execute("""
        WITH RECURSIVE Descendants(from_node, to_node) AS (
            SELECT from_node, to_node FROM Edge WHERE from_node = %s
            UNION
            SELECT e.from_node, e.to_node FROM Edge e JOIN Descendants d ON e.from_node = d.to_node
        )
        SELECT DISTINCT Node.id, Node.type, Node.content FROM Node JOIN Descendants ON Node.id = Descendants.to_node;
    """, (node_id,))
    return cur.fetchall()


def siblings(cur, node_id, direction="following"):
    # Zuerst ermitteln wir den übergeordneten Knoten (parent) und die Position im Elternknoten
    cur.execute("SELECT type FROM Node WHERE id = %s", (node_id,))
    row = cur.fetchone()
    if not row:
        return []
    # (Hier könnte man optional prüfen, ob es sich um einen Publikationsknoten handelt)

    # Parent-Knoten
    cur.execute("SELECT from_node FROM Edge WHERE to_node = %s", (node_id,))
    parent_row = cur.fetchone()
    if not parent_row:
        return []
    parent_id = parent_row[0]

    # Eigene Position im Elternknoten
    cur.execute("SELECT position FROM Edge WHERE to_node = %s", (node_id,))
    pos_row = cur.fetchone()
    if not pos_row:
        return []
    my_pos = pos_row[0]

    # Je nach Richtung Abfrage anpassen – ohne Typfilterung
    if direction == "following":
        query = """
            SELECT n.id, n.s_id, n.type, n.content, e.position
            FROM Edge e
            JOIN Node n ON e.to_node = n.id
            WHERE e.from_node = %s
              AND e.position > %s
            ORDER BY e.position ASC
        """
    else:  # "preceding"
        query = """
            SELECT n.id, n.s_id, n.type, n.content, e.position
            FROM Edge e
            JOIN Node n ON e.to_node = n.id
            WHERE e.from_node = %s
              AND e.position < %s
            ORDER BY e.position DESC
        """

    cur.execute(query, (parent_id, my_pos))
    return cur.fetchall()



def print_nodes(label, nodes):
    print(f"{label}:")
    if not nodes:
        print("  Keine Knoten gefunden.")
        return

    # Wir erwarten in nodes-Tupeln jetzt z.B.: (id, s_id, type, content, [position])
    for node in nodes:
        if len(node) == 3:
            nid, ntype, ncontent = node
            print(node)
            # print(f"  id={nid}, type={ntype}, content={ncontent}")
        elif len(node) == 4:
            nid, nsid, ntype, ncontent = node
            print(node)
            #print(f"  id={nid}, s_id={nsid}, type={ntype}, content={ncontent}")
        elif len(node) == 5:
            nid, nsid, ntype, ncontent, pos = node
            print(node)
            #print(f"  id={nid}, s_id={nsid}, type={ntype}, content={ncontent}, position={pos}")
        else:
            # Fallback für unerwartete Tupelformate
            print(f"  {node}")



def test_queries(cur):
    print("\nTeste XPath-Funktionen:\n")

    print("Ancestors von 'Daniel Ulrich Schmitt':")
    #cur.execute("SELECT id FROM Node WHERE content = 'Daniel Ulrich Schmitt'")
    result = 'Daniel Ulrich Schmitt' #cur.fetchone()
    if result:
        nodes = ancestor_nodes(cur, result)
        print_nodes("Ancestors", nodes)
    else:
        print("Knoten mit Inhalt 'Daniel Ulrich Schmitt' nicht gefunden")

    print("\nDescendants von VLDB 2023 (s_id='vldb_2023'):")
    cur.execute("SELECT id FROM Node WHERE s_id = 'vldb_2023'")
    result = cur.fetchone()
    if result:
        descendants = descendant_nodes(cur, result[0])
        print_nodes("Descendants", descendants)
    else:
        print("Knoten mit s_id 'vldb_2023' nicht gefunden")

    print("\nSiblings von 'SchmittKAMM23':")
    cur.execute("SELECT id FROM Node WHERE s_id = 'SchmittKAMM23'")
    sid_result = cur.fetchone()
    if sid_result:
        following = siblings(cur, sid_result[0], "following")
        print_nodes("Following siblings", following)
        preceding = siblings(cur, sid_result[0], "preceding")
        print_nodes("Preceding siblings", preceding)
    else:
        print("Knoten mit s_id 'SchmittKAMM23' nicht gefunden")


def main():
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
