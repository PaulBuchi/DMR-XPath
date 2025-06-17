import os
import re
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
        position: int = 0,
        verbose: bool = False
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
            child.insert_to_db(cur, self.db_id, idx, verbose)


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
    node_content: any
) -> List[Tuple[int, str, Optional[str]]]:
    """
    Berechnet alle ancestor-Knoten eines gegebenen Knotens in der DB.
    Gibt eine Liste von Tupeln (id, type, content) zurück.
    """
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

    for node in nodes:
        print(node)

def test_queries(cur: psycopg2.extensions.cursor) -> None:
    """
    Führt Testabfragen für Ancestor, Descendant, und Siblings durch,
    analog zu Phase 1 der Aufgabenstellung.
    """
    print("\nTeste XPath-Funktionen:\n")

    # Ancestors von "Daniel Ulrich Schmitt"
    print("Ancestors von 'Daniel Ulrich Schmitt':")
    result = 'Daniel Ulrich Schmitt'
    if result:
        nodes = ancestor_nodes(cur, result)
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

    # Siblings von "SchalerHS23"
    print("\nSiblings von 'SchalerHS23':")
    cur.execute("SELECT id FROM Node WHERE s_id = 'SchalerHS23';")
    sid_result2 = cur.fetchone()
    if sid_result2:
        following2 = siblings(cur, sid_result2[0], direction="following")
        print_nodes("Following siblings", following2)
        preceding2 = siblings(cur, sid_result2[0], direction="preceding")
        print_nodes("Preceding siblings", preceding2)
    else:
        print("Knoten mit s_id 'SchalerHS23' nicht gefunden")


def extract_venue_publications_simple(dblp_file: str, output_file: str) -> Dict[str, int]:
    """
    Extrahiert Publikationen von VLDB, SIGMOD und ICDE aus der DBLP-Datei.
    Verwendet einfache Regex-basierte Textverarbeitung ohne XML-Parser.
    """
    print("Starting venue-specific publication extraction...")
    print("  Using simple text processing approach...")

    venue_counts = {'vldb': 0, 'sigmod': 0, 'icde': 0}

    # Regex patterns für venue classification
    venue_patterns = {
        'vldb': re.compile(r'key="(conf/vldb/|journals/pvldb/)'),
        'sigmod': re.compile(r'key="(conf/sigmod/|journals/pacmmod/)'),
        'icde': re.compile(r'key="(conf/icde/)')
    }

    # Entity-Ersetzungen für häufige Zeichen
    entity_replacements = {
        '&uuml;': 'ü', '&auml;': 'ä', '&ouml;': 'ö', '&szlig;': 'ß',
        '&Uuml;': 'Ü', '&Auml;': 'Ä', '&Ouml;': 'Ö',
        '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'",
        '&reg;': '®', '&micro;': 'µ', '&times;': '×',
        '&eacute;': 'é', '&iacute;': 'í', '&aacute;': 'á', '&oacute;': 'ó', '&uacute;': 'ú',
        '&Eacute;': 'É', '&Iacute;': 'Í', '&Aacute;': 'Á', '&Oacute;': 'Ó', '&Uacute;': 'Ú',
        '&ccedil;': 'ç', '&Ccedil;': 'Ç', '&ntilde;': 'ñ', '&Ntilde;': 'Ñ',
        '&Aring;': 'Å', '&aring;': 'å'
    }

    def resolve_entities(text: str) -> str:
        """Ersetzt bekannte Entities durch ihre Unicode-Zeichen."""
        # First handle known entities
        for entity, replacement in entity_replacements.items():
            text = text.replace(entity, replacement)

        # Handle any remaining & that are not part of valid entities
        # This is a simple approach - replace standalone & with &amp;
        import re
        # Find & that are not followed by a valid entity pattern
        text = re.sub(r'&(?![a-zA-Z0-9#]+;)', '&amp;', text)

        return text

    try:
        with open(output_file, 'w', encoding='utf-8') as out_file:
            # Schreibe XML-Header und DTD-Referenz - match exact format
            out_file.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            out_file.write('<!DOCTYPE bib SYSTEM "dblp.dtd">\n')
            out_file.write('<bib>\n')

            with open(dblp_file, 'r', encoding='utf-8') as in_file:
                current_publication_lines = []
                in_target_publication = False
                current_venue = None
                processed_lines = 0

                for line in in_file:
                    processed_lines += 1

                    # Progress-Update
                    if processed_lines % 1000000 == 0:
                        print(f"    Processed {processed_lines:,} lines, extracted {sum(venue_counts.values())} publications...")

                    stripped_line = line.strip()

                    # Check if this is the start of an article or inproceedings
                    if stripped_line.startswith('<article ') or stripped_line.startswith('<inproceedings '):
                        # Check if this publication belongs to our target venues
                        current_venue = None
                        for venue, pattern in venue_patterns.items():
                            if pattern.search(stripped_line):
                                current_venue = venue
                                break

                        if current_venue:
                            in_target_publication = True
                            current_publication_lines = [line]
                        else:
                            in_target_publication = False
                            current_publication_lines = []

                    elif in_target_publication:
                        current_publication_lines.append(line)

                        # Check if this is the end of the publication
                        if stripped_line.startswith('</article>') or stripped_line.startswith('</inproceedings>'):
                            # Check if publication has meaningful content
                            publication_text = ''.join(current_publication_lines)
                            has_content = ('<author>' in publication_text and
                                         '<title>' in publication_text and
                                         '<year>' in publication_text)

                            if has_content:
                                # Write the publication to output file
                                for pub_line in current_publication_lines:
                                    resolved_line = resolve_entities(pub_line)
                                    if not resolved_line.startswith('\t'):
                                        resolved_line = '\t' + resolved_line
                                    out_file.write(resolved_line)

                                venue_counts[current_venue] += 1

                                if sum(venue_counts.values()) % 1000 == 0:
                                    print(f"    Extracted {sum(venue_counts.values())} publications...")

                            # Reset for next publication
                            in_target_publication = False
                            current_publication_lines = []
                            current_venue = None

            out_file.write('</bib>\n')

        print("Extraction completed:")
        for venue, count in venue_counts.items():
            print(f"  {venue.upper()}: {count} publications")

        return venue_counts

    except Exception as e:
        print(f"Error during extraction: {e}")
        return venue_counts


def extract_venue_publications(dblp_file: str, output_file: str) -> Dict[str, int]:
    """
    Wrapper function that calls the simple extraction method.
    """
    return extract_venue_publications_simple(dblp_file, output_file)


def validate_toy_example_inclusion(extracted_file: str) -> bool:
    """
    Überprüft, ob alle Publikationen aus dem Toy-Beispiel in der extrahierten Datei enthalten sind.
    Verwendet einfache Textsuche statt XML-Parsing.
    """
    print("Validating toy example inclusion...")

    # Erwartete Keys aus dem Toy-Beispiel
    expected_keys = [
        'journals/pvldb/SchmittKAMM23',
        'conf/sigmod/HutterAK0L22',
        'journals/pacmmod/ThielKAHMS23',
        'journals/pvldb/SchalerHS23'
    ]

    found_keys = set()

    try:
        with open(extracted_file, 'r', encoding='utf-8') as f:
            content = f.read()

            for key in expected_keys:
                if f'key="{key}"' in content:
                    found_keys.add(key)

        missing_keys = set(expected_keys) - found_keys
        if missing_keys:
            print(f"ERROR: {len(missing_keys)} publications from toy example are missing:")
            for key in missing_keys:
                print(f"  - {key}")
            return False
        else:
            print("✓ All toy example publications found in extracted data")
            return True

    except Exception as e:
        print(f"Error validating toy example: {e}")
        return False


def count_nikolaus_augsten_publications(extracted_file: str) -> Dict[str, int]:
    """
    Zählt die Publikationen von Nikolaus Augsten pro Venue.
    Verwendet robuste Textsuche mit verschiedenen Namensvariationen.
    Angepasst für das spezielle Format mit mehrzeiligen Publikationen.
    """
    print("Counting Nikolaus Augsten publications...")

    venue_counts = {'vldb': 0, 'sigmod': 0, 'icde': 0}

    # Regex patterns für venue classification
    venue_patterns = {
        'vldb': re.compile(r'key="(conf/vldb/|journals/pvldb/)'),
        'sigmod': re.compile(r'key="(conf/sigmod/|journals/pacmmod/)'),
        'icde': re.compile(r'key="(conf/icde/)')
    }

    # Simple name pattern (most reliable)
    name_pattern = re.compile(r'Nikolaus\s+Augsten', re.IGNORECASE)

    try:
        with open(extracted_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        i = 0
        while i < len(lines):
            line_number = i + 1
            stripped_line = lines[i].strip()

            # Skip non-publication lines
            if not (stripped_line.startswith('<article ') or stripped_line.startswith('<inproceedings ')):
                i += 1
                continue

            # Check venue for this publication
            current_venue = None
            for venue, pattern in venue_patterns.items():
                if pattern.search(stripped_line):
                    current_venue = venue
                    break

            if not current_venue:
                i += 1
                continue

            # Collect the full publication (may span multiple lines)
            pub_type = 'article' if stripped_line.startswith('<article') else 'inproceedings'
            end_tag = f'</{pub_type}>'
            publication_text = stripped_line

            # If the publication doesn't end on the same line, collect more lines
            if end_tag not in stripped_line:
                j = i + 1
                while j < len(lines) and end_tag not in lines[j].strip():
                    publication_text += " " + lines[j].strip()
                    j += 1
                if j < len(lines):
                    publication_text += " " + lines[j].strip()

            # Check if this publication contains Nikolaus Augsten
            if name_pattern.search(publication_text):
                venue_counts[current_venue] += 1
                # Debug: print found publication
                # print(f"Found Nikolaus Augsten in {current_venue} at line {line_number}")

            i += 1

        print("Nikolaus Augsten publications:")
        for venue, count in venue_counts.items():
            print(f"  {venue.upper()}: {count} publications")

        return venue_counts

    except Exception as e:
        print(f"Error counting publications: {e}")
        return venue_counts


def find_toy_example_positions(extracted_file: str) -> Dict[str, str]:
    """
    Findet die genauen Zeilenpositionen der Toy-Beispiel-Publikationen in der extrahierten Datei.
    Angepasst für das spezielle Format mit allen Elementen auf einer Zeile.
    """
    print("Finding toy example publication positions...")

    # Erwartete Keys aus dem Toy-Beispiel
    target_keys = [
        'journals/pvldb/SchmittKAMM23',
        'conf/sigmod/HutterAK0L22',
        'journals/pacmmod/ThielKAHMS23',
        'journals/pvldb/SchalerHS23'
    ]

    positions = {}

    try:
        with open(extracted_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for i, line in enumerate(lines):
            line_number = i + 1  # 1-based line numbering
            stripped_line = line.strip()

            # Check if this line contains one of our target publications
            if stripped_line.startswith('<article ') or stripped_line.startswith('<inproceedings '):
                for key in target_keys:
                    if f'key="{key}"' in stripped_line:
                        # In this format, each publication is on a single line
                        # So start and end line are the same
                        end_line = line_number

                        # Extract just the publication key name for cleaner output
                        key_name = key.split('/')[-1]  # e.g., 'SchmittKAMM23'

                        # Check if it spans multiple lines (look for closing tag on same line)
                        pub_type = 'article' if stripped_line.startswith('<article') else 'inproceedings'
                        end_tag = f'</{pub_type}>'

                        if end_tag not in stripped_line:
                            # Multi-line publication, find the end
                            for j in range(i + 1, len(lines)):
                                if end_tag in lines[j].strip():
                                    end_line = j + 1
                                    break

                        if line_number == end_line:
                            positions[key_name] = f"Line {line_number}"
                        else:
                            positions[key_name] = f"Lines {line_number}-{end_line}"

                        print(f"  {key_name}: {positions[key_name]}")
                        break

        if not positions:
            print("  No toy example publications found in the extracted file")

        return positions

    except Exception as e:
        print(f"Error finding toy example positions: {e}")
        return positions


def parse_extracted_data(file_path: str) -> Dict[str, Dict[str, List[etree._Element]]]:
    """
    Parst die extrahierte my_small_bib.xml und gruppiert nach venue und Jahr.
    """
    parser = etree.XMLParser(
        load_dtd=True,
        no_network=False,
        resolve_entities=True,
        huge_tree=True
    )
    tree = etree.parse(file_path, parser)
    venues: Dict[str, Dict[str, List[etree._Element]]] = defaultdict(
        lambda: defaultdict(list)
    )
    root = tree.getroot()

    # The root element is now <bib> directly
    if root.tag == "bib":
        bib = root
    else:
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


def get_database_statistics(cur: psycopg2.extensions.cursor) -> Tuple[int, int]:
    """
    Gibt die Anzahl der Tupel in den Node- und Edge-Tabellen zurück.
    """
    cur.execute("SELECT COUNT(*) FROM Node;")
    node_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM Edge;")
    edge_count = cur.fetchone()[0]

    return node_count, edge_count


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
    print("  Inserting into database...")
    root_node.insert_to_db(cur, verbose=False)

    conn.commit()

    # 6. Datenbankstatistiken
    node_count, edge_count = get_database_statistics(cur)
    print(f"  Database import completed.")

    # 7. Teste XPath Accelerator
    print("\n6. Testing XPath accelerator...")
    test_queries(cur)

    cur.close()
    conn.close()

    # Zusammenfassung
    print("\n=== Phase 2 Summary ===")
    print("Venue publication counts:")
    for venue, count in venue_counts.items():
        print(f"  {venue.upper()}: {count:,}")

    print("\nNikolaus Augsten publications:")
    for venue, count in augsten_counts.items():
        print(f"  {venue.upper()}: {count}")

    print(f"\nDatabase statistics:")
    print(f"  Node table: {node_count:,} tuples")
    print(f"  Edge table: {edge_count:,} tuples")

    print(f"\nValidation status:")
    print(f"  Toy example inclusion: {'✓ PASS' if validation_success else '✗ FAIL'}")

    print(f"\nToy example publication positions:")
    if toy_positions:
        for pub_key, position in toy_positions.items():
            print(f"  {pub_key}: {position}")
    else:
        print("  No toy example publications found")

    print(f"\nFile metrics:")
    print(f"  Size: {file_size_kb:.1f} KB")
    print(f"  Lines: {line_count:,}")


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
    root_node.insert_to_db(cur, verbose=True)

    print("Datenbank erfolgreich mit Knoten und Kanten gefüllt.")
    conn.commit()

    test_queries(cur)

    cur.close()
    conn.close()


def select_phase() -> int:
    """
    Allows user to select which phase to run.
    Returns 1 for Phase 1 (toy example) or 2 for Phase 2 (DBLP processing).
    """
    import sys

    # Check for command line argument
    if len(sys.argv) > 1:
        try:
            phase = int(sys.argv[1])
            if phase in [1, 2]:
                return phase
        except ValueError:
            pass

    # Check environment variable
    import os
    env_phase = os.environ.get('XPATH_PHASE')
    if env_phase:
        try:
            phase = int(env_phase)
            if phase in [1, 2]:
                return phase
        except ValueError:
            pass

    # Interactive prompt
    while True:
        try:
            print("XPath Accelerator - Phase Selection")
            print("1. Phase 1: Toy Example Processing")
            print("2. Phase 2: DBLP Data Processing")
            choice = input("Select phase (1 or 2): ").strip()

            if choice in ['1', '2']:
                return int(choice)
            else:
                print("Invalid choice. Please enter 1 or 2.")
        except (KeyboardInterrupt, EOFError):
            print("\nExiting...")
            sys.exit(0)


if __name__ == "__main__":
    phase = select_phase()

    if phase == 1:
        print("Running Phase 1: Toy Example Processing\n")
        main()
    else:
        print("Running Phase 2: DBLP Data Processing\n")
        main_phase2()
