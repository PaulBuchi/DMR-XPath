# xml_parser.py
"""
XML-Parsing-Funktionen:
 - parse_toy_example: Liest dein Toy-XML ein und gruppiert Publikationen.
 - extract_venue_counts: Zählt SIGMOD/VLDB/ICDE-Tags per Regex.
"""

import os
import re
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from lxml import etree

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



def extract_venue_publications(
    dblp_file: str,
    output_file: str,
    max_pubs: Optional[int] = None
) -> Dict[str, int]:
    """
    Extrahiert alle <article> und <inproceedings> per Streaming-Parser,
    schreibt sie pretty-printed mit Einrückung und bricht ab, sobald
    insgesamt max_pubs Publications geschrieben wurden (wenn gesetzt).

    :param dblp_file:   Pfad zur DBLP-XML-Datei
    :param output_file: Pfad zur Ausgabedatei (XML)
    :param max_pubs:    Optional: Maximale Anzahl zu extrahierender Publikationen
    :return:            Dict[venue, count]
    """
    max_pubs = 10000
    venue_prefixes = {
        'vldb':   ('conf/vldb/', 'journals/pvldb/'),
        'sigmod': ('conf/sigmod/', 'journals/pacmmod/'),
        'icde':   ('conf/icde/',),
    }
    venue_counts = dict.fromkeys(venue_prefixes.keys(), 0)
    total_written = 0

    header = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<!DOCTYPE dblp SYSTEM "dblp.dtd">\n'
        '<bib>\n'
    )
    footer = '</bib>\n'

    with open(output_file, 'w', encoding='utf-8') as out:
        out.write(header)

        context = etree.iterparse(
            dblp_file,
            events=('end',),
            tag=('article', 'inproceedings'),
            recover=True,
            huge_tree=True
        )
        # Initialisiere den Parser mit dem DTD
        for _, elem in context:
            # Prüfe, ob das Element ein <article> oder <inproceedings> ist
            key = (elem.get('key') or '').lower()
            # Prüfe, ob der Key mit einem der Venue-Präfixe beginnt
            for venue, prefixes in venue_prefixes.items():
                # Wenn der Key mit einem der Präfixe beginnt, extrahiere das Elementund schreibe es in die Ausgabedatei
                if any(key.startswith(p) for p in prefixes):
                    xml_str = etree.tostring(
                        elem,
                        encoding='unicode',
                        pretty_print=True
                    )
                    xml_str = resolve_entities(xml_str)

                    lines = xml_str.splitlines()
                    for idx, line in enumerate(lines):
                        # 1 Tab für Start-/End-Tag, 2 Tabs für Kindelemente
                        indent = '\t' if idx in (0, len(lines)-1) else '\t\t'
                        out.write(f'{indent}{line}\n')

                    venue_counts[venue] += 1
                    total_written += 1
                    break  # nicht weitere Venues prüfen

            # Speicher freigeben, damit der Parser klein bleibt
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

            # Abbruch, wenn Limit erreicht
            if max_pubs is not None and total_written >= max_pubs:
                print(f"Reached limit of {max_pubs} publications, stopping early.")
                break

        out.write(footer)

    print("Extraction completed:")
    for vn, cnt in venue_counts.items():
        print(f"  {vn.upper():6s}: {cnt} publications")

    return venue_counts


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
