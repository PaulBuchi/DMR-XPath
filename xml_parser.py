# xml_parser.py
"""
XML-Parsing-Funktionen:
 - parse_toy_example: Liest dein Toy-XML ein und gruppiert Publikationen.
 - extract_venue_counts: Zählt SIGMOD/VLDB/ICDE-Tags per Regex.
"""

import re
from collections import defaultdict
from typing import Dict, List, Optional
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
