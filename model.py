"""
Node-Klasse und Baumaufbau für das XPath Accelerator System.
"""
from typing import Dict, List, Optional
from lxml import etree
import psycopg2.extensions


class Node:
    """
    Repräsentiert einen Knoten im XPath Accelerator EDGE Model mit beliebig vielen Kindern.
    Implementiert post-order numbering für effiziente XPath-Abfragen.
    Nach dem Einfügen in die DB speichert 'db_id' die generierte ID.
    """

    def __init__(
        self,
        type_: str,
        content: Optional[str] = None,
        s_id: Optional[str] = None,
        attributes: Optional[Dict[str, str]] = None
    ) -> None:
        self.type: str = type_
        self.content: Optional[str] = content
        self.children: List["Node"] = []
        self.db_id: Optional[int] = None
        self.s_id: Optional[str] = s_id
        self.attributes: Dict[str, str] = attributes or {}
        self.pre_order: Optional[int] = None
        self.post_order: Optional[int] = None

    def add_child(self, child: "Node") -> None:
        """Fügt diesem Knoten ein Kind hinzu."""
        self.children.append(child)

    def calculate_traversal_orders(self, pre_counter: List[int], post_counter: List[int]) -> None:
        """
        Berechnet sowohl Pre-Order- als auch Post-Order-Nummerierung für diesen Knoten und alle Kinder.
        Pre-Order: Knoten wird nummeriert, bevor die Kinder besucht werden.
        Post-Order: Knoten wird nummeriert, nachdem alle Kinder besucht wurden.
        """
        # Pre-Order: Nummeriere diesen Knoten zuerst
        self.pre_order = pre_counter[0]
        pre_counter[0] += 1

        # Dann alle Kinder besuchen
        for child in self.children:
            child.calculate_traversal_orders(pre_counter, post_counter)

        # Post-Order: Nummeriere diesen Knoten nach den Kindern
        self.post_order = post_counter[0]
        post_counter[0] += 1

    def calculate_post_order(self, counter: List[int]) -> int:
        """
        Berechnet die Post-Order-Nummerierung für diesen Knoten und alle Kinder.
        Post-Order: Kinder werden vor dem Parent nummeriert.
        (Backward compatibility method)
        """
        # Erst alle Kinder nummerieren
        for child in self.children:
            child.calculate_post_order(counter)

        # Dann diesen Knoten nummerieren
        self.post_order = counter[0]
        counter[0] += 1
        return self.post_order

    def insert_to_db(
        self,
        cur: psycopg2.extensions.cursor,
        parent_id: Optional[int] = None,
        verbose: bool = False
    ) -> None:
        """
        Fügt diesen Knoten in das XPath Accelerator Schema ein:
        - accel: Core node information with post-order numbering
        - content: Node textual content (if any)
        - attribute: Node XML attributes (if any)

        Note: Post-order numbering should be calculated before calling this method.
        """
        # Generate unique ID if not set
        if self.db_id is None:
            # Use post-order number as ID for consistency
            self.db_id = self.post_order

        # Insert into accel table
        cur.execute(
            "INSERT INTO accel (id, pre_order, post_order, s_id, parent, type) VALUES (%s, %s, %s, %s, %s, %s);",
            (self.db_id, self.pre_order, self.post_order, self.s_id, parent_id, self.type)
        )

        # Insert content if present
        if self.content is not None and self.content.strip():
            cur.execute(
                "INSERT INTO content (id, text) VALUES (%s, %s);",
                (self.db_id, self.content)
            )

        # Insert attributes if present
        for attr_name, attr_value in self.attributes.items():
            cur.execute(
                "INSERT INTO attribute (id, text) VALUES (%s, %s);",
                (self.db_id, f"{attr_name}={attr_value}")
            )

        # Recursively insert children
        for child in self.children:
            child.insert_to_db(cur, self.db_id, verbose)

    def insert_to_original_db(
        self,
        cur: psycopg2.extensions.cursor,
        parent_id: Optional[int] = None,
        position: int = 0,
        verbose: bool = False
    ) -> None:
        """
        Fügt diesen Knoten in das Original Node/Edge Schema ein (Phase 1 Kompatibilität).
        Verwendet SERIAL PRIMARY KEY für automatische ID-Zuweisung.
        """
        cur.execute(
            "INSERT INTO Node (s_id, type, content) VALUES (%s, %s, %s) RETURNING id;",
            (self.s_id, self.type, self.content)
        )
        self.db_id = cur.fetchone()[0]

        if parent_id is not None:
            cur.execute(
                "INSERT INTO Edge (from_node, to_node, position) VALUES (%s, %s, %s);",
                (parent_id, self.db_id, position)
            )

        for idx, child in enumerate(self.children):
            child.insert_to_original_db(cur, self.db_id, idx, verbose)


def build_edge_model(
    venues: Dict[str, Dict[str, List[etree._Element]]]
) -> Node:
    """
    Baut den Baum nach dem EDGE Model auf:
    bib -> venue -> year -> Publikationen -> Kinder (author, title, ...).
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


def annotate_traversal_orders(root_node: Node) -> None:
    """
    Annotates all nodes in the dataset with their corresponding pre-order and post-order
    traversal numbers. This is the main function for Task 3.

    Args:
        root_node: The root node of the XML tree structure

    The function modifies the nodes in-place, setting their pre_order and post_order attributes.
    """
    print("Annotating all nodes with pre-order and post-order traversal numbers...")

    # Initialize counters
    pre_counter = [1]   # Start from 1
    post_counter = [1]  # Start from 1

    # Calculate traversal orders for the entire tree
    root_node.calculate_traversal_orders(pre_counter, post_counter)

    print(f"Annotation complete: {pre_counter[0] - 1} nodes processed")
    print(f"Pre-order range: 1 to {pre_counter[0] - 1}")
    print(f"Post-order range: 1 to {post_counter[0] - 1}")
