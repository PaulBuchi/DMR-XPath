# axes.py
"""
XPath-Achsenfunktionen (ancestor, descendant, siblings, etc.)
"""
from typing import List, Optional, Tuple
import psycopg2


def ancestor_nodes(
    cur: psycopg2.extensions.cursor,
    node_content: any
) -> List[Tuple[int, str, Optional[str]]]:
    """
    Berechnet alle ancestor-Knoten eines gegebenen Knotens in der DB.
    Funktioniert mit beiden Schemas (Node/Edge und accel/content).
    """
    # Check which schema is being used
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
    has_accel = cur.fetchone()[0]

    if has_accel:
        # Use new accel/content schema
        cur.execute(
            """WITH RECURSIVE ancestors(id) AS (
                SELECT a.parent
                FROM accel a
                JOIN content c ON a.id = c.id
                WHERE a.type = 'author' AND c.text = %s AND a.parent IS NOT NULL
                UNION
                SELECT a.parent
                FROM ancestors anc
                JOIN accel a ON anc.id = a.id
                WHERE a.parent IS NOT NULL
                )
                SELECT a.id, a.s_id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.id IN (SELECT id FROM ancestors);""",
            (node_content, )
        )
    else:
        # Use original Node/Edge schema
        cur.execute(
            """WITH RECURSIVE ancestors(id) AS (
                SELECT e.from_node FROM Node n JOIN Edge e ON n.id = e.to_node
                WHERE n.type = 'author' AND n.content = %s
                UNION
                SELECT e.from_node FROM ancestors a JOIN Edge e ON a.id = e.to_node
                )
                SELECT n.id, n.s_id, n.type, n.content FROM Node n
                WHERE n.id IN (SELECT id FROM ancestors)
                ORDER BY n.id;""",
            (node_content, )
        )
    return cur.fetchall()


def descendant_nodes(
    cur: psycopg2.extensions.cursor,
    node_id: int
) -> List[Tuple[int, str, Optional[str]]]:
    """
    Berechnet alle descendant-Knoten eines gegebenen Knotens in der DB.
    Funktioniert mit beiden Schemas (Node/Edge und accel/content).
    """
    # Check which schema is being used
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
    has_accel = cur.fetchone()[0]

    if has_accel:
        # Use new accel/content schema
        cur.execute(
            """
            WITH RECURSIVE descendants(id) AS (
                SELECT id FROM accel WHERE parent = %s
                UNION
                SELECT a.id
                FROM accel a
                JOIN descendants d ON a.parent = d.id
            )
            SELECT DISTINCT a.id, a.type, c.text
            FROM accel a
            LEFT JOIN content c ON a.id = c.id
            WHERE a.id IN (SELECT id FROM descendants);
            """,
            (node_id,)
        )
    else:
        # Use original Node/Edge schema
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
            JOIN Descendants ON Node.id = Descendants.to_node
            ORDER BY Node.id;
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
    vom Typ <article>. Funktioniert mit beiden Schemas.
    direction muss 'following' oder 'preceding' sein.
    """
    # Check which schema is being used
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
    has_accel = cur.fetchone()[0]

    if has_accel:
        # Use new accel/content schema
        cur.execute("SELECT type, parent, post_order FROM accel WHERE id = %s;", (node_id,))
        row = cur.fetchone()
        if row is None or row[0] != "article":
            return []

        _, parent_id, my_post = row
        if not parent_id:
            return []

        if direction == "following":
            cur.execute(
                """
                SELECT a.id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.parent = %s
                  AND a.type = 'article'
                  AND a.post_order > %s
                ORDER BY a.post_order;
                """,
                (parent_id, my_post)
            )
        else:  # preceding
            cur.execute(
                """
                SELECT a.id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.parent = %s
                  AND a.type = 'article'
                  AND a.post_order < %s
                ORDER BY a.post_order DESC;
                """,
                (parent_id, my_post)
            )
    else:
        # Use original Node/Edge schema
        cur.execute("SELECT type FROM Node WHERE id = %s;", (node_id,))
        row = cur.fetchone()
        if row is None or row[0] != "article":
            return []

        # Get parent
        cur.execute("SELECT from_node FROM Edge WHERE to_node = %s;", (node_id,))
        parent = cur.fetchone()
        if not parent:
            return []
        parent_id = parent[0]

        # Get position
        cur.execute("SELECT position FROM Edge WHERE to_node = %s;", (node_id,))
        pos_row = cur.fetchone()
        if not pos_row:
            return []
        my_position = pos_row[0]

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


def xpath_ancestor_window_original(cur: psycopg2.extensions.cursor, context_node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    """
    Implements the ancestor axis using the original Node/Edge schema.
    Uses recursive CTE to find all ancestor nodes.

    Special case: If the context node is an author, find ancestors of ALL authors with the same content
    to match the behavior of the recursive ancestor_nodes function.
    """
    # Check if this is an author node
    cur.execute("SELECT type, content FROM Node WHERE id = %s;", (context_node_id,))
    node_info = cur.fetchone()

    if node_info and node_info[0] == 'author' and node_info[1]:
        # For author nodes, use the same logic as ancestor_nodes function
        author_content = node_info[1]
        cur.execute("""
            WITH RECURSIVE ancestors(id) AS (
                SELECT e.from_node FROM Node n JOIN Edge e ON n.id = e.to_node
                WHERE n.type = 'author' AND n.content = %s
                UNION
                SELECT e.from_node FROM ancestors a JOIN Edge e ON a.id = e.to_node
            )
            SELECT n.id, n.type, n.content FROM Node n
            WHERE n.id IN (SELECT id FROM ancestors)
            ORDER BY n.id;
        """, (author_content,))
    else:
        # For non-author nodes, find direct ancestors
        cur.execute("""
            WITH RECURSIVE ancestors(id) AS (
                SELECT e.from_node FROM Edge e WHERE e.to_node = %s
                UNION
                SELECT e.from_node FROM ancestors a JOIN Edge e ON a.id = e.to_node
            )
            SELECT n.id, n.type, n.content FROM Node n
            WHERE n.id IN (SELECT id FROM ancestors)
            ORDER BY n.id;
        """, (context_node_id,))

    return cur.fetchall()


def xpath_descendant_window_original(cur: psycopg2.extensions.cursor, context_node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    """
    Implements the descendant axis using the original Node/Edge schema.
    Uses recursive CTE to find all descendant nodes.
    """
    cur.execute("""
        WITH RECURSIVE descendants(from_node, to_node) AS (
            SELECT from_node, to_node FROM Edge WHERE from_node = %s
            UNION
            SELECT e.from_node, e.to_node
            FROM Edge e
            JOIN descendants d ON e.from_node = d.to_node
        )
        SELECT DISTINCT n.id, n.type, n.content
        FROM Node n
        JOIN descendants d ON n.id = d.to_node
        ORDER BY n.id;
    """, (context_node_id,))
    return cur.fetchall()


def xpath_following_sibling_window_original(cur: psycopg2.extensions.cursor, context_node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    """
    Implements the following-sibling axis using the original Node/Edge schema.
    """
    # Get node type and parent
    cur.execute("SELECT type FROM Node WHERE id = %s;", (context_node_id,))
    row = cur.fetchone()
    if row is None or row[0] != "article":
        return []

    # Get parent
    cur.execute("SELECT from_node FROM Edge WHERE to_node = %s;", (context_node_id,))
    parent = cur.fetchone()
    if not parent:
        return []
    parent_id = parent[0]

    # Get position
    cur.execute("SELECT position FROM Edge WHERE to_node = %s;", (context_node_id,))
    pos_row = cur.fetchone()
    if not pos_row:
        return []
    my_position = pos_row[0]

    cur.execute("""
        SELECT n.id, n.type, n.content
        FROM Edge e
        JOIN Node n ON e.to_node = n.id
        WHERE e.from_node = %s
          AND e.position > %s
          AND n.type = 'article'
        ORDER BY e.position;
    """, (parent_id, my_position))

    return cur.fetchall()


def xpath_preceding_sibling_window_original(cur: psycopg2.extensions.cursor, context_node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    """
    Implements the preceding-sibling axis using the original Node/Edge schema.
    """
    # Get node type and parent
    cur.execute("SELECT type FROM Node WHERE id = %s;", (context_node_id,))
    row = cur.fetchone()
    if row is None or row[0] != "article":
        return []

    # Get parent
    cur.execute("SELECT from_node FROM Edge WHERE to_node = %s;", (context_node_id,))
    parent = cur.fetchone()
    if not parent:
        return []
    parent_id = parent[0]

    # Get position
    cur.execute("SELECT position FROM Edge WHERE to_node = %s;", (context_node_id,))
    pos_row = cur.fetchone()
    if not pos_row:
        return []
    my_position = pos_row[0]

    cur.execute("""
        SELECT n.id, n.type, n.content
        FROM Edge e
        JOIN Node n ON e.to_node = n.id
        WHERE e.from_node = %s
          AND e.position < %s
          AND n.type = 'article'
        ORDER BY e.position DESC;
    """, (parent_id, my_position))

    return cur.fetchall()


def xpath_ancestor_window(cur: psycopg2.extensions.cursor, context_node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    """
    Implements the ancestor axis using SQL window functions.
    Works with both Node/Edge and accel/content schemas.

    Formula: ancestor(v) = {u | pre_order(u) < pre_order(v) AND post_order(u) > post_order(v)}

    Args:
        cur: Database cursor
        context_node_id: ID of the context node

    Returns:
        List of tuples (id, type, content) for ancestor nodes
    """
    # Check which schema is being used
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
    has_accel = cur.fetchone()[0]

    if has_accel:
        # Use new accel/content schema with pre/post-order numbers
        cur.execute("""
            SELECT a.type, c.text, a.pre_order, a.post_order
            FROM accel a
            LEFT JOIN content c ON a.id = c.id
            WHERE a.id = %s;
        """, (context_node_id,))

        result = cur.fetchone()
        if not result:
            return []

        node_type, node_content, context_pre, context_post = result

        # Special case: If this is an author node, find ancestors of ALL authors with same content
        if node_type == 'author' and node_content:
            # Use the same logic as ancestor_nodes function for consistency
            cur.execute("""
                WITH RECURSIVE ancestors(id) AS (
                    SELECT a.parent
                    FROM accel a
                    JOIN content c ON a.id = c.id
                    WHERE a.type = 'author' AND c.text = %s AND a.parent IS NOT NULL
                    UNION
                    SELECT a.parent
                    FROM ancestors anc
                    JOIN accel a ON anc.id = a.id
                    WHERE a.parent IS NOT NULL
                )
                SELECT a.id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.id IN (SELECT id FROM ancestors)
                ORDER BY a.id;
            """, (node_content,))
        else:
            # Use window function approach to find ancestors
            cur.execute("""
                SELECT a.id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.pre_order < %s
                  AND a.post_order > %s
                ORDER BY a.pre_order;
            """, (context_pre, context_post))

        return cur.fetchall()
    else:
        # Use original Node/Edge schema with recursive approach
        return xpath_ancestor_window_original(cur, context_node_id)


def xpath_descendant_window(cur: psycopg2.extensions.cursor, context_node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    """
    Implements the descendant axis using SQL window functions.
    Works with both Node/Edge and accel/content schemas.

    Formula: descendant(v) = {u | pre_order(u) > pre_order(v) AND post_order(u) < post_order(v)}

    Args:
        cur: Database cursor
        context_node_id: ID of the context node

    Returns:
        List of tuples (id, type, content) for descendant nodes
    """
    # Check which schema is being used
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
    has_accel = cur.fetchone()[0]

    if has_accel:
        # Use new accel/content schema with pre/post-order numbers
        cur.execute("""
            SELECT pre_order, post_order
            FROM accel
            WHERE id = %s;
        """, (context_node_id,))

        result = cur.fetchone()
        if not result:
            return []

        context_pre, context_post = result

        # Use window function approach to find descendants
        cur.execute("""
            SELECT a.id, a.type, c.text
            FROM accel a
            LEFT JOIN content c ON a.id = c.id
            WHERE a.pre_order > %s
              AND a.post_order < %s
            ORDER BY a.pre_order;
        """, (context_pre, context_post))

        return cur.fetchall()
    else:
        # Use original Node/Edge schema with recursive approach
        return xpath_descendant_window_original(cur, context_node_id)


def xpath_following_sibling_window(cur: psycopg2.extensions.cursor, context_node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    """
    Implements the following-sibling axis using SQL window functions.
    Works with both Node/Edge and accel/content schemas.

    Formula: following-sibling(v) = {u | parent(u) = parent(v) AND pre_order(u) > pre_order(v)}

    Args:
        cur: Database cursor
        context_node_id: ID of the context node

    Returns:
        List of tuples (id, type, content) for following sibling nodes
    """
    # Check which schema is being used
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
    has_accel = cur.fetchone()[0]

    if has_accel:
        # Use new accel/content schema
        cur.execute("""
            SELECT parent, pre_order, type
            FROM accel
            WHERE id = %s;
        """, (context_node_id,))

        result = cur.fetchone()
        if not result or result[0] is None:  # No parent means no siblings
            return []

        context_parent, context_pre, context_type = result

        # For article nodes, only return article siblings
        if context_type == 'article':
            cur.execute("""
                SELECT a.id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.parent = %s
                  AND a.pre_order > %s
                  AND a.type = 'article'
                ORDER BY a.pre_order;
            """, (context_parent, context_pre))
        else:
            # For other node types, return all siblings
            cur.execute("""
                SELECT a.id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.parent = %s
                  AND a.pre_order > %s
                ORDER BY a.pre_order;
            """, (context_parent, context_pre))

        return cur.fetchall()
    else:
        # Use original Node/Edge schema
        return xpath_following_sibling_window_original(cur, context_node_id)


def xpath_preceding_sibling_window(cur: psycopg2.extensions.cursor, context_node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    """
    Implements the preceding-sibling axis using SQL window functions.
    Works with both Node/Edge and accel/content schemas.

    Formula: preceding-sibling(v) = {u | parent(u) = parent(v) AND pre_order(u) < pre_order(v)}

    Args:
        cur: Database cursor
        context_node_id: ID of the context node

    Returns:
        List of tuples (id, type, content) for preceding sibling nodes
    """
    # Check which schema is being used
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'accel');")
    has_accel = cur.fetchone()[0]

    if has_accel:
        # Use new accel/content schema
        cur.execute("""
            SELECT parent, pre_order, type
            FROM accel
            WHERE id = %s;
        """, (context_node_id,))

        result = cur.fetchone()
        if not result or result[0] is None:  # No parent means no siblings
            return []

        context_parent, context_pre, context_type = result

        # For article nodes, only return article siblings
        if context_type == 'article':
            cur.execute("""
                SELECT a.id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.parent = %s
                  AND a.pre_order < %s
                  AND a.type = 'article'
                ORDER BY a.pre_order;
            """, (context_parent, context_pre))
        else:
            # For other node types, return all siblings
            cur.execute("""
                SELECT a.id, a.type, c.text
                FROM accel a
                LEFT JOIN content c ON a.id = c.id
                WHERE a.parent = %s
                  AND a.pre_order < %s
                ORDER BY a.pre_order;
            """, (context_parent, context_pre))

        return cur.fetchall()
    else:
        # Use original Node/Edge schema
        return xpath_preceding_sibling_window_original(cur, context_node_id)