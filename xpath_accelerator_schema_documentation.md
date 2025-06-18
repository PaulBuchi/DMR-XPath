# XPath Accelerator Database Schema Documentation

## Overview

The XPath Accelerator system implements a specialized database schema optimized for efficient XPath query processing over XML data. The schema is based on the EDGE (Extended Document Graph Encoding) model and consists of three primary tables that work together to represent XML documents in a way that enables fast ancestor, descendant, and sibling queries.

## Schema Design

### Table 1: `accel` - Core Node Table

```sql
CREATE TABLE accel (
    id INT PRIMARY KEY,
    post INT NOT NULL,
    s_id VARCHAR(255),
    parent INT,
    type VARCHAR(50),
    FOREIGN KEY (parent) REFERENCES accel(id)
);
```

**Semantic Meaning:**
The `accel` table is the core table that represents the hierarchical structure of XML documents using the EDGE model. Each row represents a single node in the XML tree.

**Column Descriptions:**
- **`id`**: Unique identifier for each node. In our implementation, this corresponds to the post-order number for consistency and optimization.
- **`post`**: Post-order traversal number. This is crucial for efficient XPath queries as it enables fast ancestor/descendant relationships through simple numeric comparisons.
- **`s_id`**: Semantic identifier for nodes that have special meaning (e.g., publication keys like "SchmittKAMM23", venue-year combinations like "vldb_2023").
- **`parent`**: Foreign key reference to the parent node's `id`. NULL for the root node. This creates the tree hierarchy.
- **`type`**: The XML element type (e.g., "bib", "venue", "year", "article", "author", "title").

### Table 2: `content` - Node Content Storage

```sql
CREATE TABLE content (
    id INT PRIMARY KEY,
    text TEXT,
    FOREIGN KEY (id) REFERENCES accel(id)
);
```

**Semantic Meaning:**
The `content` table stores the textual content of XML nodes. This separation allows for optimization - nodes without content don't consume space in this table, and text-based searches can be performed efficiently.

**Column Descriptions:**
- **`id`**: Foreign key reference to the corresponding node in the `accel` table.
- **`text`**: The textual content of the XML element (e.g., author names, publication titles, years).

**Design Rationale:**
- Separating content from structure allows for better storage optimization
- Enables efficient text-based queries without scanning structural information
- Reduces the size of the main `accel` table for structural queries

### Table 3: `attribute` - Node Attributes Storage

```sql
CREATE TABLE attribute (
    id INT,
    text TEXT,
    PRIMARY KEY (id, text),
    FOREIGN KEY (id) REFERENCES accel(id)
);
```

**Semantic Meaning:**
The `attribute` table stores XML attributes as key-value pairs. Each row represents one attribute of an XML element.

**Column Descriptions:**
- **`id`**: Foreign key reference to the corresponding node in the `accel` table.
- **`text`**: The attribute in "key=value" format (e.g., "key=conf/vldb/SchmittKAMM23").

**Design Rationale:**
- Supports multiple attributes per node through the composite primary key
- Flexible storage for any XML attributes without schema changes
- Enables efficient attribute-based queries

## EDGE Model Implementation

### Post-Order Numbering

The schema implements post-order numbering in the `post` column of the `accel` table. This is fundamental to the EDGE model's efficiency:

1. **Post-order traversal**: Children are numbered before their parents
2. **Ancestor/Descendant relationships**: Node A is an ancestor of Node B if A.post > B.post and A is on the path from root to B
3. **Sibling relationships**: Siblings share the same parent and can be ordered by their post numbers

### Hierarchical Structure

The `parent` foreign key in the `accel` table creates the tree structure:
- Root node has `parent = NULL`
- Each node references its immediate parent
- Tree traversal is possible through recursive queries

### Example Data Structure

For a simplified XML document:
```xml
<bib>
  <venue content="vldb">
    <year content="2023" s_id="vldb_2023">
      <article s_id="SchmittKAMM23">
        <author>Daniel Ulrich Schmitt</author>
        <title>Example Title</title>
      </article>
    </year>
  </venue>
</bib>
```

The tables would contain:

**accel table:**
| id | post | s_id | parent | type |
|----|------|------|--------|------|
| 1  | 1    | NULL | NULL   | bib |
| 2  | 2    | NULL | 1      | venue |
| 3  | 3    | vldb_2023 | 2 | year |
| 4  | 4    | SchmittKAMM23 | 3 | article |
| 5  | 5    | NULL | 4      | author |
| 6  | 6    | NULL | 4      | title |

**content table:**
| id | text |
|----|------|
| 2  | vldb |
| 3  | 2023 |
| 5  | Daniel Ulrich Schmitt |
| 6  | Example Title |

## Query Optimization

### Ancestor Queries
```sql
-- Find all ancestors of a node with specific content
WITH RECURSIVE ancestors(id) AS (
    SELECT a.parent FROM accel a JOIN content c ON a.id = c.id
    WHERE a.type = 'author' AND c.text = 'Daniel Ulrich Schmitt'
    UNION
    SELECT a.parent FROM ancestors anc JOIN accel a ON anc.id = a.id
    WHERE a.parent IS NOT NULL
)
SELECT a.id, a.type, c.text FROM accel a LEFT JOIN content c ON a.id = c.id
WHERE a.id IN (SELECT id FROM ancestors);
```

### Descendant Queries
```sql
-- Find all descendants of a node
WITH RECURSIVE descendants(id) AS (
    SELECT id FROM accel WHERE parent = ?
    UNION
    SELECT a.id FROM accel a JOIN descendants d ON a.parent = d.id
)
SELECT a.id, a.type, c.text FROM accel a LEFT JOIN content c ON a.id = c.id
WHERE a.id IN (SELECT id FROM descendants);
```

### Sibling Queries
```sql
-- Find following siblings using post-order numbers
SELECT a.id, a.type, c.text FROM accel a LEFT JOIN content c ON a.id = c.id
WHERE a.parent = ? AND a.type = 'article' AND a.post > ?
ORDER BY a.post;
```

## Benefits of This Schema

1. **Efficient XPath Processing**: Post-order numbering enables fast ancestor/descendant queries
2. **Storage Optimization**: Content and attributes are stored separately, reducing space for structural queries
3. **Scalability**: The schema handles large XML documents efficiently
4. **Flexibility**: Supports arbitrary XML structures without schema modifications
5. **Query Performance**: Optimized for the most common XPath operations

## Integration with Application

The schema integrates with the XPath accelerator application through:
- **Node class**: Represents XML elements with post-order calculation
- **Query functions**: Implement XPath axes (ancestor, descendant, sibling)
- **Data import**: Converts XML documents to the EDGE representation
- **Test framework**: Validates query correctness and performance
