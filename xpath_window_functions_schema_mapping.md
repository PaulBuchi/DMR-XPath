# XPath Axes as Window Functions - Schema Mapping

## Dual Schema Support

Our implementation supports two database schemas:

### Schema 1: Original Node/Edge (Phase 1 Compatible)
```sql
CREATE TABLE Node (
    id SERIAL PRIMARY KEY,
    s_id TEXT,
    type TEXT,
    content TEXT
);

CREATE TABLE Edge (
    id SERIAL PRIMARY KEY,
    from_node INTEGER REFERENCES Node(id),
    to_node INTEGER REFERENCES Node(id),
    position INTEGER
);
```

### Schema 2: XPath Accelerator (Phase 2 Advanced)
```sql
CREATE TABLE accel (
    id INT PRIMARY KEY,
    pre_order INT NOT NULL,
    post_order INT NOT NULL,
    s_id VARCHAR(255),
    parent INT,
    type VARCHAR(50),
    FOREIGN KEY (parent) REFERENCES accel(id)
);

CREATE TABLE content (
    id INT PRIMARY KEY,
    text TEXT,
    FOREIGN KEY (id) REFERENCES accel(id)
);

CREATE TABLE attribute (
    id INT,
    text TEXT,
    PRIMARY KEY (id, text),
    FOREIGN KEY (id) REFERENCES accel(id)
);
```

## XPath Axes Window Function Mapping

Based on the lecture slides (Chapter 2.2, Slide 19), each XPath axis can be expressed using pre-order and post-order numbers with window functions.

### 1. Ancestor Axis

**Theoretical Formula:**
```
ancestor(v) = {u | pre_order(u) < pre_order(v) AND post_order(u) > post_order(v)}
```

**Window Function Mapping:**
- **Window**: All nodes ordered by pre_order
- **Condition**: For context node v, select nodes u where:
  - `u.pre_order < v.pre_order` 
  - `u.post_order > v.post_order`
- **Window Function**: Use `LAG()` to look back in pre-order sequence
- **Result**: Nodes that started before v and ended after v (ancestors)

### 2. Descendant Axis

**Theoretical Formula:**
```
descendant(v) = {u | pre_order(u) > pre_order(v) AND post_order(u) < post_order(v)}
```

**Window Function Mapping:**
- **Window**: All nodes ordered by pre_order
- **Condition**: For context node v, select nodes u where:
  - `u.pre_order > v.pre_order`
  - `u.post_order < v.post_order`
- **Window Function**: Use `LEAD()` to look ahead in pre-order sequence
- **Result**: Nodes that started after v and ended before v (descendants)

### 3. Following-Sibling Axis

**Theoretical Formula:**
```
following-sibling(v) = {u | parent(u) = parent(v) AND pre_order(u) > pre_order(v)}
```

**Window Function Mapping:**
- **Window**: Nodes with same parent, ordered by pre_order
- **Partition**: `PARTITION BY parent`
- **Condition**: For context node v, select nodes u where:
  - `u.parent = v.parent`
  - `u.pre_order > v.pre_order`
- **Window Function**: Use `LEAD()` within partition to find following siblings
- **Result**: Siblings that appear after v in document order

### 4. Preceding-Sibling Axis

**Theoretical Formula:**
```
preceding-sibling(v) = {u | parent(u) = parent(v) AND pre_order(u) < pre_order(v)}
```

**Window Function Mapping:**
- **Window**: Nodes with same parent, ordered by pre_order
- **Partition**: `PARTITION BY parent`
- **Condition**: For context node v, select nodes u where:
  - `u.parent = v.parent`
  - `u.pre_order < v.pre_order`
- **Window Function**: Use `LAG()` within partition to find preceding siblings
- **Result**: Siblings that appear before v in document order

## Schema-Specific Implementation Strategy

### Input Processing
For each axis function, given a context node ID:
1. **Retrieve context node info**: Query `accel` table to get `pre_order`, `post_order`, and `parent` values
2. **Apply window function**: Use appropriate window function with the retrieved values
3. **Filter results**: Apply axis-specific conditions using the context node's traversal numbers

### Window Function Approach Benefits
1. **Efficiency**: Single-pass computation using pre/post-order numbers
2. **Scalability**: Avoids recursive queries for large datasets
3. **Simplicity**: Direct mathematical relationships instead of tree traversal
4. **Optimization**: Database can optimize window functions better than recursive CTEs

### Key Insight: Pre/Post-Order Relationships
The power of this approach lies in the mathematical properties of pre/post-order numbering:

- **Ancestor relationship**: `pre(ancestor) < pre(node) < post(node) < post(ancestor)`
- **Descendant relationship**: `pre(node) < pre(descendant) < post(descendant) < post(node)`
- **Sibling relationship**: Same parent + different pre-order positions

This enables direct computation without tree traversal, making window functions ideal for XPath axis evaluation.

## Implementation Notes

1. **Context Node Lookup**: Each function starts by querying the context node's traversal numbers
2. **Window Boundaries**: Use UNBOUNDED PRECEDING/FOLLOWING for ancestor/descendant axes
3. **Partition Strategy**: Use parent-based partitioning for sibling axes
4. **Result Ordering**: Maintain document order (pre-order) in results
5. **Performance**: Window functions should outperform recursive approaches on large datasets
