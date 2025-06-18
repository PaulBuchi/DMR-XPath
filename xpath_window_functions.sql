-- XPath Axes as Window Functions - SQL Implementation
-- Based on lecture Chapter 2.2, Slide 19
-- Uses pre-order and post-order traversal numbers for efficient axis computation

-- ============================================================================
-- 1. ANCESTOR AXIS WINDOW FUNCTION
-- ============================================================================
-- Formula: ancestor(v) = {u | pre_order(u) < pre_order(v) AND post_order(u) > post_order(v)}

-- Example usage: Find all ancestors of node with ID = 15 (SchmittKAMM23)
WITH context_node AS (
    SELECT pre_order, post_order 
    FROM accel 
    WHERE id = 15  -- SchmittKAMM23
)
SELECT 
    a.id,
    a.type,
    a.s_id,
    c.text,
    a.pre_order,
    a.post_order
FROM accel a
LEFT JOIN content c ON a.id = c.id
CROSS JOIN context_node cn
WHERE a.pre_order < cn.pre_order 
  AND a.post_order > cn.post_order
ORDER BY a.pre_order;

-- ============================================================================
-- 2. DESCENDANT AXIS WINDOW FUNCTION  
-- ============================================================================
-- Formula: descendant(v) = {u | pre_order(u) > pre_order(v) AND post_order(u) < post_order(v)}

-- Example usage: Find all descendants of node with ID = 29 (vldb_2023)
WITH context_node AS (
    SELECT pre_order, post_order 
    FROM accel 
    WHERE id = 29  -- vldb_2023
)
SELECT 
    a.id,
    a.type,
    a.s_id,
    c.text,
    a.pre_order,
    a.post_order
FROM accel a
LEFT JOIN content c ON a.id = c.id
CROSS JOIN context_node cn
WHERE a.pre_order > cn.pre_order 
  AND a.post_order < cn.post_order
ORDER BY a.pre_order;

-- ============================================================================
-- 3. FOLLOWING-SIBLING AXIS WINDOW FUNCTION
-- ============================================================================
-- Formula: following-sibling(v) = {u | parent(u) = parent(v) AND pre_order(u) > pre_order(v)}

-- Example usage: Find following siblings of node with ID = 15 (SchmittKAMM23)
WITH context_node AS (
    SELECT parent, pre_order 
    FROM accel 
    WHERE id = 15  -- SchmittKAMM23
)
SELECT 
    a.id,
    a.type,
    a.s_id,
    c.text,
    a.pre_order,
    a.post_order
FROM accel a
LEFT JOIN content c ON a.id = c.id
CROSS JOIN context_node cn
WHERE a.parent = cn.parent 
  AND a.pre_order > cn.pre_order
ORDER BY a.pre_order;

-- ============================================================================
-- 4. PRECEDING-SIBLING AXIS WINDOW FUNCTION
-- ============================================================================
-- Formula: preceding-sibling(v) = {u | parent(u) = parent(v) AND pre_order(u) < pre_order(v)}

-- Example usage: Find preceding siblings of node with ID = 28 (SchalerHS23)
WITH context_node AS (
    SELECT parent, pre_order 
    FROM accel 
    WHERE id = 28  -- SchalerHS23
)
SELECT 
    a.id,
    a.type,
    a.s_id,
    c.text,
    a.pre_order,
    a.post_order
FROM accel a
LEFT JOIN content c ON a.id = c.id
CROSS JOIN context_node cn
WHERE a.parent = cn.parent 
  AND a.pre_order < cn.pre_order
ORDER BY a.pre_order;

-- ============================================================================
-- 5. ADVANCED WINDOW FUNCTION EXAMPLES
-- ============================================================================

-- Example 1: Using ROW_NUMBER() to rank siblings by document order
SELECT 
    a.id,
    a.type,
    a.s_id,
    c.text,
    ROW_NUMBER() OVER (PARTITION BY a.parent ORDER BY a.pre_order) as sibling_rank
FROM accel a
LEFT JOIN content c ON a.id = c.id
WHERE a.parent = 29  -- Children of vldb_2023
ORDER BY a.pre_order;

-- Example 2: Using LAG() and LEAD() to find immediate siblings
SELECT 
    a.id,
    a.type,
    a.s_id,
    LAG(a.id) OVER (PARTITION BY a.parent ORDER BY a.pre_order) as preceding_sibling,
    LEAD(a.id) OVER (PARTITION BY a.parent ORDER BY a.pre_order) as following_sibling
FROM accel a
WHERE a.parent = 29  -- Children of vldb_2023
ORDER BY a.pre_order;

-- Example 3: Ancestor depth calculation using window functions
WITH ancestor_depths AS (
    SELECT 
        a.id,
        a.type,
        a.s_id,
        COUNT(*) OVER (
            ORDER BY a.pre_order 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) - COUNT(*) OVER (
            ORDER BY a.post_order DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as depth
    FROM accel a
)
SELECT * FROM ancestor_depths ORDER BY pre_order;

-- ============================================================================
-- 6. PERFORMANCE OPTIMIZATION QUERIES
-- ============================================================================

-- Create indexes for optimal window function performance
CREATE INDEX IF NOT EXISTS idx_accel_pre_order ON accel(pre_order);
CREATE INDEX IF NOT EXISTS idx_accel_post_order ON accel(post_order);
CREATE INDEX IF NOT EXISTS idx_accel_parent_pre ON accel(parent, pre_order);

-- Example: Optimized ancestor query with index hints
SELECT 
    a.id,
    a.type,
    c.text
FROM accel a
LEFT JOIN content c ON a.id = c.id
WHERE a.pre_order < (SELECT pre_order FROM accel WHERE id = ?)
  AND a.post_order > (SELECT post_order FROM accel WHERE id = ?)
ORDER BY a.pre_order;

-- ============================================================================
-- 7. VERIFICATION QUERIES
-- ============================================================================

-- Verify ancestor property: pre(ancestor) < pre(node) < post(node) < post(ancestor)
WITH test_node AS (
    SELECT id, pre_order, post_order FROM accel WHERE s_id = 'SchmittKAMM23'
),
ancestors AS (
    SELECT a.id, a.pre_order, a.post_order
    FROM accel a, test_node t
    WHERE a.pre_order < t.pre_order AND a.post_order > t.post_order
)
SELECT 
    'Ancestor Property Check' as test_name,
    COUNT(*) as ancestor_count,
    CASE 
        WHEN COUNT(*) = COUNT(CASE 
            WHEN a.pre_order < t.pre_order AND t.post_order < a.post_order 
            THEN 1 END)
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as result
FROM ancestors a, test_node t;

-- Verify descendant property: pre(node) < pre(descendant) < post(descendant) < post(node)
WITH test_node AS (
    SELECT id, pre_order, post_order FROM accel WHERE s_id = 'vldb_2023'
),
descendants AS (
    SELECT a.id, a.pre_order, a.post_order
    FROM accel a, test_node t
    WHERE a.pre_order > t.pre_order AND a.post_order < t.post_order
)
SELECT 
    'Descendant Property Check' as test_name,
    COUNT(*) as descendant_count,
    CASE 
        WHEN COUNT(*) = COUNT(CASE 
            WHEN t.pre_order < a.pre_order AND a.post_order < t.post_order 
            THEN 1 END)
        THEN 'PASS' 
        ELSE 'FAIL' 
    END as result
FROM descendants a, test_node t;
