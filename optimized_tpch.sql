-- 16 original
SELECT
  p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
FROM partsupp, part
WHERE
   p_partkey = ps_partkey
   AND p_brand <> 'Brand#45'
   AND p_type not like 'MEDIUM POLISHED%'
   AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
   AND ps_suppkey not in (
       SELECT s_suppkey
       FROM supplier
       WHERE s_comment like '%Customer%Complaints%'
   )

-- 16 optimized
SELECT
  p_brand, p_type, p_size, count(distinct ps_suppkey) as supplier_cnt
FROM partsupp, part, supplier
WHERE p_partkey = ps_partkey
  AND ps_suppkey = s_suppkey
  AND p_brand <> 'Brand#45'
  AND p_type not like 'MEDIUM POLISHED%'
  AND p_size in (49, 14, 23, 45, 19, 3, 36, 9)
  AND s_comment NOT LIKE '%Customer%Complaints%'
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;




-- 17 original
SELECT
  sum(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem, part
WHERE
   p_partkey = l_partkey
   AND p_brand = 'Brand#23'
   AND p_container = 'MED BOX'
   AND l_quantity < (
       SELECT 0.2 * avg(l_quantity)
       FROM lineitem
       WHERE l_partkey = p_partkey
   );

-- 17 optimized
SELECT
  sum(l_extendedprice) / 7.0 as avg_yearly
FROM lineitem, part, (
  SELECT
  l_partkey as partkey, avg(l_quantity) as avg_quantity
  FROM lineitem
  GROUP BY partkey
) as x
WHERE p_partkey = l_partkey
  AND l_partkey = x.partkey
  AND p_brand = 'Brand#23'
  AND p_container = 'MED BOX'
  AND l_quantity < 0.2 * x.avg_quantity;



-- 18 original
SELECT
  c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE
  o_orderkey in (
      SELECT l_orderkey
      FROM lineitem
      GROUP BY l_orderkey
      having sum(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate;

-- 18 optimized
SELECT
  c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum(l_quantity)
FROM customer, orders, lineitem
WHERE c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
HAVING sum(l_quantity) > 300
ORDER BY o_totalprice DESC, o_orderdate;
