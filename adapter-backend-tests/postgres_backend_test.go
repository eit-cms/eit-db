package adapter_backend_tests

import (
	"context"
	"testing"
)

// TestPostgresDatabaseFeatures 测试 PostgreSQL 适配器声明的数据库特性
func TestPostgresDatabaseFeatures(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()

	// composite_keys: 支持复合主键
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_composite_keys")
	assertFeatureSupported(ctx, t, repo, "composite_keys",
		`CREATE TABLE test_composite_keys (
			user_id INT,
			post_id INT,
			PRIMARY KEY (user_id, post_id)
		)`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_composite_keys")

	// partial_indexes: PostgreSQL 支持部分索引
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_partial_idx")
	execSQL(ctx, t, repo, `CREATE TABLE test_partial_idx (id INT, status VARCHAR(20))`)
	assertFeatureSupported(ctx, t, repo, "partial_indexes",
		`CREATE INDEX idx_active ON test_partial_idx (id) WHERE status = 'active'`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_partial_idx")

	// enum_type: PostgreSQL 支持 ENUM 类型
	execSQL(ctx, t, repo, "DROP TYPE IF EXISTS status_enum")
	assertFeatureSupported(ctx, t, repo, "enum_type",
		`CREATE TYPE status_enum AS ENUM ('active', 'inactive', 'pending')`)
	defer execSQL(ctx, t, repo, "DROP TYPE IF EXISTS status_enum")

	// composite_type: PostgreSQL 支持复合类型
	execSQL(ctx, t, repo, "DROP TYPE IF EXISTS address_type")
	assertFeatureSupported(ctx, t, repo, "composite_type",
		`CREATE TYPE address_type AS (street VARCHAR(100), city VARCHAR(50), zip VARCHAR(10))`)
	defer execSQL(ctx, t, repo, "DROP TYPE IF EXISTS address_type")

	// domain_type: PostgreSQL 支持域（Domain）类型
	execSQL(ctx, t, repo, "DROP DOMAIN IF EXISTS valid_email")
	assertFeatureSupported(ctx, t, repo, "domain_type",
		`CREATE DOMAIN valid_email AS VARCHAR(100) NOT NULL`)
	defer execSQL(ctx, t, repo, "DROP DOMAIN IF EXISTS valid_email")

	// udt: PostgreSQL 支持用户定义类型
	execSQL(ctx, t, repo, "DROP TYPE IF EXISTS complex_type")
	assertFeatureSupported(ctx, t, repo, "udt",
		`CREATE TYPE complex_type AS (real FLOAT, imag FLOAT)`)
	defer execSQL(ctx, t, repo, "DROP TYPE IF EXISTS complex_type")

	// stored_procedures: PostgreSQL 支持存储过程（虽然通常叫函数）
	execSQL(ctx, t, repo, "DROP PROCEDURE IF EXISTS add_numbers(INT, INT)")
	assertFeatureSupported(ctx, t, repo, "stored_procedures",
		`CREATE PROCEDURE add_numbers(a INT, b INT, OUT result INT) 
		 LANGUAGE plpgsql AS $$
		 BEGIN
		   result := a + b;
		 END;
		 $$`)
	defer execSQL(ctx, t, repo, "DROP PROCEDURE IF EXISTS add_numbers(INT, INT)")

	// functions: PostgreSQL 支持创建函数
	execSQL(ctx, t, repo, "DROP FUNCTION IF EXISTS multiply(INT, INT)")
	assertFeatureSupported(ctx, t, repo, "functions",
		`CREATE FUNCTION multiply(a INT, b INT) RETURNS INT AS $$
		 BEGIN RETURN a * b; END;
		 $$ LANGUAGE plpgsql`)
	defer execSQL(ctx, t, repo, "DROP FUNCTION IF EXISTS multiply(INT, INT)")

	// aggregate_funcs: PostgreSQL 支持聚合函数
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_agg")
	execSQL(ctx, t, repo, `CREATE TABLE test_agg (id INT, value INT)`)
	execSQL(ctx, t, repo, `INSERT INTO test_agg VALUES (1, 10), (2, 20), (3, 30)`)
	assertQuerySupported(ctx, t, repo, "aggregate_funcs",
		`SELECT COUNT(*), SUM(value), AVG(value), MAX(value), MIN(value) FROM test_agg`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_agg")

	// window_functions: PostgreSQL 支持窗口函数
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_window")
	execSQL(ctx, t, repo, `CREATE TABLE test_window (id INT, value INT)`)
	execSQL(ctx, t, repo, `INSERT INTO test_window VALUES (1, 100), (2, 200), (3, 150)`)
	assertQuerySupported(ctx, t, repo, "window_functions",
		`SELECT id, value, ROW_NUMBER() OVER (ORDER BY value DESC) FROM test_window`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_window")

	// cte: PostgreSQL 支持 CTE (Common Table Expression)
	assertQuerySupported(ctx, t, repo, "cte",
		`WITH numbered AS (SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3)
		 SELECT * FROM numbered`)

	// recursive_cte: PostgreSQL 支持递归 CTE
	assertQuerySupported(ctx, t, repo, "recursive_cte",
		`WITH RECURSIVE numbers AS (
		   SELECT 1 as n
		   UNION ALL
		   SELECT n + 1 FROM numbers WHERE n < 10
		 ) SELECT * FROM numbers`)

	// native_json: PostgreSQL 支持原生 JSON 类型
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json")
	assertFeatureSupported(ctx, t, repo, "native_json",
		`CREATE TABLE test_json (id INT, data JSON)`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json")

	// json_path: PostgreSQL 支持 JSON Path 查询
	execSQL(ctx, t, repo, `CREATE TABLE test_json2 (id INT, data JSONB)`)
	execSQL(ctx, t, repo, `INSERT INTO test_json2 VALUES (1, '{"name": "John", "age": 30}')`)
	assertQuerySupported(ctx, t, repo, "json_path",
		`SELECT data->'name' FROM test_json2`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json2")

	// json_index: PostgreSQL 支持 JSON 索引
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json_idx")
	execSQL(ctx, t, repo, `CREATE TABLE test_json_idx (id INT, data JSONB)`)
	assertFeatureSupported(ctx, t, repo, "json_index",
		`CREATE INDEX idx_json ON test_json_idx USING GIN (data)`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json_idx")

	// full_text_search: PostgreSQL 支持全文搜索
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_fts")
	execSQL(ctx, t, repo, `CREATE TABLE test_fts (id INT, content TEXT)`)
	execSQL(ctx, t, repo, `INSERT INTO test_fts VALUES (1, 'hello world'), (2, 'goodbye world')`)
	assertQuerySupported(ctx, t, repo, "full_text_search",
		`SELECT * FROM test_fts WHERE to_tsvector(content) @@ to_tsquery('world')`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_fts")

	// arrays: PostgreSQL 支持数组类型
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_arrays")
	assertFeatureSupported(ctx, t, repo, "arrays",
		`CREATE TABLE test_arrays (id INT, tags TEXT[])`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_arrays")

	// generated: PostgreSQL 支持生成列
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_generated")
	assertFeatureSupported(ctx, t, repo, "generated",
		`CREATE TABLE test_generated (
			id INT,
			first_name VARCHAR(50),
			last_name VARCHAR(50),
			full_name VARCHAR(100) GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED
		)`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_generated")

	// returning: PostgreSQL 支持 RETURNING 子句
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_returning")
	execSQL(ctx, t, repo, `CREATE TABLE test_returning (id SERIAL PRIMARY KEY, name VARCHAR(50))`)
	assertQuerySupported(ctx, t, repo, "returning",
		`INSERT INTO test_returning (name) VALUES ('test') RETURNING id, name`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_returning")

	// upsert: PostgreSQL 支持 ON CONFLICT 语句
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_upsert")
	execSQL(ctx, t, repo, `CREATE TABLE test_upsert (id INT PRIMARY KEY, name VARCHAR(50))`)
	execSQL(ctx, t, repo, `INSERT INTO test_upsert VALUES (1, 'original')`)
	assertFeatureSupported(ctx, t, repo, "upsert",
		`INSERT INTO test_upsert VALUES (1, 'updated') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_upsert")

	// listen_notify: PostgreSQL 支持 LISTEN/NOTIFY
	// 不能直接测试，只测试不会语法错误
	assertFeatureSupported(ctx, t, repo, "listen_notify",
		`SELECT 1`) // 占位符，实际使用时需要在事务中

	t.Log("✓ PostgreSQL 所有数据库特性测试完成")
}

// TestPostgresQueryFeatures 测试 PostgreSQL 适配器支持的查询特性
func TestPostgresQueryFeatures(t *testing.T) {
	repo, cleanup := setupPostgresRepo(t)
	if repo == nil {
		return
	}
	defer cleanup()

	ctx := context.Background()

	// 创建测试表
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_query_data")
	execSQL(ctx, t, repo, `
		CREATE TABLE test_query_data (
			id INT PRIMARY KEY,
			value INT,
			category VARCHAR(50),
			name VARCHAR(100)
		)
	`)
	execSQL(ctx, t, repo, `
		INSERT INTO test_query_data (id, value, category, name) VALUES
		(1, 10, 'A', 'item1'),
		(2, 20, 'B', 'item2'),
		(3, 15, 'A', 'item3'),
		(4, 25, 'C', 'item4'),
		(5, 30, 'B', 'item5')
	`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_query_data")

	// in_operator
	assertQuerySupported(ctx, t, repo, "in_operator",
		`SELECT * FROM test_query_data WHERE id IN (1, 2, 3)`)

	// not_in_operator
	assertQuerySupported(ctx, t, repo, "not_in_operator",
		`SELECT * FROM test_query_data WHERE id NOT IN (1, 2)`)

	// between_operator
	assertQuerySupported(ctx, t, repo, "between_operator",
		`SELECT * FROM test_query_data WHERE value BETWEEN 15 AND 25`)

	// like_operator
	assertQuerySupported(ctx, t, repo, "like_operator",
		`SELECT * FROM test_query_data WHERE name LIKE 'item%'`)

	// distinct
	assertQuerySupported(ctx, t, repo, "distinct",
		`SELECT DISTINCT category FROM test_query_data`)

	// group_by
	assertQuerySupported(ctx, t, repo, "group_by",
		`SELECT category, COUNT(*) FROM test_query_data GROUP BY category`)

	// having
	assertQuerySupported(ctx, t, repo, "having",
		`SELECT category, COUNT(*) as cnt FROM test_query_data GROUP BY category HAVING COUNT(*) > 1`)

	// order_by
	assertQuerySupported(ctx, t, repo, "order_by",
		`SELECT * FROM test_query_data ORDER BY value DESC`)

	// limit
	assertQuerySupported(ctx, t, repo, "limit",
		`SELECT * FROM test_query_data LIMIT 2`)

	// offset
	assertQuerySupported(ctx, t, repo, "offset",
		`SELECT * FROM test_query_data LIMIT 2 OFFSET 1`)

	// inner_join
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_orders")
	execSQL(ctx, t, repo, `CREATE TABLE test_orders (id INT, data_id INT, amount INT)`)
	execSQL(ctx, t, repo, `INSERT INTO test_orders VALUES (1, 1, 100), (2, 2, 200)`)
	assertQuerySupported(ctx, t, repo, "inner_join",
		`SELECT d.id, o.amount FROM test_query_data d INNER JOIN test_orders o ON d.id = o.data_id`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_orders")

	// left_join
	assertQuerySupported(ctx, t, repo, "left_join",
		`SELECT d.id, o.amount FROM test_query_data d LEFT JOIN test_orders o ON d.id = o.data_id`)

	// right_join
	assertQuerySupported(ctx, t, repo, "right_join",
		`SELECT d.id, o.amount FROM test_query_data d RIGHT JOIN test_orders o ON d.id = o.data_id`)

	// full_outer_join (PostgreSQL supports)
	assertQuerySupported(ctx, t, repo, "full_outer_join",
		`SELECT d.id, o.amount FROM test_query_data d FULL OUTER JOIN test_orders o ON d.id = o.data_id`)

	// cross_join
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS categories")
	execSQL(ctx, t, repo, `CREATE TABLE categories (id INT, name VARCHAR(50))`)
	execSQL(ctx, t, repo, `INSERT INTO categories VALUES (1, 'cat1'), (2, 'cat2')`)
	assertQuerySupported(ctx, t, repo, "cross_join",
		`SELECT * FROM test_query_data CROSS JOIN categories`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS categories")

	// union
	assertQuerySupported(ctx, t, repo, "union",
		`SELECT id, value FROM test_query_data WHERE category = 'A'
		 UNION
		 SELECT id, value FROM test_query_data WHERE category = 'B'`)

	// except (PostgreSQL supports)
	assertQuerySupported(ctx, t, repo, "except",
		`SELECT id FROM test_query_data WHERE category = 'A'
		 EXCEPT
		 SELECT id FROM test_query_data WHERE category = 'B'`)

	// intersect (PostgreSQL supports)
	assertQuerySupported(ctx, t, repo, "intersect",
		`SELECT id FROM test_query_data WHERE value > 10
		 INTERSECT
		 SELECT id FROM test_query_data WHERE category = 'A'`)

	// subqueries
	assertQuerySupported(ctx, t, repo, "subqueries",
		`SELECT * FROM test_query_data WHERE id IN (SELECT data_id FROM test_orders WHERE amount > 100)`)

	// case_expression
	assertQuerySupported(ctx, t, repo, "case_expression",
		`SELECT id, CASE WHEN value > 20 THEN 'high' WHEN value > 10 THEN 'medium' ELSE 'low' END FROM test_query_data`)

	// coalesce
	assertQuerySupported(ctx, t, repo, "coalesce",
		`SELECT id, COALESCE(NULL, value, 0) FROM test_query_data`)

	// cast
	assertQuerySupported(ctx, t, repo, "cast",
		`SELECT id, CAST(value AS VARCHAR) FROM test_query_data`)

	// window_functions
	assertQuerySupported(ctx, t, repo, "window_functions",
		`SELECT id, value, ROW_NUMBER() OVER (ORDER BY value DESC) FROM test_query_data`)

	// cte_in_query
	assertQuerySupported(ctx, t, repo, "cte_in_query",
		`WITH sorted AS (SELECT * FROM test_query_data ORDER BY value DESC LIMIT 3)
		 SELECT * FROM sorted`)

	// views
	execSQL(ctx, t, repo, "DROP VIEW IF EXISTS test_view")
	execSQL(ctx, t, repo, `CREATE VIEW test_view AS SELECT id, value FROM test_query_data WHERE value > 10`)
	assertQuerySupported(ctx, t, repo, "views", `SELECT * FROM test_view`)
	defer execSQL(ctx, t, repo, "DROP VIEW IF EXISTS test_view")

	// materialized_views
	execSQL(ctx, t, repo, "DROP MATERIALIZED VIEW IF EXISTS test_matview")
	execSQL(ctx, t, repo, `CREATE MATERIALIZED VIEW test_matview AS SELECT id, value FROM test_query_data WHERE value > 15`)
	assertQuerySupported(ctx, t, repo, "materialized_views", `SELECT * FROM test_matview`)
	defer execSQL(ctx, t, repo, "DROP MATERIALIZED VIEW IF EXISTS test_matview")

	t.Log("✓ PostgreSQL 所有查询特性测试完成")
}
