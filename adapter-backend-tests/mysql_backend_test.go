package adapter_backend_tests

import (
	"context"
	"testing"
)

// TestMySQLDatabaseFeatures 测试 MySQL 适配器声明的数据库特性
func TestMySQLDatabaseFeatures(t *testing.T) {
	repo, cleanup := setupMySQLRepo(t)
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
		) ENGINE=InnoDB`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_composite_keys")

	// composite_indexes: 支持复合索引
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_comp_idx")
	execSQL(ctx, t, repo, `CREATE TABLE test_comp_idx (id INT, name VARCHAR(50), email VARCHAR(100)) ENGINE=InnoDB`)
	assertFeatureSupported(ctx, t, repo, "composite_indexes",
		`CREATE INDEX idx_composite ON test_comp_idx (name, email)`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_comp_idx")

	// enum_type: MySQL 支持 ENUM 类型
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_enum")
	assertFeatureSupported(ctx, t, repo, "enum_type",
		`CREATE TABLE test_enum (
			id INT PRIMARY KEY,
			status ENUM('active', 'inactive', 'pending')
		) ENGINE=InnoDB`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_enum")

	// stored_procedures: MySQL 支持存储过程
	execSQL(ctx, t, repo, "DROP PROCEDURE IF EXISTS add_numbers")
	assertFeatureSupported(ctx, t, repo, "stored_procedures",
		`CREATE PROCEDURE add_numbers(IN a INT, IN b INT, OUT result INT)
		 BEGIN
		   SET result = a + b;
		 END`)
	defer execSQL(ctx, t, repo, "DROP PROCEDURE IF EXISTS add_numbers")

	// functions: MySQL 支持函数
	execSQL(ctx, t, repo, "DROP FUNCTION IF EXISTS get_full_name")
	assertFeatureSupported(ctx, t, repo, "functions",
		`CREATE FUNCTION get_full_name(first_name VARCHAR(50), last_name VARCHAR(50)) 
		 RETURNS VARCHAR(100) DETERMINISTIC
		 RETURN CONCAT(first_name, ' ', last_name)`)
	defer execSQL(ctx, t, repo, "DROP FUNCTION IF EXISTS get_full_name")

	// window_functions: MySQL 8.0+ 支持窗口函数
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_window")
	execSQL(ctx, t, repo, `CREATE TABLE test_window (id INT, value INT) ENGINE=InnoDB`)
	execSQL(ctx, t, repo, `INSERT INTO test_window VALUES (1, 100), (2, 200), (3, 150)`)
	assertQuerySupported(ctx, t, repo, "window_functions",
		`SELECT id, value, ROW_NUMBER() OVER (ORDER BY value DESC) FROM test_window`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_window")

	// cte: MySQL 8.0+ 支持 CTE
	assertQuerySupported(ctx, t, repo, "cte",
		`WITH numbered AS (SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3)
		 SELECT * FROM numbered`)

	// recursive_cte: MySQL 8.0+ 支持递归 CTE
	assertQuerySupported(ctx, t, repo, "recursive_cte",
		`WITH RECURSIVE numbers AS (
		   SELECT 1 as n
		   UNION ALL
		   SELECT n + 1 FROM numbers WHERE n < 10
		 ) SELECT * FROM numbers`)

	// native_json: MySQL 5.7+ 原生 JSON 支持
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json")
	assertFeatureSupported(ctx, t, repo, "native_json",
		`CREATE TABLE test_json (id INT, data JSON) ENGINE=InnoDB`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json")

	// json_path: MySQL JSON 路径查询
	execSQL(ctx, t, repo, `CREATE TABLE test_json2 (id INT, data JSON) ENGINE=InnoDB`)
	execSQL(ctx, t, repo, `INSERT INTO test_json2 VALUES (1, '{"name": "John", "age": 30}')`)
	assertQuerySupported(ctx, t, repo, "json_path",
		`SELECT JSON_EXTRACT(data, '$.name') FROM test_json2`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json2")

	// json_index: MySQL 8.0+ JSON 索引
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json_idx")
	execSQL(ctx, t, repo, `CREATE TABLE test_json_idx (id INT, data JSON, name VARCHAR(50) GENERATED ALWAYS AS (JSON_EXTRACT(data, '$.name')) STORED) ENGINE=InnoDB`)
	assertFeatureSupported(ctx, t, repo, "json_index",
		`CREATE INDEX idx_json_name ON test_json_idx (name)`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_json_idx")

	// full_text_search: MySQL 支持全文搜索
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_fts")
	execSQL(ctx, t, repo, `CREATE TABLE test_fts (id INT PRIMARY KEY, content TEXT, FULLTEXT INDEX ft_content(content)) ENGINE=InnoDB`)
	execSQL(ctx, t, repo, `INSERT INTO test_fts VALUES (1, 'hello world'), (2, 'mysql database')`)
	assertQuerySupported(ctx, t, repo, "full_text_search",
		`SELECT * FROM test_fts WHERE MATCH(content) AGAINST('world' IN BOOLEAN MODE)`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_fts")

	// generated: MySQL 5.7+ 支持生成列
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_generated")
	assertFeatureSupported(ctx, t, repo, "generated",
		`CREATE TABLE test_generated (
			id INT,
			first_name VARCHAR(50),
			last_name VARCHAR(50),
			full_name VARCHAR(100) GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)) STORED
		) ENGINE=InnoDB`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_generated")

	// upsert: MySQL 支持 ON DUPLICATE KEY UPDATE
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_upsert")
	execSQL(ctx, t, repo, `CREATE TABLE test_upsert (id INT PRIMARY KEY, name VARCHAR(50)) ENGINE=InnoDB`)
	execSQL(ctx, t, repo, `INSERT INTO test_upsert VALUES (1, 'original')`)
	assertFeatureSupported(ctx, t, repo, "upsert",
		`INSERT INTO test_upsert VALUES (1, 'updated') ON DUPLICATE KEY UPDATE name = VALUES(name)`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS test_upsert")

	t.Log("✓ MySQL 所有数据库特性测试完成")
}

// TestMySQLQueryFeatures 测试 MySQL 适配器支持的查询特性
func TestMySQLQueryFeatures(t *testing.T) {
	repo, cleanup := setupMySQLRepo(t)
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
		) ENGINE=InnoDB
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
	execSQL(ctx, t, repo, `CREATE TABLE test_orders (id INT, data_id INT, amount INT) ENGINE=InnoDB`)
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

	// Note: MySQL does NOT support FULL OUTER JOIN (handled in queryfeatures.go)
	// cross_join
	execSQL(ctx, t, repo, "DROP TABLE IF EXISTS categories")
	execSQL(ctx, t, repo, `CREATE TABLE categories (id INT, name VARCHAR(50)) ENGINE=InnoDB`)
	execSQL(ctx, t, repo, `INSERT INTO categories VALUES (1, 'cat1'), (2, 'cat2')`)
	assertQuerySupported(ctx, t, repo, "cross_join",
		`SELECT * FROM test_query_data CROSS JOIN categories`)
	defer execSQL(ctx, t, repo, "DROP TABLE IF EXISTS categories")

	// union
	assertQuerySupported(ctx, t, repo, "union",
		`SELECT id, value FROM test_query_data WHERE category = 'A'
		 UNION
		 SELECT id, value FROM test_query_data WHERE category = 'B'`)

	// Note: MySQL does NOT support EXCEPT and INTERSECT (handled in queryfeatures.go)
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
		`SELECT id, CAST(value AS CHAR) FROM test_query_data`)

	// window_functions (MySQL 8.0+)
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

	t.Log("✓ MySQL 所有查询特性测试完成")
}
