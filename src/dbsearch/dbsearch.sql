CREATE OR REPLACE
PACKAGE DBSEARCH
AUTHID CURRENT_USER
AS
  /**
    * DBSEARCH v1.0
    * Copyright (c) 2017 Jürgen Maus
    * https://github.com/jumaus/ora-scripts

    * The package provides functionality to search for values in the database.
    * To achieve this in a more dynamic fashion it uses regular expressions.
    */

  TYPE T_FIND_ROW IS RECORD (
    c_owner       VARCHAR2(30),
    c_table_name  VARCHAR2(30),
    c_column_name VARCHAR2(30),
    c_data_type   VARCHAR2(30),
    c_value       VARCHAR2(4000),
    c_rowid       VARCHAR2(100),
    c_sql         VARCHAR2(4000)
  );
  TYPE TAB_FIND_ROW IS TABLE OF T_FIND_ROW;

  /**
    * Enables or disables the printing of the generated sql's to the console.
    *
    * @param p_flag   True (default) prints the sql to the console, false otherwise.
    */
  PROCEDURE enable_sql_output(p_flag IN BOOLEAN DEFAULT TRUE);

  /**
    * Searches for a regular expression pattern in all tables and columns
    * specified by the parameters.
    *
    * By default:
    *   - regexp search mode is case insensitive
    *   - owner pattern is restricted to himself
    *   - table pattern is restricted to alphanumeric characters and underline
    *   - column pattern is restricted to alphanumeric characters and underline
    *   - column type pattern is restricted to DATE, NUMBER, CHAR, CLOB, VARCHAR2
    *     and TIMESTAMP
    *   - max-rows-to-skip is a security mechanism to skip very large tables
    *     which can have a high impact on the database, to disable this specify 0
    *
    * @param p_search_pattern       A regular expression to search for
    * @param p_regex_mode           The matching mode,
    *                               e. g. 'i', 'c', 'n', 'm' and 'x'
    * @param p_owner_pattern        A regexp to match owners
    * @param p_table_pattern        A regexp to match table names
    * @param p_column_pattern       A regexp to match column names
    * @param p_column_type_pattern  A regexp to match column types
    * @param p_max_rows_to_skip
    */
  FUNCTION find(
    p_search_pattern      IN VARCHAR2,
    p_regex_mode          IN VARCHAR2       DEFAULT 'i',
    p_owner_pattern       IN VARCHAR2       DEFAULT '^'||user||'$',
    p_table_pattern       IN VARCHAR2       DEFAULT '^[[:alnum:]_]+$',
    p_column_pattern      IN VARCHAR2       DEFAULT '^[[:alnum:]_]+$',
    p_column_type_pattern IN VARCHAR2       DEFAULT '^(DATE|NUMBER|N?CHAR|N?CLOB|N?VARCHAR2|TIMESTAMP.*)$',
    p_max_rows_to_skip    IN SIMPLE_INTEGER DEFAULT 10000
  ) RETURN TAB_FIND_ROW PIPELINED;

END;
/


CREATE OR REPLACE
PACKAGE BODY DBSEARCH
AS

  sql_output_enabled BOOLEAN := false;

  /* internal used types */
  TYPE tab_tables IS TABLE OF ALL_TABLES%ROWTYPE;
  TYPE tab_columns IS TABLE OF ALL_TAB_COLS%ROWTYPE;

  PROCEDURE enable_sql_output(p_flag IN BOOLEAN DEFAULT TRUE) AS
  BEGIN
    sql_output_enabled := p_flag;
  END;

  PROCEDURE check_and_raise(
    p_condition IN BOOLEAN,
    p_err_msg   IN VARCHAR2
  )
  AS
  BEGIN
    IF p_condition THEN
      raise_application_error(-20000, p_err_msg);
    END IF;
  END;

  FUNCTION load_tables(
    p_owner_pattern       IN VARCHAR2,
    p_table_pattern       IN VARCHAR2,
    p_regexp_mode         IN VARCHAR2
  ) RETURN tab_tables IS
    l_tables tab_tables;
  BEGIN
    SELECT *
    BULK COLLECT INTO l_tables
    FROM ALL_TABLES
    WHERE regexp_like(OWNER, p_owner_pattern, p_regexp_mode)
      AND regexp_like(TABLE_NAME, p_table_pattern, p_regexp_mode)
    ORDER BY TABLE_NAME;

    RETURN l_tables;
  END;

  FUNCTION load_columns(
    p_owner               IN VARCHAR2,
    p_table_name          IN VARCHAR2,
    p_column_pattern      IN VARCHAR2,
    p_column_type_pattern IN VARCHAR2,
    p_regexp_mode         IN VARCHAR2
  ) RETURN tab_columns IS
    l_columns tab_columns;
  BEGIN
    SELECT *
    BULK COLLECT INTO l_columns
    FROM ALL_TAB_COLS
    WHERE OWNER = p_owner
      AND TABLE_NAME = p_table_name
      AND regexp_like(COLUMN_NAME, p_column_pattern, p_regexp_mode)
      AND regexp_like(DATA_TYPE, p_column_type_pattern, p_regexp_mode)
    ORDER BY COLUMN_ID;

    RETURN l_columns;
  END;

  FUNCTION get_column_datatype(
    p_column_rec all_tab_cols%ROWTYPE
  ) RETURN VARCHAR2 AS
    l_datatype VARCHAR2(100);
  BEGIN
    l_datatype := p_column_rec.data_type;

    IF p_column_rec.data_type = 'NUMBER' AND p_column_rec.data_precision IS NOT NULL THEN
      l_datatype := l_datatype
                  || '(' || p_column_rec.data_precision
                  || ',' || p_column_rec.data_scale || ')';
    ELSIF regexp_like(p_column_rec.data_type, 'CHAR') THEN
      l_datatype := l_datatype || '(' || p_column_rec.data_length || ')';
    END IF;

    RETURN l_datatype;
  END;

  FUNCTION build_search_query(
    p_columns               IN tab_columns,
    p_regex_search_pattern  IN VARCHAR2,
    p_regex_mode            IN VARCHAR2
  ) RETURN VARCHAR2 AS
    l_col_pattern VARCHAR2(500):= ' regexp_like(to_char({column}), ''{pattern}'', ''{match}'')';
    l_sql         VARCHAR2(32767);
    l_column      VARCHAR2(32767);
  BEGIN
    l_sql := 'SELECT rowid, q.* FROM '||p_columns(1).owner||'.'||p_columns(1).table_name || ' q WHERE ';
    FOR i IN 1..p_columns.COUNT LOOP
      IF i > 1 THEN l_sql := l_sql || ' OR'; END IF;
      l_column := l_col_pattern;
      l_column := regexp_replace(l_column, '{column}',  p_columns(i).column_name);
      l_column := regexp_replace(l_column, '{pattern}', p_regex_search_pattern);
      l_column := regexp_replace(l_column, '{match}', p_regex_mode);

      l_sql := l_sql || l_column;
    END LOOP;

    IF sql_output_enabled THEN
      DBMS_OUTPUT.PUT_LINE(l_sql);
    END IF;

    RETURN l_sql;
  END;

  PROCEDURE prepare_cursor(
    p_cursor_id     IN            INTEGER,
    p_sql           IN            VARCHAR2,
    p_column_count  IN OUT NOCOPY INTEGER,
    p_desc_tab      IN OUT NOCOPY DBMS_SQL.DESC_TAB
  ) AS
    l_status    INTEGER;
    l_colValue  VARCHAR2(32767);
  BEGIN
    DBMS_SQL.parse(p_cursor_id, p_sql, DBMS_SQL.NATIVE);
    l_status := DBMS_SQL.execute(p_cursor_id);
    -- get column descriptions
    DBMS_SQL.describe_columns(p_cursor_id, p_column_count, p_desc_tab);

    -- register columns with output variable
    FOR i IN 1..p_column_count LOOP
      DBMS_SQL.define_column(p_cursor_id, i, l_colValue, 32767);
    END LOOP;
  END;

  FUNCTION find(
    p_search_pattern      IN VARCHAR2,
    p_regex_mode          IN VARCHAR2       DEFAULT 'i',
    p_owner_pattern       IN VARCHAR2       DEFAULT '^'||user||'$',
    p_table_pattern       IN VARCHAR2       DEFAULT '^[[:alnum:]_]+$',
    p_column_pattern      IN VARCHAR2       DEFAULT '^[[:alnum:]_]+$',
    p_column_type_pattern IN VARCHAR2       DEFAULT '^(DATE|NUMBER|N?CHAR|N?CLOB|N?VARCHAR2|TIMESTAMP.*)$',
    p_max_rows_to_skip    IN SIMPLE_INTEGER DEFAULT 10000
  ) RETURN TAB_FIND_ROW PIPELINED
  AS
    l_tables  tab_tables;
    l_cols    tab_columns;

    cursor_id  INTEGER := DBMS_SQL.OPEN_CURSOR;
    l_sql      VARCHAR2(32767);
    l_colCount INTEGER;
    l_colDesc  DBMS_SQL.DESC_TAB;
    l_colValue VARCHAR2(32767);
    l_status   INTEGER;
    l_rowid    VARCHAR2(100);

    l_row T_FIND_ROW;
  BEGIN
    -- ensure parameters are not null
    check_and_raise((p_search_pattern IS NULL), 'The search pattern cannot be null or empty!');
    check_and_raise((p_regex_mode IS NULL), 'The regex mode cannot be null or empty!');
    check_and_raise((p_owner_pattern IS NULL), 'The owner pattern cannot be null or empty!');
    check_and_raise((p_table_pattern IS NULL), 'The table pattern cannot be null or empty!');
    check_and_raise((p_column_pattern IS NULL), 'The column pattern cannot be null or empty!');
    check_and_raise((p_column_type_pattern IS NULL), 'The pattern for column types cannot be null or empty!');

    l_tables := load_tables(p_owner_pattern, p_table_pattern, p_regex_mode);

    FOR i IN 1..l_tables.COUNT LOOP

      l_cols := load_columns(
        l_tables(i).owner,
        l_tables(i).table_name,
        p_column_pattern,
        p_column_type_pattern,
        p_regex_mode
      );

      IF l_cols.COUNT = 0 THEN
        dbms_output.put_line('Table '||l_tables(i).owner||'.'||l_tables(i).table_name||' has no columns!');
        CONTINUE;
      END IF;

      IF p_max_rows_to_skip > 0 AND l_tables(i).num_rows > p_max_rows_to_skip THEN
        DBMS_OUTPUT.PUT_LINE('Table '
          ||l_tables(i).owner||'.'||l_tables(i).table_name
          ||' consists of '||l_tables(i).num_rows
          ||'. Table will be skipped.'
        );
        CONTINUE;
      END IF;

      l_sql := build_search_query(
          l_cols,
          p_search_pattern,
          p_regex_mode
      );

      prepare_cursor(cursor_id, l_sql, l_colCount, l_colDesc);

      LOOP
        EXIT WHEN (DBMS_SQL.fetch_rows(cursor_id) = 0);

        FOR j IN 1..l_colCount LOOP
          DBMS_SQL.column_value(cursor_id, j, l_colValue);

          -- store the rowid
          IF j = 1 THEN
            l_rowid := l_colValue;
            CONTINUE;
          END IF;

          IF regexp_like(
              l_colValue,
              p_search_pattern,
              p_regex_mode
          ) THEN
            l_row.c_owner       := l_tables(i).owner;
            l_row.c_table_name  := l_tables(i).table_name;
            l_row.c_column_name := l_cols(j -1).column_name;
            l_row.c_data_type   := get_column_datatype(l_cols(j -1));
            l_row.c_value     := l_colValue;
            l_row.c_rowid     := l_rowid;
            l_row.c_sql       := 'SELECT * FROM '
                                ||l_tables(i).owner||'.'
                                ||l_tables(i).table_name
                                ||' WHERE ROWID = '''||l_rowid||'''';

            PIPE ROW (l_row);
          END IF;
        END LOOP;
      END LOOP;
    END LOOP;

    DBMS_SQL.CLOSE_CURSOR(cursor_id);
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_SQL.CLOSE_CURSOR(cursor_id);
      RAISE;
  END;
END;
/
