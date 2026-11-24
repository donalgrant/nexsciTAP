import os
import sqlite3
import tempfile
import unittest

import pytest

from TAP.tablevalidator import TableValidationError, TableValidator


def _make_db(table_names, schema_name='TAP_SCHEMA', tables_table='tables'):
    """Create SQLite DBs with TAP_SCHEMA attached, mimicking real setup.

    Returns (conn, tap_schema_path) — caller is responsible for cleanup.
    """
    fd, tap_schema_path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    schema_conn = sqlite3.connect(tap_schema_path)
    schema_conn.execute('CREATE TABLE ' + tables_table + ' (table_name TEXT)')
    for name in table_names:
        schema_conn.execute(
            'INSERT INTO ' + tables_table + ' VALUES (?)', (name,))
    schema_conn.commit()
    schema_conn.close()

    conn = sqlite3.connect(':memory:')
    conn.execute(
        'ATTACH DATABASE ? AS ' + schema_name, (tap_schema_path,))

    return conn, tap_schema_path


class TestTableValidator(unittest.TestCase):

    def setUp(self):
        self.conn, self._tap_schema_path = _make_db([
            'ps',
            'pscomppars',
            'stellarhosts',
            'TAP_SCHEMA.tables',
            'TAP_SCHEMA.columns',
            'TAP_SCHEMA.schemas',
            'cumulative',
        ])
        self.connectInfo = {
            'tap_schema': 'TAP_SCHEMA',
            'tables_table': 'tables',
        }

    def tearDown(self):
        self.conn.close()
        if os.path.exists(self._tap_schema_path):
            os.unlink(self._tap_schema_path)

    def test_exact_match(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['ps'])  # should not raise

    def test_default_connectInfo(self):
        """Works with no connectInfo (uses TAP_SCHEMA.tables default)."""
        v = TableValidator(self.conn)
        v.validate(['ps'])

    def test_case_insensitive(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['PS'])
        v.validate(['Ps'])
        v.validate(['TAP_SCHEMA.Tables'])

    def test_schema_prefix_in_whitelist_bare_in_query(self):
        """TAP_SCHEMA.columns is whitelisted; query says just 'columns'."""
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['columns'])

    def test_bare_in_whitelist_unknown_schema_in_query(self):
        """'ps' is whitelisted bare; 'public.ps' has unknown schema — rejected."""
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        # Assert TableValidationError specifically so a revert to generic Exception
        # would be caught — tap.py relies on this type for 403 vs 400 distinction.
        with self.assertRaises(TableValidationError):
            v.validate(['public.ps'])

    def test_known_schema_bare_table_in_query(self):
        """'ps' is whitelisted bare; 'tap_schema.ps' uses known schema — allowed."""
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['tap_schema.ps'])

    def test_disallowed_table_raises(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        # Assert TableValidationError specifically so a revert to generic Exception
        # would be caught — tap.py relies on this type for 403 vs 400 distinction.
        with self.assertRaises(TableValidationError) as ctx:
            v.validate(['pg_catalog.pg_tables'])
        self.assertIn('not available', str(ctx.exception))

    def test_multi_table_all_valid(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        v.validate(['ps', 'pscomppars', 'stellarhosts'])

    def test_multi_table_one_invalid(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        # Assert TableValidationError specifically so a revert to generic Exception
        # would be caught — tap.py relies on this type for 403 vs 400 distinction.
        with self.assertRaises(TableValidationError) as ctx:
            v.validate(['ps', 'information_schema.tables', 'stellarhosts'])
        self.assertIn('information_schema.tables', str(ctx.exception))

    def test_empty_table_list_raises(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        # Empty list is a protocol-level error, not a table access rejection —
        # generic Exception is correct here (not TableValidationError).
        with self.assertRaises(Exception):
            v.validate([])

    def test_system_catalog_blocked(self):
        v = TableValidator(self.conn, connectInfo=self.connectInfo)
        for bad_table in [
            'ALL_TABLES',
            'DBA_USERS',
            'V$SESSION',
            'information_schema.tables',
            'pg_catalog.pg_class',
            'EXOFOP.FILES',
        ]:
            # Assert TableValidationError specifically so a revert to generic Exception
            # would be caught — tap.py relies on this type for 403 vs 400 distinction.
            with self.assertRaises(TableValidationError, msg=f'{bad_table} should be blocked'):
                v.validate([bad_table])


class TestValidateStatement(unittest.TestCase):
    """Tests for the pre-connection validate_statement() static method."""

    def test_clean_select_passes(self):
        TableValidator.validate_statement("SELECT ra, dec FROM ps WHERE ra > 10")

    def test_select_with_functions_passes(self):
        TableValidator.validate_statement(
            "SELECT TOP 10 ra, dec, count(*) FROM ps GROUP BY ra, dec")

    def test_semicolon_rejected(self):
        with self.assertRaises(Exception) as ctx:
            TableValidator.validate_statement("SELECT 1; DROP TABLE ps")
        self.assertIn('semicolon', str(ctx.exception).lower())

    def test_insert_rejected(self):
        with self.assertRaises(Exception) as ctx:
            TableValidator.validate_statement("INSERT INTO ps VALUES (1, 2)")
        self.assertIn('INSERT', str(ctx.exception))

    def test_drop_rejected(self):
        with self.assertRaises(Exception) as ctx:
            TableValidator.validate_statement("DROP TABLE ps")
        self.assertIn('DROP', str(ctx.exception))

    def test_delete_rejected(self):
        with self.assertRaises(Exception):
            TableValidator.validate_statement("DELETE FROM ps WHERE ra = 0")

    def test_update_rejected(self):
        with self.assertRaises(Exception):
            TableValidator.validate_statement("UPDATE ps SET ra = 0")

    def test_dangerous_function_utl_http(self):
        with self.assertRaises(Exception) as ctx:
            TableValidator.validate_statement(
                "SELECT UTL_HTTP.request('http://evil.com') FROM dual")
        self.assertIn('UTL_HTTP', str(ctx.exception))

    def test_dangerous_function_dbms_sql(self):
        with self.assertRaises(Exception):
            TableValidator.validate_statement(
                "SELECT DBMS_SQL.execute(1) FROM dual")

    def test_dangerous_function_sys_context(self):
        with self.assertRaises(Exception):
            TableValidator.validate_statement(
                "SELECT SYS_CONTEXT('USERENV', 'SESSION_USER') FROM dual")

    def test_system_catalog_all_users(self):
        """System catalog references should raise TableValidationError (403)."""
        with self.assertRaises(TableValidationError):
            TableValidator.validate_statement("SELECT * FROM ALL_USERS")

    def test_system_catalog_dba_tables(self):
        with self.assertRaises(TableValidationError):
            TableValidator.validate_statement(
                "SELECT * FROM ps UNION ALL SELECT table_name, 1 FROM DBA_TABLES")

    def test_system_catalog_v_session(self):
        with self.assertRaises(TableValidationError):
            TableValidator.validate_statement("SELECT * FROM V$SESSION")

    def test_word_boundary_no_false_positive(self):
        """Keywords inside longer words should not trigger rejection."""
        TableValidator.validate_statement(
            "SELECT updated, created, dataset FROM ps")

    def test_case_insensitive_rejection(self):
        with self.assertRaises(Exception):
            TableValidator.validate_statement("select * from ps; drop table ps")
        with self.assertRaises(Exception):
            TableValidator.validate_statement("Insert Into ps values (1)")


class TestTableNames(unittest.TestCase):
    """Tests for TableNames.extract_tables() parsing changes."""

    def setUp(self):
        from TAP.tablenames import TableNames
        self.tn = TableNames()

    def test_simple_select(self):
        tables = self.tn.extract_tables("SELECT ra, dec FROM ps")
        self.assertEqual(tables, ['ps'])

    def test_multi_table_join(self):
        tables = self.tn.extract_tables(
            "SELECT a.ra FROM ps a, stellarhosts b WHERE a.id = b.id")
        self.assertIn('ps', tables)
        self.assertIn('stellarhosts', tables)

    def test_order_by_not_extracted(self):
        """ORDER BY columns should not appear as table names (StopIteration fix)."""
        tables = self.tn.extract_tables(
            "SELECT ra, dec FROM ps ORDER BY ra DESC")
        self.assertEqual(tables, ['ps'])
        self.assertNotIn('ra', tables)

    def test_group_by_not_extracted(self):
        tables = self.tn.extract_tables(
            "SELECT dec, count(*) FROM ps GROUP BY dec")
        self.assertEqual(tables, ['ps'])
        self.assertNotIn('dec', tables)

    def test_oracle_rownum_not_skipped(self):
        """Oracle-dialect queries (ROWNUM) should still extract tables."""
        tables = self.tn.extract_tables(
            "SELECT * FROM ps WHERE ROWNUM <= 10")
        self.assertIn('ps', tables)

    def test_dml_skipped(self):
        """DML statements should be skipped, returning no tables."""
        tables = self.tn.extract_tables("INSERT INTO ps VALUES (1, 2)")
        self.assertEqual(tables, [])

    def test_ddl_skipped(self):
        tables = self.tn.extract_tables("DROP TABLE ps")
        self.assertEqual(tables, [])

    def test_schema_qualified_table(self):
        tables = self.tn.extract_tables(
            "SELECT * FROM tap_schema.columns")
        self.assertIn('tap_schema.columns', tables)

    def test_case_insensitive_extraction(self):
        tables = self.tn.extract_tables("SELECT * FROM PS")
        self.assertEqual(tables, ['ps'])


if __name__ == '__main__':
    unittest.main()


# ---- a function where a table belongs (security review, 23 November 2026) ----

@pytest.mark.parametrize('query', [
    "select * from read_text('/etc/passwd')",
    "select * from fp_xsc cross join duckdb_settings()",
    "select * from fp_xsc join duckdb_settings() on true",
    "select (select value from duckdb_settings() where name='x') as v from fp_xsc",
    "select * from fp_xsc where exists (select 1 from duckdb_settings())",
    "select 1 from tap_schema.tables, (from duckdb_settings())",
    "select 1 from tap_schema.tables, duckdb_settings()",
    "select 1 from a, b, glob('s3://bucket/catalogs/*')",
    "select (select count(*) from query('select 1')) as v from fp_xsc",
    "select * from/**/read_text('x')",
    "select * from\nread_blob ('x')",
    'select * from "read_text"(\'x\')',
    "select * from main.duckdb_secrets()",
    "select * from (read_text('x'))",
    "select * from fp_xsc, lateral range(10)",
    "SELECT TOP 1 (SELECT content FROM read_text('s3://b/catalogs/_banner.json')) AS v FROM fp_xsc",
])
def test_a_function_cannot_stand_for_a_table(query):
    with pytest.raises(Exception, match='where a table belongs'):
        TableValidator.validate_statement(query)


@pytest.mark.parametrize('query', [
    "select ra, dec from fp_xsc where contains(point('icrs', ra, dec), circle('icrs', 10, 20, 0.1)) = 1",
    "select a.ra from fp_xsc a join allwise b on 1 = contains(point('icrs', a.ra, a.dec), circle('icrs', b.ra, b.dec, 0.001))",
    "select count(*), avg(w1mpro) from allwise group by round(ra, 0) order by count(*), max(dec)",
    "select ra from fp_xsc, allwise where sin(fp_xsc.ra) > 0 order by ra, abs(dec)",
    "select ra from fp_xsc where designation in ('from read_text(x)', 'a, f(b)')",
    "select ra from (select ra, dec from fp_xsc where ra < 1) t where t.dec > 0",
    "select t.ra from tap_upload.mine t, fp_xsc x where t.ra = x.ra",
    "select ra from fp_xsc where ra in (select max(ra) from allwise)",
    "select top 5 ra -- from f(x)\nfrom fp_xsc",
])
def test_ordinary_queries_are_not_taken_for_one(query):
    TableValidator.validate_statement(query)
