import mysql.connector
import psycopg2
import pyodbc  # Added for SQL Server
import cx_Oracle # Added for Oracle
import logging
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)

class DBManager:
    def __init__(self, db_config: dict):
        """
        Initializes the DBManager with database configuration.

        Args:
            db_config: A dictionary containing database credentials and details
                       (driver, database, username, password, host, port, prefix).
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self._connect()

    def _connect(self):
        """Establishes a connection to the database."""
        if self.connection:
            self.close() # Close existing connection if any
        
        driver = self.db_config.get('driver')
        db_name = self.db_config.get('database')
        user = self.db_config.get('username')
        password = self.db_config.get('password')
        host = self.db_config.get('host')
        port = self.db_config.get('port')

        if not all([driver, db_name, user, password, host, port]):
            msg = "Database configuration is incomplete. Cannot connect."
            logger.error(msg)
            raise ConnectionError(msg)

        try:
            if driver == 'mysql':
                self.connection = mysql.connector.connect(
                    host=host,
                    user=user,
                    password=password,
                    database=db_name,
                    port=int(port)
                )
                logger.info(f"Successfully connected to MySQL database: {db_name} at {host}:{port}")
            elif driver == 'pgsql':
                self.connection = psycopg2.connect(
                    host=host,
                    user=user,
                    password=password,
                    dbname=db_name,
                    port=int(port)
                )
                logger.info(f"Successfully connected to PostgreSQL database: {db_name} at {host}:{port}")
            elif driver == 'mssql': # Added for SQL Server
                # Connection string might vary based on SQL Server setup (e.g., integrated security)
                # Basic example: conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host},{port};DATABASE={db_name};UID={user};PWD={password}'
                # For simplicity, assuming direct host, port, user, password similar to others for now.
                # Users might need to adjust their settings.php 'driver_options' or this logic if specific DSN is needed.
                self.connection = pyodbc.connect(
                    driver='{ODBC Driver 17 for SQL Server}', # Common driver name
                    server=f'{host},{port}', # Often port is specified with server
                    database=db_name,
                    uid=user,
                    pwd=password
                )
                logger.info(f"Successfully connected to SQL Server database: {db_name} at {host}:{port}")
            elif driver == 'oracle': # Added for Oracle
                # Oracle connection might require a DSN or specific service name/SID format.
                # Basic example: dsn = cx_Oracle.makedsn(host, port, service_name='your_service_name') or sid='your_sid'
                # For now, assuming a simple host/port/dbname setup. db_name might map to service_name or SID.
                # This might need to be user-configurable via settings.php 'driver_options'.
                dsn = cx_Oracle.makedsn(host, int(port), service_name=db_name) # Or sid=db_name
                self.connection = cx_Oracle.connect(
                    user=user,
                    password=password,
                    dsn=dsn
                )
                logger.info(f"Successfully connected to Oracle database: {db_name} (service/sid) at {host}:{port}")
            else:
                msg = f"Unsupported database driver: {driver}"
                logger.error(msg)
                raise ValueError(msg)
            
            if self.connection:
                self.cursor = self.connection.cursor(dictionary=(driver == 'mysql')) # dictionary cursor for mysql
        except mysql.connector.Error as err:
            msg = f"MySQL Error: {err}"
            logger.error(msg)
            self.connection = None
            raise ConnectionError(msg) from err
        except psycopg2.Error as err:
            msg = f"PostgreSQL Error: {err}"
            logger.error(msg)
            self.connection = None
            raise ConnectionError(msg) from err
        except pyodbc.Error as err: # Added for SQL Server
            msg = f"SQL Server (pyodbc) Error: {err}"
            logger.error(msg)
            self.connection = None
            raise ConnectionError(msg) from err
        except cx_Oracle.Error as err: # Added for Oracle
            msg = f"Oracle (cx_Oracle) Error: {err}"
            logger.error(msg)
            self.connection = None
            raise ConnectionError(msg) from err
        except ValueError as ve:
             raise ve # Reraise unsupported driver error
        except Exception as e:
            msg = f"An unexpected error occurred during database connection: {e}"
            logger.error(msg)
            self.connection = None
            raise ConnectionError(msg) from e

    def close(self):
        """Closes the database connection."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed.")

    def execute_query(self, query: str, params: Optional[tuple] = None, fetch_one: bool = False) -> Optional[List[Dict[str, Any]] | Dict[str, Any]]:
        """
        Executes a SQL query and fetches results.
        Ensures connection is active before executing.

        Args:
            query: The SQL query string to execute.
            params: A tuple of parameters to use with the query (for preventing SQL injection).
            fetch_one: If True, fetches only one row. Otherwise, fetches all.

        Returns:
            A list of dictionaries (rows) or a single dictionary if fetch_one is True,
            or None if the query fails or no results are found.
            For PostgreSQL, column names are used as keys.
            For MySQL, column names are used as keys (due to dictionary=True cursor).
            For SQL Server (pyodbc), rows are tuple-like, will need conversion.
            For Oracle (cx_Oracle), rows can be accessed by index or by column name (if cursor configured).
        """
        if not self.connection or not self.cursor:
            logger.warning("No active database connection. Attempting to reconnect...")
            try:
                self._connect()
            except ConnectionError as e:
                logger.error(f"Reconnection failed: {e}")
                return None
        
        if not self.cursor:
             logger.error("Cursor is not available even after attempting reconnection.")
             return None

        try:
            self.cursor.execute(query, params or ())
            if self.cursor.description: # Check if the query produces results (e.g., SELECT)
                # Column names for pyodbc and potentially cx_Oracle if not fetching dicts
                column_names = [desc[0] for desc in self.cursor.description]

                if fetch_one:
                    row = self.cursor.fetchone()
                    if row:
                        driver = self.db_config.get('driver')
                        # For psycopg2, cursor.fetchone() returns a tuple. Convert to dict.
                        if driver == 'pgsql' and isinstance(row, tuple):
                            return {desc[0]: value for desc, value in zip(self.cursor.description, row)}
                        # For pyodbc (mssql), rows are pyodbc.Row objects or tuples, convert to dict
                        elif driver == 'mssql':
                            return {col_name: getattr(row, col_name) for col_name in column_names}
                        # For cx_Oracle, rows are tuples by default, convert to dict
                        elif driver == 'oracle' and isinstance(row, tuple):
                             return {col_name: value for col_name, value in zip(column_names, row)}
                        return row # MySQL dictionary cursor already returns dict
                    return None
                else:
                    rows = self.cursor.fetchall()
                    if rows:
                        driver = self.db_config.get('driver')
                        if driver == 'pgsql' and rows and isinstance(rows[0], tuple):
                            return [{desc[0]: value for desc, value in zip(self.cursor.description, row)} for row in rows]
                        # For pyodbc (mssql), convert list of pyodbc.Row or tuples to list of dicts
                        elif driver == 'mssql':
                            return [{col_name: getattr(row_obj, col_name) for col_name in column_names} for row_obj in rows]
                        # For cx_Oracle, convert list of tuples to list of dicts
                        elif driver == 'oracle' and rows and isinstance(rows[0], tuple):
                            return [{col_name: value for col_name, value in zip(column_names, row_tuple)} for row_tuple in rows]
                        return rows # MySQL dictionary cursor already returns list of dicts
                    return [] # Return empty list if no rows found
            else: # For queries that don't return rows (INSERT, UPDATE, DELETE)
                self.connection.commit() # Ensure changes are committed
                return None # Or perhaps a success indicator like rowcount, but MCP tools might expect data or null
        except mysql.connector.Error as err:
            logger.error(f"MySQL Query Error: {err} for query: {query[:200]}... with params: {params}")
            # Consider rolling back if it's a transactional error
            # self.connection.rollback()
            return None
        except psycopg2.Error as err:
            logger.error(f"PostgreSQL Query Error: {err} for query: {query[:200]}... with params: {params}")
            # self.connection.rollback()
            return None
        except pyodbc.Error as err: # Added for SQL Server
            logger.error(f"SQL Server Query Error: {err} for query: {query[:200]}... with params: {params}")
            return None
        except cx_Oracle.Error as err: # Added for Oracle
            logger.error(f"Oracle Query Error: {err} for query: {query[:200]}... with params: {params}")
            return None
        except Exception as e:
            logger.error(f"Generic Query Error: {e} for query: {query[:200]}... with params: {params}")
            return None

    def get_tables(self) -> Optional[List[str]]:
        """Retrieves a list of table names from the database."""
        driver = self.db_config.get('driver')
        prefix = self.db_config.get('prefix', '')
        query = ""

        if driver == 'mysql':
            query = "SHOW TABLES"
        elif driver == 'pgsql':
            # This query lists tables from the public schema. 
            # You might need to adjust if using different schemas.
            query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';"
        elif driver == 'mssql': # Added for SQL Server
            # Placeholder - Query to list tables for SQL Server
            query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = DB_NAME();"
            # query = "SELECT name FROM sys.tables WHERE type = 'U';" # Alternative
        elif driver == 'oracle': # Added for Oracle
            # Placeholder - Query to list tables for Oracle (current user's tables)
            query = "SELECT table_name FROM user_tables;"
            # query = "SELECT object_name FROM user_objects WHERE object_type = 'TABLE';" # Alternative
        else:
            logger.error(f"Unsupported driver for get_tables: {driver}")
            return None

        results = self.execute_query(query)
        if results is not None:
            # Results from execute_query are list of dicts
            # We need to extract the table name which is the first value in each dict for mysql
            # or the value associated with 'tablename' key for pgsql
            if driver == 'mysql':
                 # The key for table name in SHOW TABLES result is e.g., 'Tables_in_your_db_name'
                return [list(row.values())[0] for row in results if isinstance(row, dict) and row]
            elif driver == 'pgsql':
                return [row['tablename'] for row in results if isinstance(row, dict) and 'tablename' in row]
            elif driver == 'mssql': # Added for SQL Server
                return [row['TABLE_NAME'] for row in results if isinstance(row, dict) and 'TABLE_NAME' in row]
            elif driver == 'oracle': # Added for Oracle
                return [row['table_name'].lower() for row in results if isinstance(row, dict) and 'table_name' in row] # Oracle names are often upper, standardize to lower
        return None

    def get_table_schema(self, table_name: str) -> Optional[Dict[str, str]]:
        """Retrieves the schema (column names and types) of a given table."""
        driver = self.db_config.get('driver')
        full_table_name = f"{self.db_config.get('prefix', '')}{table_name}"
        schema = {}

        if driver == 'mysql':
            query = f"DESCRIBE {mysql.connector.utils.escape_table_name(full_table_name)}" 
            # Note: escape_table_name is a conceptual placeholder; actual escaping needs to be handled carefully if using mysql.connector.
            # For DESCRIBE, the table name typically doesn't need complex escaping unless it has very unusual characters.
            # A safer direct approach without a specific escape function from the library for this command:
            # Ensure full_table_name is alphanumeric with underscores to prevent injection before direct insertion.
            if not re.match(r'^[a-zA-Z0-9_]+$', full_table_name):
                logger.error(f"Invalid table name for schema retrieval: {full_table_name}")
                return None
            query = f"DESCRIBE `{full_table_name}`" # Use backticks for MySQL

        elif driver == 'pgsql':
            query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = 'public';
            """
            # For pgsql, execute_query will handle parameters correctly
            results = self.execute_query(query, params=(full_table_name,))
            if results:
                for row in results:
                    schema[row['column_name']] = row['data_type']
                return schema
            return None # If query fails or no results
        elif driver == 'mssql': # Added for SQL Server
            # Placeholder - Query to get table schema for SQL Server
            query = """
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_CATALOG = DB_NAME() AND TABLE_NAME = %s;
            """
            # Note: pyodbc uses '?' as placeholder, not %s. This will need adjustment in execute_query or here.
            # For now, assuming execute_query can adapt or we use direct formatting carefully.
            # Let's use direct formatting for now, assuming table_name is safe (it should be from user input)
            # However, parameterization is always better.
            # query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{full_table_name}' AND TABLE_CATALOG = DB_NAME();"
            # The execute_query method needs to handle '?' for pyodbc if params are used.
            # For now, let's adjust the query here assuming params will be passed as a tuple.
            query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_CATALOG = DB_NAME();"
            results = self.execute_query(query, params=(full_table_name,)) # pyodbc uses ?
            if results:
                for row in results:
                    schema[row['COLUMN_NAME']] = row['DATA_TYPE']
                return schema
            return None
        elif driver == 'oracle': # Added for Oracle
            # Placeholder - Query to get table schema for Oracle
            query = """
                SELECT COLUMN_NAME, DATA_TYPE 
                FROM USER_TAB_COLUMNS 
                WHERE TABLE_NAME = :table_name
            """
            # cx_Oracle uses named placeholders like :name or positional like :1, :2
            results = self.execute_query(query, params={'table_name': full_table_name.upper()}) # Oracle table/column names often uppercase
            if results:
                for row in results:
                     # Assuming the keys are 'COLUMN_NAME' and 'DATA_TYPE' from how execute_query now processes Oracle results
                    schema[row['COLUMN_NAME']] = row['DATA_TYPE']
                return schema
            return None
        else:
            logger.error(f"Unsupported driver for get_table_schema: {driver}")
            return None

    def prepare_query(self, query: str) -> str:
        """Replaces {{table_name}} with the prefixed table name."""
        prefix = self.db_config.get('prefix', '')
        # Regex to find {{table_name}} occurrences
        return query.replace('{', '{{').replace('}', '}}').replace('{{{{', '{{').replace('}}}}', '}}').format(**{table_name: prefix + table_name for table_name in self._extract_table_names(query)})

    def _extract_table_names(self, query: str) -> List[str]:
        """Helper to extract unique table names from {{table_name}} placeholders in a query."""
        import re
        # Matches {{table_name}} allowing for spaces around table_name
        matches = re.findall(r"{\{\s*([a-zA-Z0-9_]+)\s*\}}", query)
        return list(set(matches)) # Return unique table names

    # New Drupal-specific methods for DBManager class:
    def get_node_by_id(self, nid: int) -> Optional[Dict[str, Any]]:
        """Fetches basic data for a specific node by its ID."""
        query = self.prepare_query("""
            SELECT 
                nfd.nid, nfd.vid, nfd.type, nfd.langcode, nfd.status, nfd.uid, 
                nfd.title, nfd.created, nfd.changed,
                ufd.name AS author_name,
                COALESCE(nb.body_value, nrb.body_value) AS body_value,
                COALESCE(nb.body_summary, nrb.body_summary) AS body_summary,
                COALESCE(nb.body_format, nrb.body_format) AS body_format
            FROM 
                {node_field_data} nfd
            LEFT JOIN 
                {users_field_data} ufd ON nfd.uid = ufd.uid
            LEFT JOIN 
                {node__body} nb ON nfd.nid = nb.entity_id AND nfd.vid = nb.revision_id AND nb.deleted = 0 AND nb.langcode = nfd.langcode
            LEFT JOIN
                {node_revision__body} nrb ON nfd.vid = nrb.revision_id AND nrb.deleted = 0 AND nrb.langcode = nfd.langcode
            WHERE 
                nfd.nid = %s
        """)
        return self.execute_query(query, (nid,), fetch_one=True)

    def list_content_types(self) -> Optional[List[Dict[str, Any]]]:
        """Lists all available content types (node types)."""
        query = self.prepare_query("SELECT type, name, description FROM {node_type}")
        return self.execute_query(query)

    def get_taxonomy_term_by_id(self, tid: int) -> Optional[Dict[str, Any]]:
        """Fetches data for a specific taxonomy term by its ID."""
        query = self.prepare_query("""
            SELECT 
                tfd.tid, tfd.vid, tfd.name, tfd.description, tfd.langcode,
                tv.name AS vocabulary_name
            FROM 
                {taxonomy_term_field_data} tfd
            LEFT JOIN
                {taxonomy_vocabulary} tv ON tfd.vid = tv.vid
            WHERE 
                tfd.tid = %s
        """)
        return self.execute_query(query, (tid,), fetch_one=True)

    def list_vocabularies(self) -> Optional[List[Dict[str, Any]]]:
        """Lists all taxonomy vocabularies."""
        query = self.prepare_query("SELECT vid, name, description FROM {taxonomy_vocabulary}")
        return self.execute_query(query)

    def get_user_by_id(self, uid: int) -> Optional[Dict[str, Any]]:
        """Fetches basic data for a specific user by ID."""
        base_query_fields = """
            ufd.uid, ufd.name, ufd.mail, ufd.status, ufd.created, ufd.changed, ufd.langcode
        """
        group_by_fields = """
            ufd.uid, ufd.name, ufd.mail, ufd.status, ufd.created, ufd.changed, ufd.langcode
        """
        driver = self.db_config.get('driver')
        roles_aggregation = ""

        if driver == 'mysql':
            roles_aggregation = "GROUP_CONCAT(DISTINCT ur.roles_target_id) as roles"
        elif driver == 'pgsql':
            roles_aggregation = "STRING_AGG(DISTINCT ur.roles_target_id, ',') as roles"
        elif driver == 'mssql':
            roles_aggregation = "STRING_AGG(ur.roles_target_id, ',') WITHIN GROUP (ORDER BY ur.roles_target_id) as roles"
        elif driver == 'oracle':
            roles_aggregation = "LISTAGG(ur.roles_target_id, ',') WITHIN GROUP (ORDER BY ur.roles_target_id) as roles"
        else: 
            roles_aggregation = "NULL as roles"

        if roles_aggregation == "NULL as roles":
            query = self.prepare_query(f"""
                SELECT 
                    {base_query_fields}
                FROM 
                    {{users_field_data}} ufd
                WHERE 
                    ufd.uid = %s
            """)
        else:
            query = self.prepare_query(f"""
                SELECT 
                    {base_query_fields},
                    {roles_aggregation}
                FROM 
                    {{users_field_data}} ufd
                LEFT JOIN
                    {{user__roles}} ur ON ufd.uid = ur.entity_id
                WHERE 
                    ufd.uid = %s
                GROUP BY
                    {group_by_fields}
            """)
        return self.execute_query(query, (uid,), fetch_one=True)

    def list_paragraphs_by_node_id(self, nid: int, paragraph_field_name: str) -> Optional[List[Dict[str, Any]]]:
        """
        Lists paragraph entities referenced by a specific node via a given paragraph field.
        """
        node_paragraph_field_table = f"node__{paragraph_field_name}"
        query = self.prepare_query(f"""
            SELECT
                p_ref.{paragraph_field_name}_target_id AS paragraph_id,
                p_ref.{paragraph_field_name}_target_revision_id AS paragraph_revision_id,
                pfd.id AS paragraph_item_id, 
                pfd.type AS paragraph_type,
                pfd.langcode AS paragraph_langcode,
                pfd.status AS paragraph_status
            FROM
                {{{node_paragraph_field_table}}} p_ref
            JOIN
                {{paragraphs_item_field_data}} pfd ON p_ref.{paragraph_field_name}_target_id = pfd.id 
                                            AND p_ref.{paragraph_field_name}_target_revision_id = pfd.revision_id
            WHERE
                p_ref.entity_id = %s AND p_ref.deleted = 0
            ORDER BY
                p_ref.delta ASC
        """)
        logger.info(f"Executing paragraph query for node {nid}, field {paragraph_field_name} with prepared query: {{query}}")
        try:
            results = self.execute_query(query, (nid,))
            if results is None:
                logger.warning(f"Paragraph query for node {nid}, field {paragraph_field_name} returned None.")
            return results
        except Exception as e:
            logger.error(f"Error in list_paragraphs_by_node_id for nid {nid}, field {paragraph_field_name}: {e}")
            logger.error(f"Problematic query was: {{query}}")
            return None

# Example Usage (for testing purposes, typically this class would be used by the MCP server)
if __name__ == '__main__':
    import os
    from drupal_settings_parser import parse_settings_php

    logging.basicConfig(level=logging.INFO)

    # Create a dummy settings.php for MySQL for testing
    dummy_settings_mysql_content = """<?php
    $databases['default']['default'] = [
      'database' => 'test_drupal_db',
      'username' => 'root',       // Replace with your MySQL test user
      'password' => 'password',   // Replace with your MySQL test password
      'host' => 'localhost',
      'port' => '3306',
      'driver' => 'mysql',
      'prefix' => 'dr_',
    ];
    """
    mysql_test_file = "dummy_settings_mysql.php"
    with open(mysql_test_file, "w") as f:
        f.write(dummy_settings_mysql_content)

    # Create a dummy settings.php for PostgreSQL for testing
    dummy_settings_pgsql_content = """<?php
    $databases['default']['default'] = [
      'database' => 'test_drupal_db_pg', // Replace with your PostgreSQL test database
      'username' => 'pguser',      // Replace with your PostgreSQL test user
      'password' => 'pgpass',      // Replace with your PostgreSQL test password
      'host' => 'localhost',
      'port' => '5432',
      'driver' => 'pgsql',
      'prefix' => 'pg_',
    ];
    """
    pgsql_test_file = "dummy_settings_pgsql.php"
    with open(pgsql_test_file, "w") as f:
        f.write(dummy_settings_pgsql_content)

    print("--- Testing MySQL Connection (requires a running MySQL server with credentials above) ---")
    mysql_config = parse_settings_php(mysql_test_file)
    db_manager_mysql = None # Initialize to ensure it's in scope for finally
    if mysql_config:
        print(f"MySQL Config: {mysql_config}")
        try:
            db_manager_mysql = DBManager(mysql_config)
            # Create a dummy table for testing
            table_name_mysql = mysql_config.get('prefix', '') + 'test_table'
            db_manager_mysql.execute_query(f"DROP TABLE IF EXISTS `{table_name_mysql}`")
            db_manager_mysql.execute_query(f"CREATE TABLE `{table_name_mysql}` (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), value TEXT)")
            db_manager_mysql.execute_query(f"INSERT INTO `{table_name_mysql}` (name, value) VALUES (%s, %s)", ('example_name', 'example_value'))
            
            tables_mysql = db_manager_mysql.get_tables()
            print(f"MySQL Tables: {tables_mysql}")
            if tables_mysql and table_name_mysql in tables_mysql:
                print(f"Successfully found {table_name_mysql} in MySQL tables.")
                schema_mysql = db_manager_mysql.get_table_schema('test_table') # Use unprefixed name
                print(f"MySQL Schema for {table_name_mysql}: {schema_mysql}")
                assert schema_mysql and 'id' in schema_mysql and 'name' in schema_mysql

            query_result_mysql = db_manager_mysql.execute_query(f"SELECT * FROM `{table_name_mysql}` WHERE name = %s", ('example_name',), fetch_one=True)
            print(f"MySQL Query Result for example_name: {query_result_mysql}")
            assert query_result_mysql and query_result_mysql['value'] == 'example_value'

            # Test Drupal specific methods for MySQL
            print("\n--- Testing Drupal Specific Methods (MySQL) ---")
            # These will likely fail unless you have Drupal tables like 'dr_node_type', etc.
            # For this test script, it's more about ensuring the methods run without Python errors.
            try:
                content_types = db_manager_mysql.list_content_types()
                print(f"MySQL - Content Types: {content_types}")
                node_data = db_manager_mysql.get_node_by_id(1)
                print(f"MySQL - Node ID 1: {node_data}")
                # Add more calls if needed, e.g., for users, taxonomies
            except Exception as specific_e:
                print(f"MySQL - Error testing Drupal specific methods (expected if Drupal tables don't exist): {specific_e}")

        except ConnectionError as e:
            print(f"MySQL connection/test failed: {e}. Please ensure MySQL is running and configured as per dummy_settings_mysql.php")
        except Exception as e:
            print(f"An error occurred during MySQL tests: {e}")
        finally:
            if db_manager_mysql:
                db_manager_mysql.close()
    else:
        print("Failed to parse MySQL dummy settings.")

    # PostgreSQL testing 
    print("\n--- Testing PostgreSQL Connection (requires a running PostgreSQL server and test_drupal_db_pg database/user) ---")
    pgsql_config = parse_settings_php(pgsql_test_file)
    db_manager_pgsql = None # Initialize for finally block
    if pgsql_config:
        print(f"PostgreSQL Config: {pgsql_config}")
        try:
            db_manager_pgsql = DBManager(pgsql_config)
            table_name_pgsql = pgsql_config.get('prefix', '') + 'test_table_pg'
            db_manager_pgsql.execute_query(f'DROP TABLE IF EXISTS "{table_name_pgsql}" cascade') 
            db_manager_pgsql.execute_query(f'CREATE TABLE "{table_name_pgsql}" (id SERIAL PRIMARY KEY, name VARCHAR(255), value TEXT)')
            db_manager_pgsql.execute_query(f'INSERT INTO "{table_name_pgsql}" (name, value) VALUES (%s, %s)', ('pg_example', 'pg_value'))

            tables_pgsql = db_manager_pgsql.get_tables()
            print(f"PostgreSQL Tables: {tables_pgsql}")
            if tables_pgsql and table_name_pgsql in tables_pgsql:
                 print(f"Successfully found {table_name_pgsql} in PostgreSQL tables.")
                 schema_pgsql = db_manager_pgsql.get_table_schema('test_table_pg') 
                 print(f"PostgreSQL Schema for {table_name_pgsql}: {schema_pgsql}")
                 assert schema_pgsql and 'id' in schema_pgsql and 'name' in schema_pgsql

            query_result_pgsql = db_manager_pgsql.execute_query(f'SELECT * FROM "{table_name_pgsql}" WHERE name = %s', ('pg_example',), fetch_one=True)
            print(f"PostgreSQL Query Result for pg_example: {query_result_pgsql}")
            assert query_result_pgsql and query_result_pgsql['value'] == 'pg_value'
            
            # Test Drupal specific methods for PostgreSQL
            print("\n--- Testing Drupal Specific Methods (PostgreSQL) ---")
            try:
                content_types_pg = db_manager_pgsql.list_content_types()
                print(f"PostgreSQL - Content Types: {content_types_pg}")
                node_data_pg = db_manager_pgsql.get_node_by_id(1)
                print(f"PostgreSQL - Node ID 1: {node_data_pg}")
            except Exception as specific_e_pg:
                print(f"PostgreSQL - Error testing Drupal specific methods (expected if Drupal tables don't exist): {specific_e_pg}")

        except ConnectionError as e:
            print(f"PostgreSQL connection/test failed: {e}. Please ensure PostgreSQL is running, database '{pgsql_config.get('database')}' exists, and user is configured as per dummy_settings_pgsql.php")
        except Exception as e:
            print(f"An error occurred during PostgreSQL tests: {e}")
        finally:
            if db_manager_pgsql:
                db_manager_pgsql.close()
    else:
        print("Failed to parse PostgreSQL dummy settings.")

    # Clean up dummy files
    if os.path.exists(mysql_test_file):
        os.remove(mysql_test_file)
    if os.path.exists(pgsql_test_file):
        os.remove(pgsql_test_file)
    print("\nCleaned up dummy settings files.") 