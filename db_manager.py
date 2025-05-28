import mysql.connector
import psycopg2
import pyodbc  # Added for SQL Server
import cx_Oracle # Added for Oracle
import logging
from typing import List, Dict, Any, Optional
import re

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
                        result_dict = None
                        # For psycopg2, cursor.fetchone() returns a tuple. Convert to dict.
                        if driver == 'pgsql' and isinstance(row, tuple):
                            result_dict = {desc[0]: value for desc, value in zip(self.cursor.description, row)}
                        # For pyodbc (mssql), rows are pyodbc.Row objects or tuples, convert to dict
                        elif driver == 'mssql':
                            result_dict = {col_name: getattr(row, col_name) for col_name in column_names}
                        # For cx_Oracle, rows are tuples by default, convert to dict
                        elif driver == 'oracle' and isinstance(row, tuple):
                             result_dict = {col_name: value for col_name, value in zip(column_names, row)}
                        elif driver == 'mysql': # MySQL dictionary cursor already returns dict
                            result_dict = row 
                        
                        if result_dict is not None:
                            return self._sanitize_dict_values_for_json(result_dict)
                        # This fallback should ideally not be reached if all drivers are handled above
                        # and result_dict is always populated when row is not None.
                        # If row itself could be non-dict and non-tuple (e.g. primitive directly from a specific driver),
                        # it might pass through here. However, current logic aims for dicts.
                        return row 
                    return None
                else:
                    rows = self.cursor.fetchall()
                    if rows:
                        driver = self.db_config.get('driver')
                        processed_rows_list_of_dicts = []
                        if driver == 'pgsql' and rows and isinstance(rows[0], tuple):
                            processed_rows_list_of_dicts = [{desc[0]: value for desc, value in zip(self.cursor.description, r)} for r in rows]
                        # For pyodbc (mssql), convert list of pyodbc.Row or tuples to list of dicts
                        elif driver == 'mssql' and rows: # Assuming rows is a list of pyodbc.Row objects
                            processed_rows_list_of_dicts = [{col_name: getattr(row_obj, col_name) for col_name in column_names} for row_obj in rows]
                        # For cx_Oracle, convert list of tuples to list of dicts
                        elif driver == 'oracle' and rows and isinstance(rows[0], tuple):
                            processed_rows_list_of_dicts = [{col_name: value for col_name, value in zip(column_names, row_tuple)} for row_tuple in rows]
                        elif driver == 'mysql': # MySQL dictionary cursor already returns list of dicts
                            processed_rows_list_of_dicts = rows
                        else: # Fallback if rows is not empty but not handled above (e.g. unknown driver with unexpected row format)
                            # This path implies 'rows' might not be a list of dicts.
                            # If it's a list of non-dicts, _sanitize_dict_values_for_json would fail.
                            # For safety, only process if it's confirmed list of dicts (as per MySQL path)
                            # Other paths ensure conversion to list of dicts first.
                            # If code reaches here for an unhandled driver, best to return as is or log.
                            # Given current logic, this 'else' for processed_rows_list_of_dicts should ideally not be hit if rows were fetched.
                            logger.warning(f"Fetched rows for driver {driver} but format not explicitly handled for sanitization; returning as is if not list of dicts.")
                            if all(isinstance(r, dict) for r in rows):
                                processed_rows_list_of_dicts = rows
                            else: # Cannot sanitize if not list of dicts
                                return rows 


                        return [self._sanitize_dict_values_for_json(r) for r in processed_rows_list_of_dicts]
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
            # Ensure full_table_name is alphanumeric with underscores to prevent injection before direct insertion.
            if not re.match(r'^[a-zA-Z0-9_]+$', full_table_name):
                logger.error(f"Invalid table name for schema retrieval: {full_table_name}")
                return None
            query = f"DESCRIBE `{full_table_name}`" # Use backticks for MySQL
            results = self.execute_query(query)
            if results:
                for row in results: # row is a dict from dictionary cursor
                    # Example MySQL row: {'Field': 'nid', 'Type': 'int(10) unsigned', 'Null': 'NO', 'Key': 'PRI', 'Default': None, 'Extra': ''}
                    schema[row['Field']] = row['Type']
                return schema
            return None # If query fails or no results for DESCRIBE
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

    def _get_text_like_column_types(self, driver: str) -> List[str]:
        """Returns a list of common text-like column type names (lowercase) for the given driver."""
        if driver == 'mysql':
            return ['varchar', 'text', 'char', 'longtext', 'mediumtext', 'tinytext', 'enum', 'set']
        elif driver == 'pgsql':
            return ['character varying', 'varchar', 'text', 'char', 'character', 'name']
        elif driver == 'mssql':
            return ['varchar', 'nvarchar', 'text', 'ntext', 'char', 'nchar']
        elif driver == 'oracle':
            return ['varchar2', 'nvarchar2', 'clob', 'nclob', 'char', 'nchar', 'long']
        return []

    def _quote_identifier(self, identifier: str) -> str:
        """Quotes an SQL identifier based on the database driver."""
        driver = self.db_config.get('driver')
        if driver == 'mysql':
            return f"`{identifier}`"
        elif driver == 'pgsql' or driver == 'oracle': # Oracle also uses double quotes, but case sensitivity is a factor.
            return f'"{identifier}"' # Standard SQL, PostgreSQL is case-sensitive with quotes. Oracle typically folds to uppercase if not quoted.
        elif driver == 'mssql':
            return f"[{identifier}]"
        return identifier # Default to no quoting if driver unknown or doesn't need it for simple identifiers

    def prepare_query(self, query: str) -> str:
        """Replaces {{table_name}} with the prefixed table name."""
        prefix = self.db_config.get('prefix', '')
        # Regex to find {{table_name}} occurrences
        return query.replace('{', '{{').replace('}', '}}').replace('{{{{', '{{').replace('}}}}', '}}').format(**{table_name: prefix + table_name for table_name in self._extract_table_names(query)})

    def _extract_table_names(self, query: str) -> List[str]:
        """Helper to extract unique table names from {{table_name}} placeholders in a query."""
        # Matches {{table_name}} allowing for spaces around table_name
        matches = re.findall(r"{\{\s*([a-zA-Z0-9_]+)\s*\}}", query)
        return list(set(matches)) # Return unique table names

    def search_string_in_all_tables(self, search_string: str, row_limit_per_column: int = 5) -> List[Dict[str, Any]]:
        """
        Searches for a string in all text-like columns of all tables.

        Args:
            search_string: The string to search for.
            row_limit_per_column: Max number of rows to return per column match.

        Returns:
            A list of dictionaries, where each dictionary contains:
            'table_name', 'column_name', and 'matching_rows' (list of dicts).
        """
        all_findings: List[Dict[str, Any]] = []
        table_names = self.get_tables()
        if not table_names:
            logger.info("No tables found to search.")
            return all_findings

        driver = self.db_config.get('driver')
        text_like_types = self._get_text_like_column_types(driver)
        search_pattern = f"%{search_string}%"

        for table_name in table_names:
            # Skip system tables or tables with very unusual names for safety if needed,
            # but get_tables() should provide application-level tables.
            # The table_name from get_tables() is unprefixed.
            prefixed_table_name = f"{self.db_config.get('prefix', '')}{table_name}"
            
            logger.info(f"Searching in table: {prefixed_table_name}")
            schema = self.get_table_schema(table_name) # unprefixed name
            if not schema:
                logger.warning(f"Could not get schema for table: {table_name}. Skipping.")
                continue

            for column_name, column_type in schema.items():
                # Normalize column_type (e.g., 'VARCHAR(255)' -> 'varchar')
                normalized_column_type = column_type.split('(')[0].lower()
                if normalized_column_type in text_like_types:
                    logger.debug(f"Found text-like column: {column_name} ({column_type}) in table {table_name}")
                    
                    # Construct query. Table and column names from schema are considered "safe"
                    # but quoting is good practice for robustness.
                    quoted_table = self._quote_identifier(prefixed_table_name)
                    quoted_column = self._quote_identifier(column_name)
                    
                    # LIMIT clause syntax varies.
                    # MySQL, PostgreSQL: LIMIT N
                    # SQL Server: TOP N (used in SELECT TOP N ... FROM)
                    # Oracle: ROWNUM <= N (used in WHERE clause or subquery)
                    query = ""
                    params: Optional[tuple] = None
                    current_search_pattern: str

                    if driver in ['mysql', 'mssql', 'oracle']:
                        current_search_pattern = f"%{search_string.lower()}%"
                        where_clause = f"LOWER({quoted_column}) LIKE %s"
                        if driver == 'mysql':
                            query = f"SELECT * FROM {quoted_table} WHERE {where_clause} LIMIT %s"
                            params = (current_search_pattern, row_limit_per_column)
                        elif driver == 'mssql':
                            # pyodbc uses ? for placeholders
                            query = f"SELECT TOP ? * FROM {quoted_table} WHERE {where_clause}"
                            params = (row_limit_per_column, current_search_pattern) # Order for TOP then LIKE
                        elif driver == 'oracle':
                            query = f"SELECT * FROM (SELECT * FROM {quoted_table} WHERE {where_clause} ORDER BY 1) WHERE ROWNUM <= %s"
                            params = (current_search_pattern, row_limit_per_column)
                    elif driver == 'pgsql':
                        current_search_pattern = f"%{search_string}%" # ILIKE handles case itself
                        query = f"SELECT * FROM {quoted_table} WHERE {quoted_column} ILIKE %s LIMIT %s"
                        params = (current_search_pattern, row_limit_per_column)
                    else:
                        logger.warning(f"Case-insensitivity and LIMIT logic for driver {driver} not fully implemented in search_string_in_all_tables. Using basic LIKE. Skipping limit for {table_name}.{column_name}")
                        current_search_pattern = f"%{search_string}%"
                        query = f"SELECT * FROM {quoted_table} WHERE {quoted_column} LIKE %s"
                        params = (current_search_pattern,)

                    if not query:
                        continue

                    try:
                        logger.debug(f"Executing search query on {table_name}.{column_name}: {query} with params {params}")
                        matching_rows = self.execute_query(query, params)
                        if matching_rows:
                            logger.info(f"Found '{search_string}' in {table_name}.{column_name}. Rows: {len(matching_rows)}")
                            all_findings.append({
                                "table_name": table_name, # unprefixed
                                "column_name": column_name,
                                "matching_rows": matching_rows
                            })
                    except Exception as e:
                        logger.error(f"Error searching in {table_name}.{column_name}: {e}. Query: {query}")
                        # Continue to other columns/tables
        
        logger.info(f"Global search for '{search_string}' complete. Found {len(all_findings)} instances.")
        return all_findings

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

    def _sanitize_dict_values_for_json(self, row_dict: Dict[str, Any]) -> Dict[str, Any]:
        for key, value in row_dict.items():
            if isinstance(value, bytes):
                try:
                    row_dict[key] = value.decode('utf-8')
                except UnicodeDecodeError:
                    row_dict[key] = "[binary data (not displayable)]"
        return row_dict

    def list_paragraph_types_with_fields(self) -> Optional[Dict[str, Any]]:
        """
        Lists all paragraph types and their defined fields.

        Returns:
            A dictionary where keys are paragraph type machine names, 
            and values are lists of dictionaries, each representing a field 
            (with keys like 'field_name', 'field_type', 'field_label', 'required', etc.).
            Returns None on error.
        """
        paragraph_types_query = self.prepare_query("SELECT id, label FROM {paragraphs_type}")
        logger.info(f"Executing query for paragraph types: {paragraph_types_query}")
        paragraph_types_rows = self.execute_query(paragraph_types_query)
        logger.info(f"Paragraph types query result: {paragraph_types_rows}")

        if paragraph_types_rows is None: # Could be an error or no paragraph types
            logger.info("Could not retrieve paragraph types or no paragraph types found (paragraph_types_rows is None).")
            return {}
        
        if not paragraph_types_rows: # Check for empty list specifically
            logger.info("No paragraph types found (paragraph_types_rows is an empty list).")
            return {}

        result = {}
        for pt_row in paragraph_types_rows:
            paragraph_type_id = pt_row['id']
            paragraph_type_label = pt_row['label']
            logger.info(f"Processing paragraph type: ID = {paragraph_type_id}, Label = {paragraph_type_label}")
            
            # In D8/9+, field config is stored in config system, but also reflected in DB tables
            # like 'config' or specific entity field tables.
            # For a more direct DB approach for fields:
            # field_storage_config: general field properties (type)
            # field_config: instance of field on an entity bundle (label, required, settings)
            
            # This query gets fields attached to a specific paragraph bundle
            # bundle is the paragraph type's machine name (id)
            fields_query_sql = self.prepare_query(f"""
                SELECT 
                    fc.field_name, 
                    fsc.type AS field_type,
                    fc.label AS field_label,  -- This might not be directly in field_config, often in config exports
                                              -- We might need to parse serialized data or simplify
                    fc.required,
                    fc.settings AS field_settings, -- often serialized
                    fc.default_value AS field_default_value -- often serialized
                FROM 
                    {{field_config}} fc
                JOIN 
                    {{field_storage_config}} fsc ON fc.field_name = fsc.field_name AND fc.entity_type = fsc.entity_type
                WHERE 
                    fc.entity_type = 'paragraph' AND fc.bundle = %s
            """)
            
            # The label for a field instance (fc.label) might be complex if it's stored
            # as a TranslatableMarkup object and serialized in the config system.
            # The 'config' table has raw config data but parsing that is very complex.
            # The fc.label in the database *should* be the plain label if available there from older versions or simple cases.
            # We will attempt to retrieve fc.label. If it's not available or needs more complex parsing,
            # we might get NULL or a serialized string.
            # For simplicity, we're taking fc.label as is.

            # fc.label often comes from the 'config' table's data column (serialized).
            # A more robust way would be to use Drupal API if running within Drupal.
            # Here, we rely on what's queryable directly.
            # Let's assume for now `field_config` has a usable `label` column directly for this example.
            # If `fc.label` doesn't exist or isn't what we need, we'd have to look at the serialized `data`
            # attribute of `field_config` or the `config` table, which is much harder.
            # Many Drupal database tools show `label` as part of `field_config` table structure.

            # Let's re-evaluate the label source. Typically, `field_config` does NOT have a direct `label` column.
            # The label is part of the serialized `data` in `field_config` or managed by the config system.
            # The query below will be simplified and we'll note that 'field_label' might be the field_name if a true label isn't easily queryable.

            # Corrected approach: Use field_name as a stand-in if true label is too hard to get.
            # For field_type from field_storage_config.
            # For required from field_config.
            
            # Field settings and default_value are often serialized strings.
            # We will fetch them but won't attempt to deserialize them in this step for simplicity.

            fields_for_bundle_sql = self.prepare_query(f"""
                SELECT 
                    fc.field_name,
                    fsc.type AS field_type,
                    fc.required,
                    fsc.settings AS field_storage_settings, -- Serialized
                    fc.settings AS field_instance_settings, -- Serialized
                    fc.default_value_callback,
                    fc.default_value -- Serialized
                    -- fc.label IS NOT a standard column. Label is typically in serialized config.
                    -- For simplicity, the UI displaying this might use field_name if label is complex.
                FROM 
                    {{field_config}} fc
                INNER JOIN 
                    {{field_storage_config}} fsc ON fc.field_name = fsc.field_name AND fc.entity_type = fsc.entity_type
                WHERE 
                    fc.entity_type = 'paragraph' AND fc.bundle = %s
                ORDER BY fc.field_name;
            """)

            logger.info(f"Executing query for fields of paragraph type '{paragraph_type_id}': {fields_for_bundle_sql}")
            field_details_rows = self.execute_query(fields_for_bundle_sql, params=(paragraph_type_id,))
            logger.info(f"Field details query result for '{paragraph_type_id}': {field_details_rows}")
            
            current_fields = []
            if field_details_rows:
                for fd_row in field_details_rows:
                    # Sanitize serialized fields before adding
                    field_storage_settings_str = fd_row.get('field_storage_settings')
                    if isinstance(field_storage_settings_str, bytes):
                        try:
                            field_storage_settings_str = field_storage_settings_str.decode('utf-8')
                        except UnicodeDecodeError:
                            field_storage_settings_str = "[binary data]"
                    
                    field_instance_settings_str = fd_row.get('field_instance_settings')
                    if isinstance(field_instance_settings_str, bytes):
                        try:
                            field_instance_settings_str = field_instance_settings_str.decode('utf-8')
                        except UnicodeDecodeError:
                            field_instance_settings_str = "[binary data]"

                    default_value_str = fd_row.get('default_value')
                    if isinstance(default_value_str, bytes):
                        try:
                            default_value_str = default_value_str.decode('utf-8')
                        except UnicodeDecodeError:
                            default_value_str = "[binary data]"

                    current_fields.append({
                        "field_name": fd_row.get('field_name'),
                        "field_type": fd_row.get('field_type'),
                        "required": bool(fd_row.get('required')), # Ensure boolean
                        # "field_label": fd_row.get('label', fd_row.get('field_name')), # Using field_name as fallback
                        "field_storage_settings": field_storage_settings_str,
                        "field_instance_settings": field_instance_settings_str,
                        "default_value_callback": fd_row.get('default_value_callback'),
                        "default_value": default_value_str,
                    })
            
            result[paragraph_type_id] = {
                "label": paragraph_type_label,
                "machine_name": paragraph_type_id,
                "fields": current_fields
            }

        return result

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