import argparse
import logging
import asyncio
import json
import os
import re
import subprocess
import sys
import inspect
from typing import Optional, List, Dict, Any, Tuple, Sequence, Callable, Coroutine

from mcp.server import FastMCP
from mcp.server.fastmcp import Context
from mcp.types import Tool as MCPToolType, CallToolRequest, CallToolResult, TextContent, EmbeddedResource, ImageContent
from mcp.server.fastmcp.tools.base import Tool as HandlerTool

from drupal_settings_parser import parse_settings_php
from db_manager import DBManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mcp_drupal_server")

# Global variable to hold the DBManager instance
db_manager_instance: Optional[DBManager] = None

# Global variable for the FastMCP server instance, for mcp dev compatibility
# Initialize FastMCP at the global scope
server: FastMCP = FastMCP(name="MCP Drupal Database Server")

class DrupalBaseTool(MCPToolType):
    def __init__(self, name: str, description: str, input_schema: Dict[str, Any], db_manager: DBManager):
        super().__init__(name=name, description=description, inputSchema=input_schema)
        self.db_manager = db_manager

    async def handle_db_call(self, db_operation, *args, **kwargs):
        if not self.db_manager:
            logger.error("Database manager is not initialized.")
            return None, "Database manager is not initialized."
        try:
            result = await asyncio.to_thread(db_operation, *args, **kwargs)
            if result is None:
                # Check if it was an empty result from a successful query or an actual error
                # This distinction might need more refined error reporting from DBManager
                return "No data found or operation returned None.", None
            return result, None
        except ConnectionError as e:
            logger.error(f"Database connection error during tool call: {e}")
            return None, f"Database connection error: {e}"
        except ValueError as e: # e.g. bad input for an ID
            logger.error(f"Value error during tool call: {e}")
            return None, f"Invalid input: {e}"
        except Exception as e:
            logger.error(f"Unexpected error during tool call '{self.name}': {e}", exc_info=True)
            return None, f"An unexpected server error occurred: {e}"

    async def create_response(self, result: Any, error_message: Optional[str]) -> CallToolResult:
        if error_message:
            # Log with self.name if available, or a generic message if not (e.g. during db_manager init issues handled by this)
            log_name = self.name if hasattr(self, 'name') else 'UnknownTool'
            logger.warning(f"Tool call '{log_name}' failed: {error_message}")
            return CallToolResult(
                content=[TextContent(type="text", text=error_message)], 
                isError=True
            )
        else:
            log_name = self.name if hasattr(self, 'name') else 'UnknownTool'
            logger.info(f"Tool call '{log_name}' successful. Result: {str(result)[:200]}...")
            try:
                result_str = json.dumps(result)
            except TypeError as e:
                logger.error(f"Failed to serialize result to JSON for tool '{log_name}': {e}")
                return CallToolResult(
                    content=[TextContent(type="text", text=f"Server error: Failed to serialize result: {e}")],
                    isError=True
                )
            return CallToolResult(
                content=[TextContent(type="text", text=result_str)],
                isError=False
            )

    async def __call__(self, arguments: dict[str, Any]) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        raise NotImplementedError("Tool subclasses must implement __call__")

class DrupalListContentTypesTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_list_content_types",
            description="Lists all available Drupal content types (node types).",
            input_schema={"type": "object", "properties": {"random_string": {"type": "string", "description": "A dummy string argument required by the tool signature."}}, "required": ["random_string"]},
            db_manager=db_manager
        )

    async def __call__(self, random_string: str) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        logger.info(f"Executing tool: {self.name} with random_string: '{random_string}' (dummy argument)")
        db_result, db_error = await self.handle_db_call(self.db_manager.list_content_types)
        call_tool_result = await self.create_response(db_result, db_error)
        if call_tool_result.isError:
            if call_tool_result.content and isinstance(call_tool_result.content[0], TextContent):
                raise Exception(call_tool_result.content[0].text)
            raise Exception(f"Tool {self.name} failed with an unknown error.")
        return call_tool_result.content

class DrupalGetNodeByIdTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_get_node_by_id",
            description="Fetches detailed information for a specific Drupal node by its ID.",
            input_schema={
                "type": "object",
                "properties": {
                    "nid": {
                        "type": "integer",
                        "description": "The Node ID (nid)."
                    }
                },
                "required": ["nid"]
            },
            db_manager=db_manager
        )

    async def __call__(self, arguments: dict[str, Any]) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        nid = arguments.get('nid')
        logger.info(f"Executing tool: {self.name} with nid: {nid}")
        if not isinstance(nid, int):
            # Use create_response to format an error CallToolResult, then raise from its content
            call_tool_result = await self.create_response(None, "Invalid Node ID: nid must be an integer.")
            raise Exception(call_tool_result.content[0].text) 

        db_result, db_error = await self.handle_db_call(self.db_manager.get_node_by_id, nid)
        call_tool_result = await self.create_response(db_result, db_error)
        if call_tool_result.isError:
            if call_tool_result.content and isinstance(call_tool_result.content[0], TextContent):
                raise Exception(call_tool_result.content[0].text)
            raise Exception(f"Tool {self.name} failed with an unknown error.")
        return call_tool_result.content

class DrupalListVocabulariesTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_list_vocabularies",
            description="Lists all taxonomy vocabularies in Drupal.",
            input_schema={"type": "object", "properties": {"random_string": {"type": "string", "description": "A dummy string argument required by the tool signature."}}, "required": ["random_string"]},
            db_manager=db_manager
        )

    async def __call__(self, random_string: str) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        logger.info(f"Executing tool: {self.name} with random_string: '{random_string}' (dummy argument)")
        db_result, db_error = await self.handle_db_call(self.db_manager.list_vocabularies)
        call_tool_result = await self.create_response(db_result, db_error)
        if call_tool_result.isError:
            if call_tool_result.content and isinstance(call_tool_result.content[0], TextContent):
                raise Exception(call_tool_result.content[0].text)
            raise Exception(f"Tool {self.name} failed with an unknown error.")
        return call_tool_result.content

class DrupalGetTaxonomyTermByIdTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_get_taxonomy_term_by_id",
            description="Fetches detailed information for a specific taxonomy term by its ID.",
            input_schema={
                "type": "object",
                "properties": {
                    "tid": {
                        "type": "integer",
                        "description": "The Taxonomy Term ID (tid)."
                    }
                },
                "required": ["tid"]
            },
            db_manager=db_manager
        )

    async def __call__(self, arguments: dict[str, Any]) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        tid = arguments.get('tid')
        logger.info(f"Executing tool: {self.name} with tid: {tid}")
        if not isinstance(tid, int):
            # Use create_response to format an error CallToolResult, then raise from its content
            call_tool_result = await self.create_response(None, "Invalid Term ID: tid must be an integer.")
            raise Exception(call_tool_result.content[0].text)

        db_result, db_error = await self.handle_db_call(self.db_manager.get_taxonomy_term_by_id, tid)
        call_tool_result = await self.create_response(db_result, db_error)
        if call_tool_result.isError:
            if call_tool_result.content and isinstance(call_tool_result.content[0], TextContent):
                raise Exception(call_tool_result.content[0].text)
            raise Exception(f"Tool {self.name} failed with an unknown error.")
        return call_tool_result.content

class DrupalGetUserByIdTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_get_user_by_id",
            description="Fetches detailed information for a specific Drupal user by their ID.",
            input_schema={
                "type": "object",
                "properties": {
                    "uid": {
                        "type": "integer",
                        "description": "The User ID (uid)."
                    }
                },
                "required": ["uid"]
            },
            db_manager=db_manager
        )

    async def __call__(self, arguments: dict[str, Any]) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        uid = arguments.get('uid')
        logger.info(f"Executing tool: {self.name} with uid: {uid}")
        if not isinstance(uid, int):
            # Use create_response to format an error CallToolResult, then raise from its content
            call_tool_result = await self.create_response(None, "Invalid User ID: uid must be an integer.")
            raise Exception(call_tool_result.content[0].text)

        db_result, db_error = await self.handle_db_call(self.db_manager.get_user_by_id, uid)
        call_tool_result = await self.create_response(db_result, db_error)
        if call_tool_result.isError:
            if call_tool_result.content and isinstance(call_tool_result.content[0], TextContent):
                raise Exception(call_tool_result.content[0].text)
            raise Exception(f"Tool {self.name} failed with an unknown error.")
        return call_tool_result.content

class DrupalListParagraphsByNodeIdTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_list_paragraphs_by_node_id",
            description="Lists paragraph items referenced by a specific node through a given paragraph field. Note: Paragraph structure can be complex; this tool uses common conventions.",
            input_schema={
                "type": "object",
                "properties": {
                    "nid": {
                        "type": "integer",
                        "description": "The Node ID (nid) that contains the paragraphs."
                    },
                    "paragraph_field_name": {
                        "type": "string",
                        "description": "The machine name of the paragraph reference field on the node (e.g., 'field_content_paragraphs')."
                    }
                },
                "required": ["nid", "paragraph_field_name"]
            },
            db_manager=db_manager
        )

    async def __call__(self, arguments: dict[str, Any]) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        nid = arguments.get('nid')
        paragraph_field_name = arguments.get('paragraph_field_name')
        logger.info(f"Executing tool: {self.name} with nid: {nid}, paragraph_field_name: {paragraph_field_name}")

        if not isinstance(nid, int):
            # Use create_response to format an error CallToolResult, then raise from its content
            call_tool_result = await self.create_response(None, "Invalid Node ID: nid must be an integer.")
            raise Exception(call_tool_result.content[0].text)
        if not isinstance(paragraph_field_name, str) or not paragraph_field_name.strip():
            # Use create_response to format an error CallToolResult, then raise from its content
            call_tool_result = await self.create_response(None, "Invalid paragraph field name: Must be a non-empty string.")
            raise Exception(call_tool_result.content[0].text)
        
        db_result, db_error = await self.handle_db_call(self.db_manager.list_paragraphs_by_node_id, nid, paragraph_field_name)
        call_tool_result = await self.create_response(db_result, db_error)
        if call_tool_result.isError:
            if call_tool_result.content and isinstance(call_tool_result.content[0], TextContent):
                raise Exception(call_tool_result.content[0].text)
            raise Exception(f"Tool {self.name} failed with an unknown error.")
        return call_tool_result.content

class DrupalListParagraphTypesFieldsTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_list_paragraph_types_fields",
            description="Lists all paragraph types and their defined fields (name, type, required status, and settings).",
            input_schema={"type": "object", "properties": {"random_string": {"type": "string", "description": "A dummy string argument."}}, "required": ["random_string"]},
            db_manager=db_manager
        )

    async def __call__(self, random_string: str = "DEFAULT_VALUE_FOR_DIAGNOSIS") -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        logger.info(f"Executing tool: {self.name} with random_string: '{random_string}' (Note: This might be a default if input was empty at validation)")
        
        db_result, db_error = await self.handle_db_call(self.db_manager.list_paragraph_types_with_fields)
        
        call_tool_result = await self.create_response(db_result, db_error)
        if call_tool_result.isError:
            if call_tool_result.content and isinstance(call_tool_result.content[0], TextContent):
                raise Exception(call_tool_result.content[0].text)
            raise Exception(f"Tool {self.name} failed with an unknown error.")
        return call_tool_result.content

class DrupalDatabaseQueryTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_database_query",
            description="General purpose tool to interact with a Drupal database. Can list tables, get table schema, and execute read-only SQL queries.",
            input_schema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "description": "The action to perform: 'list_tables', 'get_table_schema', 'execute_sql', or 'search_all_tables'.",
                        "enum": ["list_tables", "get_table_schema", "execute_sql", "search_all_tables"]
                    },
                    "table_name": {
                        "type": "string",
                        "description": "The name of the table (without prefix) for 'get_table_schema'."
                    },
                    "sql_query": {
                        "type": "string",
                        "description": "The read-only SQL query to execute for 'execute_sql'. Use Drupal table prefixes if necessary (e.g., {node_field_data})."
                    },
                    "query_params": {
                        "type": "array",
                        "description": "A list of parameters for the SQL query (for 'execute_sql', used in placeholders like %s).",
                        "items": {"type": "string"} # Assuming params are strings, can be more generic if needed
                    },
                    "search_string": {
                        "type": "string",
                        "description": "The string to search for when action is 'search_all_tables'."
                    },
                    "row_limit_per_column": {
                        "type": "integer",
                        "description": "Maximum number of rows to return per column match for 'search_all_tables' action. Defaults to 5."
                    }
                },
                "required": ["action"]
            },
            db_manager=db_manager
        )

    async def __call__(self, 
                       action: str, 
                       table_name: Optional[str] = None, 
                       sql_query: Optional[str] = None, 
                       query_params: Optional[List[str]] = None,
                       search_string: Optional[str] = None,
                       row_limit_per_column: Optional[int] = None) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
        
        logger.info(f"Executing tool: {self.name} with action: {action}, table_name: {table_name}, sql_query: {sql_query is not None}, query_params: {query_params is not None}, search_string: {search_string is not None}")

        result = None
        error_message = None

        if action == 'list_tables':
            result, error_message = await self.handle_db_call(self.db_manager.get_tables)
        elif action == 'get_table_schema':
            if not table_name:
                error_message = "'table_name' is required for 'get_table_schema' action."
            else:
                result, error_message = await self.handle_db_call(self.db_manager.get_table_schema, table_name)
        elif action == 'execute_sql':
            if not sql_query:
                error_message = "'sql_query' is required for 'execute_sql' action."
            elif not sql_query.strip().upper().startswith("SELECT"):
                error_message = "Only SELECT queries are allowed for 'execute_sql' action for safety."
            else:
                params_tuple = tuple(query_params) if query_params else None
                # For execute_sql, the result might be a list (for SELECT) or None (for DDL/DML if allowed in future)
                # The handle_db_call will return the direct result or an error string.
                db_call_result, db_error_message = await self.handle_db_call(self.db_manager.execute_query, sql_query, params_tuple)
                if db_error_message:
                    error_message = db_error_message
                elif db_call_result is not None:
                    result = db_call_result # This will be the list of dicts or a single dict
                else:
                    # If db_call_result is None and no error, it might mean query had no output (e.g. non-SELECT, or SELECT with no rows)
                    # Check cursor.rowcount for non-SELECT statements if we want to return affected rows info.
                    # For now, we treat None result (without error) as "Query executed, no data returned or affected."
                    # The new base class returns JSON, so we should ensure result is serializable or a message.
                    if self.db_manager and self.db_manager.cursor and self.db_manager.cursor.rowcount != -1:
                        result = {"message": "Query executed successfully.", "rows_affected_or_matched": self.db_manager.cursor.rowcount}
                    else:
                        result = {"message": "Query executed. No results returned or an error occurred (check server logs if expecting data)."}
        elif action == 'search_all_tables':
            if not search_string:
                error_message = "'search_string' is required for 'search_all_tables' action."
            else:
                limit = row_limit_per_column if row_limit_per_column is not None else 5
                result, error_message = await self.handle_db_call(self.db_manager.search_string_in_all_tables, search_string, limit)
        else:
            error_message = f"Unknown action: {action}"

        # Use the base class create_response method for consistent response formatting
        # If result is already a string message (like "No data found..."), it will be wrapped in JSON by create_response.
        # If error_message is set, result will be ignored by create_response.
        call_tool_result = await self.create_response(result, error_message)
        if call_tool_result.isError:
            if call_tool_result.content and isinstance(call_tool_result.content[0], TextContent):
                raise Exception(call_tool_result.content[0].text)
            raise Exception(f"Tool {self.name} failed with an unknown error.")
        return call_tool_result.content

def find_project_root(settings_file_path: str) -> Optional[str]:
    """Tries to find a project root by looking for a .ddev directory or common markers."""
    current_path = os.path.dirname(os.path.abspath(settings_file_path))
    # Common Drupal structures: sites/default/settings.php or web/sites/default/settings.php
    # Look up a few levels for .ddev or composer.json
    for _ in range(5): # Check up to 5 levels
        if os.path.isdir(os.path.join(current_path, ".ddev")):
            return current_path
        if os.path.isfile(os.path.join(current_path, "composer.json")) and os.path.isdir(os.path.join(current_path, "web")):
             # A common pattern for composer-based Drupal projects, .ddev might be here
            if os.path.isdir(os.path.join(current_path, ".ddev")):
                return current_path
        parent_path = os.path.dirname(current_path)
        if parent_path == current_path: # Reached filesystem root
            break
        current_path = parent_path
    return None

def detect_and_get_ddev_db_info(project_root: str) -> Optional[Tuple[str, int]]:
    """Checks for a DDEV project and runs 'ddev describe' to get DB host and port."""
    if not project_root or not os.path.isdir(os.path.join(project_root, ".ddev")):
        logger.info("Not a DDEV project or .ddev directory not found at presumed root.")
        return None

    try:
        logger.info(f"DDEV project detected at {project_root}. Running 'ddev describe'...")
        original_cwd = os.getcwd()
        os.chdir(project_root) # Change to project root

        result = subprocess.run(
            ["ddev", "describe"], # Removed --project-root
            capture_output=True,
            text=True,
            check=True,
            timeout=15
        )
        output = result.stdout
        logger.debug(f"'ddev describe' output:\n{output}")

        # Regex to find the MySQL/MariaDB host and port exposed to the host machine
        # Example line: │  - db:3306 -> 127.0.0.1:32801                      │ User/Pass: 'db/db' │
        match = re.search(r"[a-zA-Z0-9_-]+:3306\s*->\s*(127\.0\.0\.1):(\d+)", output, re.IGNORECASE)
        if match:
            host = match.group(1)
            port = int(match.group(2))
            logger.info(f"DDEV discovered database at {host}:{port}")
            return host, port
        else:
            logger.warning("Could not parse DDEV host/port from 'ddev describe' output.")
            return None
    except FileNotFoundError:
        logger.warning("'ddev' command not found. Is DDEV installed and in PATH?")
        return None
    except subprocess.CalledProcessError as e:
        logger.error(f"'ddev describe' failed: {e}\nStderr: {e.stderr}")
        return None
    except subprocess.TimeoutExpired:
        logger.error("'ddev describe' timed out.")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while running/parsing 'ddev describe': {e}")
        return None
    finally:
        if 'original_cwd' in locals(): # Ensure original_cwd was set
            os.chdir(original_cwd) # Change back to original cwd

async def main():
    global db_manager_instance
    global server # Declare that we are using the global server variable

    parser = argparse.ArgumentParser(description="MCP Server for Drupal Database Interaction.")
    # Make settings_file not strictly required at argparse level
    parser.add_argument("--settings_file", type=str, help="Path to the Drupal settings.php file. Can also be set via MCP_DRUPAL_SETTINGS_FILE env var.")
    # Removed host and port arguments
    parser.add_argument("--db-host", type=str, help="Override database host (e.g., for DDEV).")
    parser.add_argument("--db-port", type=int, help="Override database port (e.g., for DDEV).")
    args = parser.parse_args()

    # Determine settings_file_to_parse
    settings_file_to_parse = args.settings_file
    if not settings_file_to_parse:
        settings_file_to_parse = os.environ.get("MCP_DRUPAL_SETTINGS_FILE")
        if settings_file_to_parse:
            logger.info("Using settings_file path from MCP_DRUPAL_SETTINGS_FILE environment variable.")
        else:
            logger.error("Error: --settings_file argument is required, or MCP_DRUPAL_SETTINGS_FILE environment variable must be set.")
            parser.print_help()
            return # Exit if no settings file path is available

    db_config_source_description = f"initial settings file: {settings_file_to_parse}"

    # DDEV detection and db_config override logic
    if args.db_host and args.db_port:
        logger.info(f"Using manually specified DB host: {args.db_host} and port: {args.db_port}")
        # Parse the original settings file first to get other DB details like user, pass, dbname
        db_config = parse_settings_php(settings_file_to_parse)
        if not db_config:
            logger.error(f"Failed to parse base database configuration from {settings_file_to_parse} when manual overrides were given. Exiting.")
            return
        db_config['host'] = args.db_host
        db_config['port'] = str(args.db_port) 
        db_config_source_description = "manual DB host/port override"
    else:
        drupal_project_root = find_project_root(settings_file_to_parse)
        if drupal_project_root and os.path.isdir(os.path.join(drupal_project_root, ".ddev")):
            logger.info(f"DDEV project detected at {drupal_project_root}.")
            
            settings_dir = os.path.dirname(os.path.abspath(settings_file_to_parse))
            potential_ddev_files_to_try = [
                os.path.join(settings_dir, 'settings.local.php'),
                os.path.join(settings_dir, 'settings.ddev.php')
            ]
            
            db_config = None
            parsed_ddev_specific_file = False
            for candidate_path in potential_ddev_files_to_try:
                if os.path.isfile(candidate_path):
                    logger.info(f"Attempting to parse DDEV-related settings from: {candidate_path}")
                    temp_config = parse_settings_php(candidate_path)
                    if temp_config and 'username' in temp_config: # Check if parsing was successful and got key fields
                        db_config = temp_config
                        settings_file_to_parse = candidate_path # Update for logging and consistency
                        db_config_source_description = f"DDEV-related settings: {candidate_path}"
                        parsed_ddev_specific_file = True
                        logger.info(f"Successfully parsed DB credentials from {candidate_path}")
                        break # Found a good DDEV settings file
                    else:
                        logger.info(f"Could not extract complete DB credentials from {candidate_path}. Will try next candidate or fall back.")
                else:
                    logger.info(f"DDEV-related candidate file not found: {candidate_path}")
            
            if not parsed_ddev_specific_file:
                logger.info(f"No suitable DDEV-specific settings file yielded credentials. Parsing main settings file: {settings_file_to_parse}")
                settings_file_to_parse = settings_file_to_parse # Ensure this is the actual file being parsed now
                db_config = parse_settings_php(settings_file_to_parse)
                db_config_source_description = f"main settings file: {settings_file_to_parse}" # Corrected source description

            if not db_config: # If, after all attempts, db_config is still None
                logger.error(f"Failed to parse database configuration from all attempted sources (last tried: {settings_file_to_parse}). Exiting.")
                return

            # Get DDEV host/port from 'ddev describe' and override if found
            ddev_db_info = detect_and_get_ddev_db_info(drupal_project_root)
            if ddev_db_info:
                ddev_host, ddev_port = ddev_db_info
                logger.info(f"Overriding DB host/port with DDEV discovered: {ddev_host}:{ddev_port}")
                db_config['host'] = ddev_host
                db_config['port'] = str(ddev_port)
                db_config_source_description += " (with DDEV host/port override)"
            else:
                logger.info("DDEV host/port discovery failed or not applicable. Using host/port from settings file.")
        else:
            logger.info("Not a DDEV project or root not found. Using settings from the provided file path.")
            db_config = parse_settings_php(settings_file_to_parse)
            if not db_config:
                logger.error(f"Failed to parse database configuration from {settings_file_to_parse}. Exiting.")
                return

    # Prepare values for logging to avoid complex f-string nesting issues
    log_db_driver = db_config.get('driver')
    log_db_host = db_config.get('host')
    log_db_port = db_config.get('port')
    log_db_user = db_config.get('username')
    logger.info(f"Effective database configuration (from {db_config_source_description}): driver '{log_db_driver}', host '{log_db_host}', port '{log_db_port}', user '{log_db_user}'")

    try:
        db_manager_instance = DBManager(db_config)
    except ConnectionError as e:
        logger.error(f"Failed to initialize DBManager: {e}. Ensure database is running and accessible.")
        return
    except ValueError as e: # Catch unsupported driver from DBManager init
        logger.error(f"Failed to initialize DBManager: {e}.")
        return

    # Instantiate all tools
    if not db_manager_instance:
        logger.error("DBManager not initialized before tool creation. Exiting.")
        return

    drupal_query_tool = DrupalDatabaseQueryTool(db_manager=db_manager_instance)
    list_content_types_tool = DrupalListContentTypesTool(db_manager=db_manager_instance)
    get_node_by_id_tool = DrupalGetNodeByIdTool(db_manager=db_manager_instance)
    list_vocabularies_tool = DrupalListVocabulariesTool(db_manager=db_manager_instance)
    get_term_by_id_tool = DrupalGetTaxonomyTermByIdTool(db_manager=db_manager_instance)
    get_user_by_id_tool = DrupalGetUserByIdTool(db_manager=db_manager_instance)
    list_paragraphs_tool = DrupalListParagraphsByNodeIdTool(db_manager=db_manager_instance)
    list_paragraph_types_fields_tool = DrupalListParagraphTypesFieldsTool(db_manager=db_manager_instance)

    all_tools_objects = [
        drupal_query_tool,
        list_content_types_tool,
        get_node_by_id_tool,
        list_vocabularies_tool,
        get_term_by_id_tool,
        get_user_by_id_tool,
        list_paragraphs_tool,
        list_paragraph_types_fields_tool
    ]
    
    # Register tools using the server's add_tool method and then try to override schema
    for tool_obj in all_tools_objects:
        if not hasattr(server, 'add_tool') or not hasattr(server, '_tool_manager') or not hasattr(server._tool_manager, '_tools'):
            logger.error("Server object or its ToolManager is not configured as expected. Cannot register tools.")
            return
        
        try:
            # Register the tool using server.add_tool.
            # This will infer a basic schema from tool_obj.__call__(self, arguments: dict).
            server.add_tool(
                fn=tool_obj.__call__,
                name=tool_obj.name,
                description=tool_obj.description
            )
            logger.info(f"Registered tool via server.add_tool: {tool_obj.name}")

            # Attempt to override the inferred schema with our predefined inputSchema.
            registered_tool_handler = server._tool_manager._tools.get(tool_obj.name)
            if registered_tool_handler:
                if hasattr(registered_tool_handler, 'parameters'):
                    # Check the signature of the __call__ method
                    call_signature = inspect.signature(tool_obj.__call__)
                    # Parameters count includes 'self'. So 2 means 'self' and one other argument.
                    if len(call_signature.parameters) == 2 and list(call_signature.parameters.keys())[0] == 'self':
                        # If the __call__ method takes exactly one argument (plus self),
                        # it's safer to rely on FastMCP's inferred schema from this direct signature
                        # to avoid potential mismatches in parameter extraction vs. validation.
                        logger.info(f"Relying on inferred schema for single-argument tool: {tool_obj.name}")
                    else:
                        logger.info(f"Overriding schema for tool: {tool_obj.name} with inputSchema: {tool_obj.inputSchema}")
                        registered_tool_handler.parameters = tool_obj.inputSchema
                else:
                    logger.warning(f"Registered tool {tool_obj.name} does not have 'parameters' attribute to override schema.")
            else:
                logger.warning(f"Could not find registered tool {tool_obj.name} in ToolManager to override schema.")

        except Exception as e:
            logger.error(f"Failed to register or modify tool {tool_obj.name}: {e}", exc_info=True)

    logger.info(f"Starting MCP Drupal Database Server with stdio transport.")
    logger.info("Server is ready to accept connections from MCP clients via stdio.")
    logger.info("Press Ctrl+C to stop the server (if run interactively).")

    try:
        # server.run is synchronous, ensure it's called correctly in an async context
        # For stdio, typically server.run(transport='stdio') is synchronous.
        # If FastMCP's run can be awaited or needs special handling for asyncio, adjust accordingly.
        # Assuming server.run(transport='stdio') is the correct synchronous call:
        # To run a sync function in async, you might need asyncio.to_thread or similar
        # However, for a top-level script, directly calling server.run might be intended.
        # Let's assume FastMCP handles the asyncio loop if run is called from within one.
        # The MCP examples for Python often show mcp.run(transport='stdio') in the `if __name__ == "__main__":` block directly.
        # If main() must be async, and server.run is sync, it needs careful handling.
        # For now, let's try the direct call as per typical MCP examples.
        # If this causes issues, we may need to wrap it or restructure.
        
        # The standard way to run with stdio:
        # server.run(transport='stdio') # This was causing the nested asyncio loop error
        await server.run_stdio_async() # Use the async version for stdio

    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    finally:
        if db_manager_instance:
            db_manager_instance.close()
        logger.info("Server shutdown complete.")

if __name__ == "__main__":
    try:
        # If main is now less async-focused due to server.run(transport='stdio') being sync,
        # we might not need asyncio.run(main()) if main() itself becomes synchronous.
        # However, other parts of main (like tool calls if they were awaited directly in main)
        # might still require an async context.
        # Given `await server.run_streamable_http_async()` was used, `main` is async.
        # `server.run(transport='stdio')` is typically blocking and synchronous.
        # To call a blocking sync function from an async function, use asyncio.to_thread.
        
        # Let's keep main() async for now and adapt the run call.
        # If server.run(transport='stdio') is blocking, we should run it in a thread.
        # Or, FastMCP might have an async version for stdio.
        # The quickstart shows `mcp.run(transport='stdio')` directly in `if __name__ == "__main__"`.
        # This implies it manages its own loop or is synchronous.
        
        # Re-evaluating: If main() contains `await` for other things (not shown in snippet but possible),
        # it must remain async. If `server.run(transport='stdio')` is blocking, it will block the asyncio event loop.
        # The example `if __name__ == "__main__": mcp.run(transport='stdio')`
        # implies `mcp.run` takes over.

        # Let's simplify `main` to be synchronous if its only async part was `server.run_streamable_http_async`.
        # Then `asyncio.run(main())` would be `main()`.
        # For now, I will assume FastMCP's `run(transport='stdio')` can be called from an async main
        # and it will block appropriately or integrate with the existing loop.
        # This is the most direct change from run_streamable_http_async().

        asyncio.run(main())
    except Exception as e:
        logger.error(f"Critical error during server startup or runtime: {e}", exc_info=True) 