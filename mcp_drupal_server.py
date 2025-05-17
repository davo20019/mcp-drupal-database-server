import argparse
import logging
import asyncio
from typing import Optional, List, Dict, Any

from model_context_protocol.server import ModelContextServer, Tool, tool
from model_context_protocol.server_types import (    
    ToolCallContext,
    ToolCallRequest,
    ToolCallResponse,
    ToolCallResponsePayload,
    ToolCallResponseContent,
    ToolSchema, 
    ToolParameterSchema
)

from drupal_settings_parser import parse_settings_php
from db_manager import DBManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mcp_drupal_server")

# Global variable to hold the DBManager instance
# This is not ideal for larger applications but suitable for this focused server.
# In a more complex scenario, consider dependency injection or context management.
db_manager_instance: Optional[DBManager] = None

class DrupalBaseTool(Tool):
    def __init__(self, name: str, description: str, parameters: List[ToolParameterSchema], db_manager: DBManager):
        super().__init__(name=name, description=description, parameters=parameters)
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

    async def create_response(self, request_id: str, result: Any, error_message: Optional[str]) -> ToolCallResponse:
        if error_message:
            logger.warning(f"Tool call '{self.name}' failed for request {request_id}: {error_message}")
            return ToolCallResponse(
                id=request_id,
                tool_name=self.name,
                payload=ToolCallResponsePayload(error=error_message)
            )
        else:
            logger.info(f"Tool call '{self.name}' successful for request {request_id}. Result: {str(result)[:200]}...")
            return ToolCallResponse(
                id=request_id,
                tool_name=self.name,
                payload=ToolCallResponsePayload(
                    content=[ToolCallResponseContent(json=result)] # Return structured JSON
                )
            )

class DrupalListContentTypesTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_list_content_types",
            description="Lists all available Drupal content types (node types).",
            parameters=[],
            db_manager=db_manager
        )

    async def __call__(self, context: ToolCallContext, request: ToolCallRequest) -> ToolCallResponse:
        logger.info(f"Executing tool: {self.name}")
        result, error = await self.handle_db_call(self.db_manager.list_content_types)
        return await self.create_response(request.id, result, error)

class DrupalGetNodeByIdTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_get_node_by_id",
            description="Fetches detailed information for a specific Drupal node by its ID.",
            parameters=[
                ToolParameterSchema(name="nid", description="The Node ID (nid).", type="integer", required=True)
            ],
            db_manager=db_manager
        )

    async def __call__(self, context: ToolCallContext, request: ToolCallRequest) -> ToolCallResponse:
        nid = request.parameters.get('nid')
        logger.info(f"Executing tool: {self.name} with nid: {nid}")
        if not isinstance(nid, int):
            return await self.create_response(request.id, None, "Invalid Node ID: nid must be an integer.")
        result, error = await self.handle_db_call(self.db_manager.get_node_by_id, nid)
        return await self.create_response(request.id, result, error)

class DrupalListVocabulariesTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_list_vocabularies",
            description="Lists all taxonomy vocabularies in Drupal.",
            parameters=[],
            db_manager=db_manager
        )

    async def __call__(self, context: ToolCallContext, request: ToolCallRequest) -> ToolCallResponse:
        logger.info(f"Executing tool: {self.name}")
        result, error = await self.handle_db_call(self.db_manager.list_vocabularies)
        return await self.create_response(request.id, result, error)

class DrupalGetTaxonomyTermByIdTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_get_taxonomy_term_by_id",
            description="Fetches detailed information for a specific taxonomy term by its ID.",
            parameters=[
                ToolParameterSchema(name="tid", description="The Taxonomy Term ID (tid).", type="integer", required=True)
            ],
            db_manager=db_manager
        )

    async def __call__(self, context: ToolCallContext, request: ToolCallRequest) -> ToolCallResponse:
        tid = request.parameters.get('tid')
        logger.info(f"Executing tool: {self.name} with tid: {tid}")
        if not isinstance(tid, int):
            return await self.create_response(request.id, None, "Invalid Term ID: tid must be an integer.")
        result, error = await self.handle_db_call(self.db_manager.get_taxonomy_term_by_id, tid)
        return await self.create_response(request.id, result, error)

class DrupalGetUserByIdTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_get_user_by_id",
            description="Fetches detailed information for a specific Drupal user by their ID.",
            parameters=[
                ToolParameterSchema(name="uid", description="The User ID (uid).", type="integer", required=True)
            ],
            db_manager=db_manager
        )

    async def __call__(self, context: ToolCallContext, request: ToolCallRequest) -> ToolCallResponse:
        uid = request.parameters.get('uid')
        logger.info(f"Executing tool: {self.name} with uid: {uid}")
        if not isinstance(uid, int):
            return await self.create_response(request.id, None, "Invalid User ID: uid must be an integer.")
        result, error = await self.handle_db_call(self.db_manager.get_user_by_id, uid)
        return await self.create_response(request.id, result, error)

class DrupalListParagraphsByNodeIdTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_list_paragraphs_by_node_id",
            description="Lists paragraph items referenced by a specific node through a given paragraph field. Note: Paragraph structure can be complex; this tool uses common conventions.",
            parameters=[
                ToolParameterSchema(name="nid", description="The Node ID (nid) that contains the paragraphs.", type="integer", required=True),
                ToolParameterSchema(name="paragraph_field_name", description="The machine name of the paragraph reference field on the node (e.g., 'field_content_paragraphs').", type="string", required=True)
            ],
            db_manager=db_manager
        )

    async def __call__(self, context: ToolCallContext, request: ToolCallRequest) -> ToolCallResponse:
        nid = request.parameters.get('nid')
        paragraph_field_name = request.parameters.get('paragraph_field_name')
        logger.info(f"Executing tool: {self.name} with nid: {nid}, paragraph_field_name: {paragraph_field_name}")

        if not isinstance(nid, int):
            return await self.create_response(request.id, None, "Invalid Node ID: nid must be an integer.")
        if not isinstance(paragraph_field_name, str) or not paragraph_field_name.strip():
            return await self.create_response(request.id, None, "Invalid paragraph field name: Must be a non-empty string.")
        
        result, error = await self.handle_db_call(self.db_manager.list_paragraphs_by_node_id, nid, paragraph_field_name)
        return await self.create_response(request.id, result, error)

class DrupalDatabaseQueryTool(DrupalBaseTool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_database_query",
            description="General purpose tool to interact with a Drupal database. Can list tables, get table schema, and execute read-only SQL queries.",
            parameters=[
                ToolParameterSchema(
                    name="action",
                    description="The action to perform: 'list_tables', 'get_table_schema', or 'execute_sql'.",
                    type="string",
                    required=True,
                    enum=['list_tables', 'get_table_schema', 'execute_sql']
                ),
                ToolParameterSchema(
                    name="table_name",
                    description="The name of the table (without prefix) for 'get_table_schema'.",
                    type="string",
                    required=False
                ),
                ToolParameterSchema(
                    name="sql_query",
                    description="The read-only SQL query to execute for 'execute_sql'. Use Drupal table prefixes if necessary (e.g., {node_field_data}).",
                    type="string",
                    required=False
                ),
                ToolParameterSchema(
                    name="query_params",
                    description="A list of parameters for the SQL query (for 'execute_sql', used in placeholders like %s).",
                    type="array", 
                    required=False
                )
            ],
            db_manager=db_manager
        )

    async def __call__(self, context: ToolCallContext, request: ToolCallRequest) -> ToolCallResponse:
        action = request.parameters.get('action')
        table_name = request.parameters.get('table_name')
        sql_query = request.parameters.get('sql_query')
        query_params = request.parameters.get('query_params')

        logger.info(f"Executing tool: {self.name} with action: {action}, table_name: {table_name}, sql_query: {sql_query is not None}, query_params: {query_params is not None}")

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
        else:
            error_message = f"Unknown action: {action}"

        # Use the base class create_response method for consistent response formatting
        # If result is already a string message (like "No data found..."), it will be wrapped in JSON by create_response.
        # If error_message is set, result will be ignored by create_response.
        return await self.create_response(request.id, result, error_message)

async def main():
    global db_manager_instance

    parser = argparse.ArgumentParser(description="MCP Server for Drupal Database Interaction.")
    parser.add_argument("--settings_file", type=str, required=True, help="Path to the Drupal settings.php file.")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host for the MCP server.")
    parser.add_argument("--port", type=int, default=6789, help="Port for the MCP server.")
    args = parser.parse_args()

    logger.info(f"Attempting to parse settings file: {args.settings_file}")
    db_config = parse_settings_php(args.settings_file)

    if not db_config:
        logger.error("Failed to parse database configuration. Exiting.")
        return

    logger.info(f"Database configuration parsed successfully: {db_config.get('driver')} on {db_config.get('host')}")

    try:
        db_manager_instance = DBManager(db_config)
    except ConnectionError as e:
        logger.error(f"Failed to initialize DBManager: {e}. Ensure database is running and accessible.")
        return
    except ValueError as e: # Catch unsupported driver from DBManager init
        logger.error(f"Failed to initialize DBManager: {e}.")
        return

    # Instantiate all tools
    drupal_query_tool = DrupalDatabaseQueryTool(db_manager=db_manager_instance)
    list_content_types_tool = DrupalListContentTypesTool(db_manager=db_manager_instance)
    get_node_by_id_tool = DrupalGetNodeByIdTool(db_manager=db_manager_instance)
    list_vocabularies_tool = DrupalListVocabulariesTool(db_manager=db_manager_instance)
    get_term_by_id_tool = DrupalGetTaxonomyTermByIdTool(db_manager=db_manager_instance)
    get_user_by_id_tool = DrupalGetUserByIdTool(db_manager=db_manager_instance)
    list_paragraphs_tool = DrupalListParagraphsByNodeIdTool(db_manager=db_manager_instance)

    all_tools = [
        drupal_query_tool,
        list_content_types_tool,
        get_node_by_id_tool,
        list_vocabularies_tool,
        get_term_by_id_tool,
        get_user_by_id_tool,
        list_paragraphs_tool
    ]
    
    server = ModelContextServer(
        tools=all_tools,
        host=args.host,
        port=args.port,
    )

    logger.info(f"Starting MCP Drupal Database Server on {args.host}:{args.port}")
    for t in all_tools:
        logger.info(f"Registered tool: {t.name}")
    logger.info("Server is ready to accept connections from MCP clients.")
    logger.info("Press Ctrl+C to stop the server.")

    try:
        await server.serve()
    except KeyboardInterrupt:
        logger.info("Server shutting down...")
    finally:
        if db_manager_instance:
            db_manager_instance.close()
        logger.info("Server shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Critical error during server startup or runtime: {e}", exc_info=True) 