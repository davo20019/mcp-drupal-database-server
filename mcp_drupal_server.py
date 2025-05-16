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

class DrupalDatabaseTool(Tool):
    def __init__(self, db_manager: DBManager):
        super().__init__(
            name="drupal_database_query",
            description="Interact with a Drupal database. Can list tables, get table schema, and execute read-only SQL queries.",
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
                    type="array", # MCP might prefer "array" with items of type "string" or "number"
                    required=False
                )
            ]
        )
        self.db_manager = db_manager

    async def __call__(self, context: ToolCallContext, request: ToolCallRequest) -> ToolCallResponse:
        action = request.parameters.get('action')
        table_name = request.parameters.get('table_name')
        sql_query = request.parameters.get('sql_query')
        query_params = request.parameters.get('query_params') # This will be a list

        content = ""
        error_message = None

        logger.info(f"Received action: {action} with params: table_name={table_name}, sql_query={sql_query}, query_params={query_params}")

        if not self.db_manager:
            error_message = "Database manager is not initialized."
            logger.error(error_message)
            return ToolCallResponse(
                id=request.id,
                tool_name=self.name,
                payload=ToolCallResponsePayload(error=error_message)
            )

        try:
            if action == 'list_tables':
                tables = await asyncio.to_thread(self.db_manager.get_tables)
                if tables is not None:
                    content = f"Tables: {tables}"
                else:
                    content = "Failed to retrieve tables or no tables found."
            
            elif action == 'get_table_schema':
                if not table_name:
                    error_message = "'table_name' is required for 'get_table_schema' action."
                else:
                    schema = await asyncio.to_thread(self.db_manager.get_table_schema, table_name)
                    if schema:
                        content = f"Schema for {table_name}: {schema}"
                    else:
                        content = f"Failed to retrieve schema for table '{table_name}' or table does not exist."
            
            elif action == 'execute_sql':
                if not sql_query:
                    error_message = "'sql_query' is required for 'execute_sql' action."
                # Basic safety check: only allow SELECT queries for now.
                # More sophisticated parsing/validation might be needed for production.
                elif not sql_query.strip().upper().startswith("SELECT"):
                    error_message = "Only SELECT queries are allowed for 'execute_sql' action for safety."
                else:
                    # Ensure query_params is a tuple if provided
                    params_tuple = tuple(query_params) if query_params else None
                    results = await asyncio.to_thread(self.db_manager.execute_query, sql_query, params_tuple)
                    if results is not None:
                        content = f"Query results: {results}"
                    else:
                        # execute_query returns None on error or for non-SELECT, [] for no rows.
                        content = "Query executed. No results returned or an error occurred (check server logs)."
                        if self.db_manager.cursor and self.db_manager.cursor.rowcount != -1:
                             content += f" Rows affected/matched: {self.db_manager.cursor.rowcount}"
            else:
                error_message = f"Unknown action: {action}"

        except ConnectionError as e:
            logger.error(f"Database connection error during tool call: {e}")
            error_message = f"Database connection error: {e}"
        except ValueError as e:
            logger.error(f"Value error during tool call: {e}")
            error_message = f"Invalid input or configuration: {e}"
        except Exception as e:
            logger.error(f"Unexpected error during tool call '{action}': {e}", exc_info=True)
            error_message = f"An unexpected error occurred: {e}"

        if error_message:
            logger.warning(f"Tool call failed: {error_message}")
            return ToolCallResponse(
                id=request.id,
                tool_name=self.name,
                payload=ToolCallResponsePayload(error=error_message)
            )
        else:
            logger.info(f"Tool call successful. Content: {content[:200]}...")
            return ToolCallResponse(
                id=request.id,
                tool_name=self.name,
                payload=ToolCallResponsePayload(
                    content=[ToolCallResponseContent(text=str(content))] # Ensure content is string
                )
            )

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

    drupal_tool = DrupalDatabaseTool(db_manager=db_manager_instance)
    
    server = ModelContextServer(
        tools=[drupal_tool],
        host=args.host,
        port=args.port,
        # You can add more server options here, like identity, etc.
    )

    logger.info(f"Starting MCP Drupal Database Server on {args.host}:{args.port}")
    logger.info(f"Registered tool: {drupal_tool.name}")
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