# MCP Drupal Database Server

This project implements a Model Context Protocol (MCP) server that allows AI models and other MCP-compatible clients to interact with a Drupal database. The server can parse Drupal's `settings.php` file to get database credentials, connect to the database (MySQL or PostgreSQL), and expose tools to list tables, get table schemas, and execute read-only SQL queries.

## Features

*   **Drupal `settings.php` Parsing**: Automatically extracts database connection details.
*   **Multi-Database Support**: Works with both MySQL and PostgreSQL Drupal installations.
*   **MCP Compliant**: Uses the official `model-context-protocol` Python SDK.
*   **Read-Only Operations**: Designed for safe data retrieval with a focus on read-only SQL execution to prevent accidental data modification through the MCP interface.
*   **Asynchronous**: Built with `asyncio` for efficient handling of requests.

## Project Structure

```
.mcp-drupal-database-server/
├── mcp_drupal_server.py    # Main MCP server application script
├── drupal_settings_parser.py # Module to parse Drupal's settings.php
├── db_manager.py             # Module to manage database connections and queries
├── requirements.txt        # Python package dependencies
└── README.md               # This file
```

## Prerequisites

*   Python 3.10+ (This is a requirement of the `mcp` package itself)
*   Access to a Drupal site's `settings.php` file.
*   A running Drupal database (MySQL or PostgreSQL) accessible from where the server will run.

### Installing Python 3.10+

If the `setup.sh` script indicates that your Python version is too old (it requires 3.10 or newer for the `mcp` package), you will need to install an appropriate Python version. Here are some common ways to do this:

*   **macOS (using Homebrew):**
    ```bash
    brew install python@3.10 # Or python@3.11, python@3.12
    # Follow any post-install instructions from Homebrew.
    # You might need to open a new terminal window after installation.
    ```
*   **Linux (Debian/Ubuntu-based):**
    ```bash
    sudo apt update
    sudo apt install python3.10 # Or python3.11, python3.12
    # For older Ubuntu versions or if the package isn't found, you might need the deadsnakes PPA:
    # sudo add-apt-repository ppa:deadsnakes/ppa
    # sudo apt update
    # sudo apt install python3.10
    ```
*   **Linux (Fedora/CentOS/RHEL-based):**
    ```bash
    sudo dnf install python3.10 # Or python3.11, python3.12 (Package names may vary)
    ```
*   **From python.org:**
    Download the official installer for your operating system from [https://www.python.org/downloads/](https://www.python.org/downloads/).

After installing a newer Python version, ensure it's available in your system's PATH. You might need to open a new terminal session. Then, if you had a `./venv` directory from a previous attempt, remove it (`rm -rf ./venv`) before re-running the `./setup.sh` script.

## Setup

1.  **Clone the repository (or create the files as listed above).**

2.  **Create and activate a virtual environment (recommended):**
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
    Alternatively, you can use the provided setup script (ensure it's executable: `chmod +x setup.sh`):
    ```bash
    ./setup.sh
    ```
    This script will also attempt to create the virtual environment and install dependencies.

3.  **Install dependencies (if not using `setup.sh` or if it fails to activate the venv for `pip`):**
    Ensure your virtual environment is active, then:
    ```bash
    pip install -r requirements.txt
    ```

## Running the Server

Once setup is complete and the virtual environment is active, you can run the server.

**For client integration (e.g., Windsurf, Claude for Desktop):**

The server is primarily designed to be launched by an MCP client using `stdio` (standard input/output) transport. The client will execute the `mcp_drupal_server.py` script directly.
Example client configuration (conceptual):
```json
{
  "mcpServers": {
    "drupal_db_server": {
      "command": "python", // Or path to venv python
      "args": [
        "/full/path/to/your/mcp-drupal-database-server/mcp_drupal_server.py",
        "--settings_file", "/path/to/your/drupal/sites/default/settings.php"
        // Add other necessary args like --db-host, --db-port if overriding DDEV/settings.php
      ],
      "env": {
        // Or set MCP_DRUPAL_SETTINGS_FILE here
        // "MCP_DRUPAL_SETTINGS_FILE": "/path/to/your/drupal/sites/default/settings.php"
      }
    }
  }
}
```
In this mode, the `--host` and `--port` arguments for the MCP server itself are not used, as communication happens over stdio.

**For direct execution (e.g., testing, or if you want an HTTP server):**

You can still use the `run_server.sh` script, which can run the server over HTTP:
```bash
./run_server.sh --settings_file /path/to/your/drupal/sites/default/settings.php --host 127.0.0.1 --port 6789
```
This script internally calls `python mcp_drupal_server.py` and can pass along `--host` and `--port` if you have modified `mcp_drupal_server.py` to handle them for HTTP mode (currently it defaults to stdio). The `run_server.sh` script itself still accepts `--host` and `--port` and would need to be adapted if you intend to use it to launch the Python script in HTTP mode.
*As of the latest changes, `mcp_drupal_server.py` itself has been streamlined for `stdio` and does not accept `--host` or `--port` for its own MCP transport. The `run_server.sh` script's ability to pass these is for its own interface; how it then translates these to the Python script would require `mcp_drupal_server.py` to re-introduce HTTP transport options.*

**Using `mcp dev` for local development:**

The server is also compatible with the `mcp dev` tool for easier local development and testing:
```bash
MCP_DRUPAL_SETTINGS_FILE="/path/to/your/drupal/sites/default/settings.php" ./venv/bin/mcp dev -e . mcp_drupal_server.py
```
This requires the `MCP_DRUPAL_SETTINGS_FILE` environment variable to be set, or you can pass `--settings_file` as an argument if `mcp dev` forwards extra arguments to the script. The `FastMCP` instance is defined globally in `mcp_drupal_server.py` for compatibility with `mcp dev`.

**Command-line arguments (for `mcp_drupal_server.py` when run by a client or `mcp dev`):**

*   `--settings_file` (required if `MCP_DRUPAL_SETTINGS_FILE` env var is not set): Absolute or relative path to the Drupal `settings.php` file.
*   `--db-host` (optional): Override the database host (e.g., for DDEV if detection fails or is not used).
*   `--db-port` (optional): Override the database port.

Upon successful startup via a client, you should see tools appear in the client. If run directly with `mcp dev` or in a way that logs to console, you'll see messages like:
```
INFO:mcp_drupal_server:Starting MCP Drupal Database Server with stdio transport.
INFO:mcp_drupal_server:Registered tool: drupal_database_query
... (other tools) ...
INFO:mcp_drupal_server:Server is ready to accept connections from MCP clients via stdio.
```

## Connecting an MCP Client (e.g., Claude, Cursor)

**Important Note on Usage Context:** This MCP server is primarily designed and recommended for **local development environments** or for use within a **secure, trusted private network**.

The primary method for connecting this server to an MCP client (like Windsurf, Claude for Desktop, Cursor) is by having the **client manage the server process directly using `stdio` transport.**

**Client-Managed Server via STDIO (Recommended):**

In this setup, the MCP client is responsible for launching and managing the lifecycle of the `mcp_drupal_server.py` process.

*Example Client Configuration (for a client that manages the server process, e.g., in a `claude_desktop_config.json` or similar):*

```json
{
  "mcpServers": {
    "drupal_db_server": { // A unique name for your server instance
      "description": "MCP Drupal Database Server",
      "command": "/full/path/to/your/venv/bin/python", // Or just "python" if venv is active for client
      "args": [
        "/full/path/to/your/mcp-drupal-database-server/mcp_drupal_server.py",
        "--settings_file", "/full/path/to/your/drupal/sites/default/settings.php"
        // Or other args like --db-host, --db-port
      ],
      "env": {
        // Example: Set the settings file via environment variable
        // "MCP_DRUPAL_SETTINGS_FILE": "/full/path/to/your/drupal/sites/default/settings.php"
      },
      "enabled": true
    }
  }
}
```

**Key considerations for client-managed servers (stdio):**
*   **Absolute Paths:** The `command` path to Python/script and the `--settings_file` path in `args` (if used) should ideally be absolute paths to ensure the client can find them.
*   **Environment Variables (`env`):** You can pass necessary configurations like `MCP_DRUPAL_SETTINGS_FILE` via the `env` block in the client's server configuration. This is often cleaner than long `args` lists.
*   **No Host/Port:** The client does not need to specify host/port for the MCP server itself, as communication is over stdio.

**Connecting to an Independently Running HTTP Server (Less Common for Desktop Clients):**

While the server is now primarily `stdio`-based, if you were to modify `mcp_drupal_server.py` to support HTTP again (e.g., by re-adding host/port arguments and `run_streamable_http_async`), you could connect to it as a networked service. This is generally not the standard method for local desktop clients which prefer managing the process via `stdio`.

For specific instructions on how to configure client-managed servers, always refer to the documentation of your particular MCP client application.

## MCP Tool: `drupal_database_query`

The server exposes one primary tool that MCP clients can use:

**Tool Name:** `drupal_database_query`

**Description:** Interact with a Drupal database. Can list tables, get table schema, and execute read-only SQL queries.

**Parameters:**

*   `action` (string, required): The action to perform. Must be one of:
    *   `'list_tables'`: Lists all tables in the connected database.
    *   `'get_table_schema'`: Retrieves the schema (columns and types) for a specified table.
    *   `'execute_sql'`: Executes a read-only SQL query.
*   `table_name` (string, optional): The name of the table (without Drupal prefix) for the `'get_table_schema'` action.
*   `sql_query` (string, optional): The read-only SQL query (must start with `SELECT`) to execute for the `'execute_sql'` action. You may need to use Drupal's table prefixes if your query involves them (e.g., `SELECT * FROM {node_field_data} LIMIT 10`). The server itself uses the prefix from `settings.php` for its internal schema operations but not for user-provided queries.
*   `query_params` (array, optional): A list of parameters to be used with placeholders (e.g., `%s`) in the `sql_query` for the `'execute_sql'` action. This helps prevent SQL injection.

**Example MCP Client Interaction (Conceptual):**

A client like Claude or Cursor, if configured to use this MCP server, might make a request like this (represented conceptually in JSON):

*To list tables:*
```json
{
  "tool_name": "drupal_database_query",
  "parameters": {
    "action": "list_tables"
  }
}
```

*To get schema for a table named 'users_field_data':*
```json
{
  "tool_name": "drupal_database_query",
  "parameters": {
    "action": "get_table_schema",
    "table_name": "users_field_data" 
  }
}
```

*To execute a specific SQL query:*
```json
{
  "tool_name": "drupal_database_query",
  "parameters": {
    "action": "execute_sql",
    "sql_query": "SELECT nid, title FROM {node_field_data} WHERE status = %s AND type = %s LIMIT %s",
    "query_params": [1, "page", 10]
  }
}
```

**Response:**

The tool will return a JSON response containing the requested information or an error message.

## New Drupal-Specific Tools

In addition to the general `drupal_database_query` tool, the server now offers more specific, higher-level tools for common Drupal entities:

**1. List Content Types**
*   **Tool Name:** `drupal_list_content_types`
*   **Description:** Lists all available Drupal content types (node types).
*   **Parameters:** None
*   **Returns:** A JSON list of content type objects, each containing `type`, `name`, and `description`.

**2. Get Node by ID**
*   **Tool Name:** `drupal_get_node_by_id`
*   **Description:** Fetches detailed information for a specific Drupal node by its ID.
*   **Parameters:**
    *   `nid` (integer, required): The Node ID.
*   **Returns:** A JSON object containing node data (e.g., `nid`, `vid`, `type`, `title`, `status`, `created`, `changed`, `uid`, `author_name`, `body_value`, etc.) or an error if not found.

**3. List Vocabularies**
*   **Tool Name:** `drupal_list_vocabularies`
*   **Description:** Lists all taxonomy vocabularies in Drupal.
*   **Parameters:** None
*   **Returns:** A JSON list of vocabulary objects, each containing `vid`, `name`, and `description`.

**4. Get Taxonomy Term by ID**
*   **Tool Name:** `drupal_get_taxonomy_term_by_id`
*   **Description:** Fetches detailed information for a specific taxonomy term by its ID.
*   **Parameters:**
    *   `tid` (integer, required): The Taxonomy Term ID.
*   **Returns:** A JSON object containing term data (e.g., `tid`, `vid`, `name`, `description`, `vocabulary_name`) or an error if not found.

**5. Get User by ID**
*   **Tool Name:** `drupal_get_user_by_id`
*   **Description:** Fetches detailed information for a specific Drupal user by their ID.
*   **Parameters:**
    *   `uid` (integer, required): The User ID.
*   **Returns:** A JSON object containing user data (e.g., `uid`, `name`, `mail`, `status`, `created`, `changed`, `roles`) or an error if not found.

**6. List Paragraphs by Node ID**
*   **Tool Name:** `drupal_list_paragraphs_by_node_id`
*   **Description:** Lists paragraph items referenced by a specific node through a given paragraph field. The query attempts to find paragraphs based on common Drupal conventions for paragraph field naming (e.g., `node__field_machine_name` for the reference field on the node, and `paragraphs_item_field_data` for the main paragraph data). 
*   **Parameters:**
    *   `nid` (integer, required): The Node ID that contains the paragraphs.
    *   `paragraph_field_name` (string, required): The machine name of the paragraph reference field on the node (e.g., `field_body_paragraphs`, `field_components`).
*   **Returns:** A JSON list of paragraph objects, each containing details like `paragraph_id`, `paragraph_revision_id`, `paragraph_type`, etc., or an error. *Note: The reliability of this tool depends heavily on the specific paragraph setup and field naming conventions of the target Drupal site. It might require adjustments in `db_manager.py` for complex or non-standard configurations.*

**Example MCP Client Interaction (Conceptual for new tools):**

*To list content types:*
```json
{
  "tool_name": "drupal_list_content_types",
  "parameters": {}
}
```

*To get node with ID 123:*
```json
{
  "tool_name": "drupal_get_node_by_id",
  "parameters": {
    "nid": 123
  }
}
```

## Security Considerations

*   **`settings.php` Path**: Ensure the path to `settings.php` is correct and the file is readable by the user running the server.
*   **Database Accessibility**: The machine running this MCP server must have network access to the Drupal database.
*   **Read-Only SQL**: The `execute_sql` action currently enforces that queries start with `SELECT`. This is a basic safeguard. For production environments, consider more robust SQL parsing, query whitelisting, or further restricting query capabilities to prevent any form of data exfiltration or denial-of-service attacks.
*   **MCP Server Exposure**: By default, the server runs on `127.0.0.1` (localhost). If you expose it to a wider network (by changing `--host` to `0.0.0.0`), ensure appropriate firewall rules and consider adding authentication/authorization to the MCP server itself if the `model-context-protocol` SDK supports it or by proxying it through a web server that can add such layers.
*   **Error Handling**: The server logs errors and attempts to return informative error messages to the MCP client. Review server logs for any issues.

## Development & Testing

*   The `drupal_settings_parser.py` and `db_manager.py` modules include `if __name__ == '__main__':` blocks with basic tests. You may need to adjust dummy database credentials or have test database instances running to fully execute these tests.
*   To test the MCP server, you would typically use an MCP client. The MCP documentation or SDKs might provide example clients or tools for testing server integrations.

## TODO / Potential Enhancements

*   More sophisticated SQL validation/sanitization for the `execute_sql` tool.
*   Drupal-specific tools (e.g., `get_node_by_id(nid)`, `list_content_types()`) that abstract away raw SQL.
*   Support for more complex `settings.php` configurations (e.g., multiple database connections, more complex prefix arrays).
*   Authentication/Authorization layer for the MCP server.
*   Packaging as a Docker container for easier deployment.

**Note on the client configuration:**

*   `mcp_drupal_db_server`: This is a unique name you assign to this server instance within your MCP client's configuration.
*   `command`: **IMPORTANT!** This should be the absolute path to your `run_server.sh` script.
*   `args`: You must provide the `"--settings_file"` argument with the absolute path to your Drupal `settings.php` file.
    *   Optional: You can also specify `"--host"` (e.g., `"127.0.0.1"`) and `"--port"` (e.g., `"6789"`) if you don't want to use the defaults defined in `run_server.sh`.
*   `cwd` (Current Working Directory): This is usually not needed if `run_server.sh` is self-contained or uses absolute paths for its operations and for the `--settings_file` argument.
*   Port Handling: When the client manages the server, the MCP client and server SDKs often handle port negotiation automatically. The server might also output its port to stdout for the client to read.
*   You can add other server definitions in the `mcpServers` object if your client supports multiple managed servers (e.g., your Firebase server). 