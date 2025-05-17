#!/bin/bash
# run_server.sh - Script to run the MCP Drupal Database Server

# Exit immediately if a command exits with a non-zero status.
set -e

VENV_DIR="venv"
SETTINGS_FILE_PATH=""
SERVER_HOST="127.0.0.1"
SERVER_PORT="6789"

# Function to print usage
usage() {
    echo "Usage: $0 --settings_file /path/to/your/drupal/sites/default/settings.php [OPTIONS]"
    echo ""
    echo "Required arguments:"
    echo "  --settings_file FILE_PATH   Path to the Drupal settings.php file."
    echo ""
    echo "Optional arguments:"
    echo "  --host HOST                 Host for the MCP server (default: ${SERVER_HOST})"
    echo "  --port PORT                 Port for the MCP server (default: ${SERVER_PORT})"
    echo "  --help                      Display this help message and exit."
    echo ""
    echo "Example:"
    echo "  ./run_server.sh --settings_file /var/www/html/sites/default/settings.php"
    echo "  ./run_server.sh --settings_file ../drupal/settings.php --host 0.0.0.0 --port 7000"
    exit 1
}

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case "$1" in
        --settings_file)
            SETTINGS_FILE_PATH="$2"
            shift 2
            ;;
        --host)
            SERVER_HOST="$2"
            shift 2
            ;;
        --port)
            SERVER_PORT="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Check if settings_file is provided; if not, prompt interactively
if [ -z "${SETTINGS_FILE_PATH}" ]; then
    # Check if --help was the only argument or if no arguments were given.
    # The 'usage' function exits, so if we are here, --help wasn't the sole cause of exit.
    # However, we want to avoid prompting if --help was an arg among others but settings_file was missing.
    # The original script structure handles '--help' by calling 'usage' which exits.
    # So, if we reach here and SETTINGS_FILE_PATH is empty, it means --settings_file was truly missing.

    echo "The --settings_file argument is required."
    read -r -p "Please enter the path to your Drupal settings.php file: " SETTINGS_FILE_PATH

    if [ -z "${SETTINGS_FILE_PATH}" ]; then
        echo "Error: No path provided for settings.php. Exiting." >&2
        exit 1
    fi
fi

# Check if settings_file exists
if [ ! -f "${SETTINGS_FILE_PATH}" ]; then
    echo "Error: Settings file not found at '${SETTINGS_FILE_PATH}'" >&2
    exit 1
fi

# Activate virtual environment if it exists and is not already active
if [ -d "${VENV_DIR}" ] && [ -z "${VIRTUAL_ENV}" ]; then
    echo "Activating virtual environment from ./${VENV_DIR}..."
    # shellcheck disable=SC1091
    source "${VENV_DIR}/bin/activate"
    echo "Virtual environment activated."
elif [ -d "${VENV_DIR}" ] && [ -n "${VIRTUAL_ENV}" ]; then
    echo "Virtual environment already active: ${VIRTUAL_ENV}"
else
    echo "Warning: Virtual environment directory '${VENV_DIR}' not found." 
    echo "Please ensure dependencies are installed globally or run ./setup.sh first."
fi

# Check if the main server script exists
if [ ! -f "mcp_drupal_server.py" ]; then
    echo "Error: mcp_drupal_server.py not found in the current directory." >&2
    echo "Please ensure you are in the correct project directory." >&2
    exit 1
fi

echo "Starting MCP Drupal Database Server..."
echo "  Settings file: ${SETTINGS_FILE_PATH}"
echo "  Host: ${SERVER_HOST}"
echo "  Port: ${SERVER_PORT}"

# Run the server
python mcp_drupal_server.py --settings_file "${SETTINGS_FILE_PATH}" --host "${SERVER_HOST}" --port "${SERVER_PORT}"

echo "Server stopped." 