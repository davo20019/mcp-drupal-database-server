#!/bin/bash
# setup.sh - Script to set up the MCP Drupal Database Server environment

# Exit immediately if a command exits with a non-zero status.
set -e

PYTHON_VERSION_MIN="3.10"

# Function to check Python version
check_python_version() {
    if ! command -v python3 &> /dev/null; then
        echo "Error: python3 is not installed. Please install Python ${PYTHON_VERSION_MIN} or higher." >&2
        exit 1
    fi

    PYTHON_VERSION_FULL_STR=$(python3 --version 2>&1 | awk '{print $2}') # e.g., "3.12.10" or "3.9.5"

    # Extract major, minor, patch for current version
    IFS='.' read -r py_major py_minor py_patch <<< "$PYTHON_VERSION_FULL_STR"
    py_patch=${py_patch:-0} # Default patch to 0 if not present

    # Extract major, minor, patch for minimum required version
    IFS='.' read -r min_major min_minor min_patch <<< "$PYTHON_VERSION_MIN"
    min_patch=${min_patch:-0} # Default patch to 0 if not present

    # Perform numeric comparison
    if [ "$py_major" -lt "$min_major" ]; then
        VERSION_CHECK_FAILED=true
    elif [ "$py_major" -eq "$min_major" ]; then
        if [ "$py_minor" -lt "$min_minor" ]; then
            VERSION_CHECK_FAILED=true
        elif [ "$py_minor" -eq "$min_minor" ]; then
            if [ "$py_patch" -lt "$min_patch" ]; then
                VERSION_CHECK_FAILED=true
            fi
        fi
    fi

    if [ "${VERSION_CHECK_FAILED:-false}" = true ]; then
        echo "Error: Python version ${PYTHON_VERSION_MIN} or higher is required. You have Python ${PYTHON_VERSION_FULL_STR}." >&2
        exit 1
    fi
    
    echo "Python version check passed: Python ${PYTHON_VERSION_FULL_STR}"
}

VENV_DIR="venv"

# Check Python version before proceeding
check_python_version

echo "Creating virtual environment in ./${VENV_DIR}..."
if [ -d "${VENV_DIR}" ]; then
    echo "Virtual environment '${VENV_DIR}' already exists. Skipping creation."
else
    python3 -m venv "${VENV_DIR}"
    echo "Virtual environment created."
fi

# Activate virtual environment
# The way to activate can differ slightly based on the shell.
# This script assumes a bash-like shell for sourcing.
if [ -f "${VENV_DIR}/bin/activate" ]; then
    echo "Activating virtual environment..."
    # shellcheck disable=SC1091
    source "${VENV_DIR}/bin/activate"
    echo "Virtual environment activated. (Run 'deactivate' to exit)"

    echo "Upgrading pip in virtual environment..."
    pip install --upgrade pip
    echo "pip upgraded."
else
    echo "Error: Could not find activation script at ${VENV_DIR}/bin/activate." >&2
    echo "Please activate the virtual environment manually." >&2
    exit 1
fi

# Install dependencies
if [ -f "requirements.txt" ]; then
    echo "Installing dependencies from requirements.txt..."
    pip install -r requirements.txt
    echo "Dependencies installed successfully."
else
    echo "Error: requirements.txt not found." >&2
    exit 1
fi

echo ""
echo "Setup complete!"
echo "To run the server, you can now use the ./run_server.sh script (after making it executable),"
echo "or manually run: python mcp_drupal_server.py --settings_file /path/to/your/settings.php"
echo "Ensure your virtual environment ('${VENV_DIR}') is active if running manually." 