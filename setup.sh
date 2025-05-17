#!/bin/bash
# setup.sh - Script to set up the MCP Drupal Database Server environment

# Exit immediately if a command exits with a non-zero status.
set -e

PYTHON_VERSION_MIN_MAJOR=3
PYTHON_VERSION_MIN_MINOR=10
PYTHON_VERSION_MIN_PATCH=0

# Store the selected Python command
PYTHON_CMD=""

# Function to check Python version
# Arguments: $1 = python command to test
check_python_version_and_set_cmd() {
    local python_to_test=$1
    if ! command -v "$python_to_test" &> /dev/null; then
        # echo "Debug: $python_to_test not found." # Optional debug
        return 1 # Command not found
    fi

    PYTHON_VERSION_FULL_STR=$("$python_to_test" --version 2>&1 | awk '{print $2}')
    # echo "Debug: $python_to_test version is $PYTHON_VERSION_FULL_STR" # Optional debug

    # Extract major, minor, patch for current version
    IFS='.' read -r py_major py_minor py_patch <<< "$PYTHON_VERSION_FULL_STR"
    py_major=${py_major:-0}
    py_minor=${py_minor:-0}
    py_patch=${py_patch:-0}

    # Perform numeric comparison
    if [ "$py_major" -lt "$PYTHON_VERSION_MIN_MAJOR" ]; then
        return 1
    elif [ "$py_major" -eq "$PYTHON_VERSION_MIN_MAJOR" ]; then
        if [ "$py_minor" -lt "$PYTHON_VERSION_MIN_MINOR" ]; then
            return 1
        elif [ "$py_minor" -eq "$PYTHON_VERSION_MIN_MINOR" ]; then
            if [ "$py_patch" -lt "$PYTHON_VERSION_MIN_PATCH" ]; then
                return 1
            fi
        fi
    fi
    
    # If we reach here, the version is sufficient
    PYTHON_CMD="$python_to_test"
    echo "Python version check passed: Using $PYTHON_CMD (version $PYTHON_VERSION_FULL_STR)"
    return 0 # Success
}

echo "Searching for a suitable Python version (>= ${PYTHON_VERSION_MIN_MAJOR}.${PYTHON_VERSION_MIN_MINOR}.${PYTHON_VERSION_MIN_PATCH})..."

# List of Python commands to try, in order of preference
PYTHON_COMMANDS_TO_TRY=("python3.12" "python3.11" "python3.10" "python3")

for cmd in "${PYTHON_COMMANDS_TO_TRY[@]}"; do
    if check_python_version_and_set_cmd "$cmd"; then
        break # Found a suitable Python, exit loop
    fi
done

# If PYTHON_CMD is still empty, no suitable Python was found
if [ -z "$PYTHON_CMD" ]; then
    echo "Error: Python ${PYTHON_VERSION_MIN_MAJOR}.${PYTHON_VERSION_MIN_MINOR}.${PYTHON_VERSION_MIN_PATCH} or higher is required." >&2
    echo "Please install a compatible Python version and ensure it's in your PATH." >&2
    # Try to get version from default python3 if it exists, for a more informative error
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION_FALLBACK_STR=$(python3 --version 2>&1 | awk '{print $2}')
        echo "Your default 'python3' is version ${PYTHON_VERSION_FALLBACK_STR}." >&2
    else
        echo "No 'python3' command was found in your PATH." >&2
    fi
    exit 1
fi

VENV_DIR="venv"

# Check Python version (using the determined PYTHON_CMD) - This is now more of a confirmation
# The check_python_version_and_set_cmd already did the core logic.
echo "Selected Python command for virtual environment: $PYTHON_CMD"


echo "Creating virtual environment in ./${VENV_DIR} using $PYTHON_CMD..."
if [ -d "${VENV_DIR}" ]; then
    echo "Virtual environment '${VENV_DIR}' already exists. Checking its Python version..."
    # Attempt to get Python version from venv
    # This is a bit tricky as 'source' then 'python --version' is needed
    # For simplicity, we'll rely on the fact that if it exists, it was likely created by a previous successful run.
    # A more robust check would involve activating and checking, or removing and recreating.
    # For now, if it exists, we assume it's okay or the user can delete it if issues arise.
    echo "Skipping creation. If you face issues, please remove the '${VENV_DIR}' directory and re-run setup."
else
    "$PYTHON_CMD" -m venv "${VENV_DIR}"
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
    echo "Please activate the virtual environment manually and then run:" >&2
    echo "pip install --upgrade pip" >&2
    echo "pip install -r requirements.txt" >&2
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
echo "The virtual environment was created/updated using $PYTHON_CMD."
echo "To run the server, you can now use the ./run_server.sh script (after making it executable),"
echo "or manually run: python mcp_drupal_server.py --settings_file /path/to/your/settings.php"
echo "Ensure your virtual environment ('${VENV_DIR}') is active if running manually." 