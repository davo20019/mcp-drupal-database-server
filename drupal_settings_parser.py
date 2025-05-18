import re
import logging

logger = logging.getLogger(__name__)

def parse_php_value(value_str: str, variables: dict) -> any:
    """Converts a PHP value string (possibly a variable) to a Python type."""
    value_str = value_str.strip()
    # Check if it's a string literal
    if (value_str.startswith("'") and value_str.endswith("'")) or \
       (value_str.startswith('"') and value_str.endswith('"')):
        return value_str[1:-1]
    # Check if it's a known variable
    if value_str.startswith("$") and value_str[1:] in variables:
        return variables[value_str[1:]]
    # Check if it's a number
    if value_str.isdigit():
        return int(value_str)
    # Check for boolean/null literals (case-insensitive)
    if value_str.lower() == 'true':
        return True
    if value_str.lower() == 'false':
        return False
    if value_str.lower() == 'null':
        return None
    # Otherwise, it might be an unquoted string or constant we don't handle
    # For this parser, we'll assume it's a string if not a recognized variable or number.
    # This might need refinement for other PHP constants.
    logger.debug(f"Unrecognized PHP value type for '{value_str}', treating as raw string or unparsed variable.")
    return value_str # Fallback: return as is, or could raise error/return None

def parse_settings_php(file_path: str) -> dict | None:
    """
    Parses a Drupal settings.php file to extract database connection details.
    Handles both direct array assignment and individual key assignments for $databases['default']['default'].
    Also handles simple variable substitutions for host, port, driver.

    Args:
        file_path: The absolute path to the settings.php file.

    Returns:
        A dictionary containing database credentials ('driver', 'database', 
        'username', 'password', 'host', 'port', 'prefix') 
        or None if parsing fails or the file is not found.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        logger.error(f"Error: settings.php file not found at {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error reading settings.php file at {file_path}: {e}")
        return None

    config = {}
    variables = {}

    # 1. Parse simple variable assignments like $host = "db";
    var_pattern = re.compile(r"^\s*\$(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?P<value>[^;]+);", re.MULTILINE)
    for match in var_pattern.finditer(content):
        var_name = match.group("name")
        raw_value = match.group("value").strip()
        # For variables, we only parse simple string or numeric literals directly for now
        if (raw_value.startswith("'") and raw_value.endswith("'")) or \
           (raw_value.startswith('"') and raw_value.endswith('"')):
            variables[var_name] = raw_value[1:-1]
        elif raw_value.isdigit():
            variables[var_name] = int(raw_value)
        else:
            # Could be a more complex expression or constant, log and skip for now for simplicity
            logger.debug(f"Skipping complex variable assignment for ${var_name}: {raw_value}")
    
    logger.debug(f"Parsed variables: {variables} from {file_path}")

    # 2. Try to find the full $databases['default']['default'] array assignment first
    db_settings_match = re.search(
        r"\$databases\s*\[\s*['\"]default['\"]\s*\]\s*\[\s*['\"]default['\"]\s*\]\s*=\s*(array\(|\[)(.*?)(\)|])\s*;",
        content,
        re.DOTALL | re.IGNORECASE
    )

    if db_settings_match:
        logger.info(f"Found $databases['default']['default'] as a full array in {file_path}")
        settings_str = db_settings_match.group(2) # Content within the array(...) or [...]
        
        # Regex for key-value pairs within the array string
        # Handles 'key' => 'value', "key" => "value", 'key' => $variable, 'key' => 123
        pair_pattern = re.compile(
            r"['\"](?P<key>\w+)['\"]\s*=>\s*(?P<value>[^,)\]]+)"
        )
        for match in pair_pattern.finditer(settings_str):
            key = match.group('key')
            val_str = match.group('value').strip()
            if key in ['driver', 'database', 'username', 'password', 'host', 'port', 'prefix']:
                config[key] = parse_php_value(val_str, variables)
    else:
        logger.info(f"Did not find full array assignment for $databases['default']['default'] in {file_path}. "
                       "Attempting to parse individual assignments.")
        # 3. If full array not found, try to parse individual assignments
        # $databases['default']['default']['key'] = 'value';
        # $databases['default']['default']['key'] = $variable;
        individual_assignment_pattern = re.compile(
            r"\$databases\s*\[\s*['\"]default['\"]\s*\]\s*\[\s*['\"]default['\"]\s*\]\s*\[\s*['\"](?P<key>\w+)['\"]\s*\]\s*=\s*(?P<value>[^;]+);",
            re.IGNORECASE
        )
        for match in individual_assignment_pattern.finditer(content):
            key = match.group('key')
            val_str = match.group('value').strip()
            if key in ['driver', 'database', 'username', 'password', 'host', 'port', 'prefix']:
                config[key] = parse_php_value(val_str, variables)
                logger.debug(f"Parsed individual assignment: config['{key}'] = {config[key]}")


    if not config:
        logger.warning(f"Could not extract any database settings from {file_path}. "
                       "Ensure $databases['default']['default'] is defined using a supported format.")
        return None
            
    # Ensure essential keys are present
    required_keys = ['driver', 'database', 'username', 'password', 'host']
    missing_keys = [key for key in required_keys if key not in config or config[key] is None]
    
    if missing_keys:
        logger.warning(f"Missing some required database configuration keys in {file_path} after parsing. "
                       f"Found: {config}. Missing: {missing_keys}")
        # Attempt to provide defaults if reasonable (e.g. port)
        if 'port' not in config or config.get('port') is None:
            if config.get('driver') == 'mysql':
                config['port'] = '3306' # Default as string, will be int-converted later
            elif config.get('driver') == 'pgsql':
                config['port'] = '5432'
            if 'port' in missing_keys and 'port' in config : # If port was missing and now set
                 missing_keys.remove('port')


        if missing_keys: # Re-check after potential defaults
             logger.error(f"Still missing required keys {missing_keys} after attempting to set defaults. Cannot proceed.")
             return None

    if 'prefix' not in config:
        config['prefix'] = '' 
        if db_settings_match: # Only try complex prefix parsing if we had a full array
            settings_str = db_settings_match.group(2)
            prefix_array_match = re.search(r"['\"]prefix['\"]\s*=>\s*(array\(|\[)(.*?)(\)|])", settings_str, re.DOTALL | re.IGNORECASE)
            if prefix_array_match:
                logger.warning("An array table prefix is defined in settings.php. This parser only supports a single string prefix. Operations might be affected for tables using non-default prefixes.")
                default_prefix_match = re.search(r"['\"]default['\"]\s*=>\s*['\"]([^'\"]*)['\"]", prefix_array_match.group(2))
                if default_prefix_match:
                    config['prefix'] = default_prefix_match.group(1)
                else:
                     config['prefix'] = ''
                     logger.warning("Could not determine a 'default' table prefix from the prefix array. Using empty prefix.")
    
    # Convert port to int if it was parsed as a string or int
    if 'port' in config:
        try:
            config['port'] = int(str(config['port'])) # Ensure it's string first then int
        except ValueError:
            logger.error(f"Invalid port number: {config['port']}. It must be an integer.")
            return None
            
    logger.info(f"Successfully parsed database configuration from {file_path}: {config}")
    return config

if __name__ == '__main__':
    # Example usage:
    logging.basicConfig(level=logging.DEBUG) # Use DEBUG for testing parser

    # Test DDEV-style settings
    ddev_style_content = """<?php
$host = "db_from_var";
$port = 3306; // numeric
$driver = "mysql_from_var";

$databases['default']['default']['database'] = "db_ddev";
$databases['default']['default']['username'] = "user_ddev";
$databases['default']['default']['password'] = "pass_ddev";
$databases['default']['default']['host'] = $host;
$databases['default']['default']['port'] = $port;
$databases['default']['default']['driver'] = $driver;
$databases['default']['default']['prefix'] = ""; // empty prefix
"""
    test_file_ddev = "dummy_ddev_settings.php"
    with open(test_file_ddev, "w", encoding="utf-8") as f:
        f.write(ddev_style_content)
    
    print("\n--- Testing DDEV Style ---")
    parsed_config_ddev = parse_settings_php(test_file_ddev)
    if parsed_config_ddev:
        print("Parsed DDEV Config:", parsed_config_ddev)
        assert parsed_config_ddev['database'] == 'db_ddev'
        assert parsed_config_ddev['username'] == 'user_ddev'
        assert parsed_config_ddev['password'] == 'pass_ddev'
        assert parsed_config_ddev['host'] == 'db_from_var' # Substituted from variable
        assert parsed_config_ddev['port'] == 3306         # Substituted and int
        assert parsed_config_ddev['driver'] == 'mysql_from_var'
        assert parsed_config_ddev['prefix'] == ''


    # Test standard array style settings
    dummy_settings_content = """<?php
// ... some comments ...
$databases['default']['default'] = [
  'database' => 'drupal_db',
  'username' => 'drupal_user',
  'password' => 'secret_password',
  'prefix' => 'main_',
  'host' => 'localhost_array',
  'port' => '3307', // string port
  'namespace' => 'Drupal\\\\Core\\\\Database\\\\Driver\\\\mysql',
  'driver' => 'mysql_array',
];
"""
    test_file_array = "dummy_array_settings.php"
    with open(test_file_array, "w", encoding="utf-8") as f:
        f.write(dummy_settings_content)

    print("\n--- Testing Array Style ---")
    parsed_config_array = parse_settings_php(test_file_array)
    if parsed_config_array:
        print("Parsed Array Config:", parsed_config_array)
        assert parsed_config_array['database'] == 'drupal_db'
        assert parsed_config_array['username'] == 'drupal_user'
        assert parsed_config_array['password'] == 'secret_password'
        assert parsed_config_array['host'] == 'localhost_array'
        assert parsed_config_array['port'] == 3307
        assert parsed_config_array['driver'] == 'mysql_array'
        assert parsed_config_array['prefix'] == 'main_'


    # Test file not found
    print("\n--- Testing non-existent file ---")
    parse_settings_php("non_existent_settings.php")

    # Clean up dummy files
    import os
    os.remove(test_file_ddev)
    os.remove(test_file_array)
    print("\nCleaned up dummy files.") 