import re
import logging

logger = logging.getLogger(__name__)

def parse_settings_php(file_path: str) -> dict | None:
    """
    Parses a Drupal settings.php file to extract database connection details.

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

    # More robust regex to find the $databases['default']['default'] array
    # This regex looks for the assignment and captures the content within the array definition.
    # It handles single and double quotes for keys and values.
    # It's designed to be non-greedy and capture the innermost relevant array.
    db_settings_match = re.search(
        r"\$databases\s*\[\s*['\"]default['\"]\s*\]\s*\[\s*['\"]default['\"]\s*\]\s*=\s*(array\(|\[)(.*?)(\)|\])\s*;",
        content,
        re.DOTALL | re.IGNORECASE
    )

    if not db_settings_match:
        logger.warning(f"Could not find the default database settings in {file_path}. "
                       "Ensure \$databases['default']['default'] is defined.")
        return None

    settings_str = db_settings_match.group(2) # Content within the array(...) or [...]

    config = {}
    # Regex to find key-value pairs within the captured settings string.
    # Handles 'key' => 'value', "key" => "value", 'key' => "value", "key" => 'value'
    # Also handles numeric values for port if not quoted.
    # Improved to handle comments and ensure correct pairing.
    pattern = re.compile(
        r"['\"](?P<key>\w+)['\"]\s*=>\s*(?:['\"](?P<value_str>[^'\"]*)['\"]|(?P<value_num>\d+))"
    )
    
    for match in pattern.finditer(settings_str):
        key = match.group('key')
        value = match.group('value_str') if match.group('value_str') is not None else match.group('value_num')
        if key in ['driver', 'database', 'username', 'password', 'host', 'port', 'prefix']:
            config[key] = value
            
    # Ensure essential keys are present
    required_keys = ['driver', 'database', 'username', 'password', 'host']
    if not all(key in config for key in required_keys):
        logger.warning(f"Missing some required database configuration keys in {file_path}. "
                       f"Found: {config.keys()}. Required: {required_keys}")
        # Attempt to provide defaults if reasonable (e.g. port)
        if 'port' not in config:
            if config.get('driver') == 'mysql':
                config['port'] = '3306'
            elif config.get('driver') == 'pgsql':
                config['port'] = '5432'
        # Re-check after potential defaults
        if not all(key in config for key in required_keys):
             logger.error(f"Still missing required keys after attempting to set defaults. Cannot proceed.")
             return None


    if 'prefix' not in config: # Prefix can be an empty string or an array. We handle simple string prefix.
        config['prefix'] = '' 
        # If prefix is an array (e.g. for different table types), this simple parser will not capture it correctly.
        # Advanced parsing for array prefixes might be needed for complex Drupal setups.
        prefix_array_match = re.search(r"['\"]prefix['\"]\s*=>\s*(array\(|\[)(.*?)(\)|\])", settings_str, re.DOTALL | re.IGNORECASE)
        if prefix_array_match:
            logger.warning("An array table prefix is defined in settings.php. This parser only supports a single string prefix. Operations might be affected for tables using non-default prefixes.")
            # Try to get the 'default' prefix if it's an array
            default_prefix_match = re.search(r"['\"]default['\"]\s*=>\s*['\"]([^'\"]*)['\"]", prefix_array_match.group(2))
            if default_prefix_match:
                config['prefix'] = default_prefix_match.group(1)
            else: # If no 'default' key, we cannot reliably get a single prefix.
                 config['prefix'] = '' # Fallback to empty
                 logger.warning("Could not determine a 'default' table prefix from the prefix array. Using empty prefix.")


    # Convert port to int if it was parsed as a string
    if 'port' in config and isinstance(config['port'], str):
        try:
            config['port'] = int(config['port'])
        except ValueError:
            logger.error(f"Invalid port number: {config['port']}. It must be an integer.")
            return None
            
    logger.info(f"Successfully parsed database configuration from {file_path}")
    return config

if __name__ == '__main__':
    # Example usage:
    # Create a dummy settings.php for testing
    dummy_settings_content = """<?php
// ... some comments ...
$databases['default']['default'] = [
  'database' => 'drupal_db',
  'username' => 'drupal_user',
  'password' => 'secret_password',
  'prefix' => '',
  'host' => 'localhost',
  'port' => '3306',
  'namespace' => 'Drupal\\Core\\Database\\Driver\\mysql',
  'driver' => 'mysql',
];

// Test with single quotes and array() syntax
$databases['default']['another'] = array(
  'database' => 'another_db',
  'username' => 'another_user',
  'password' => 'another_pass',
  'host' => '127.0.0.1',
  'driver' => 'pgsql',
  'port' => 5432, // Numeric port
);

// Test with more complex prefix
$databases['default']['complex_prefix'] = [
  'database' => 'drupal_db_complex',
  'username' => 'user_complex',
  'password' => 'pass_complex',
  'prefix' => [
    'default'   => 'main_',
    'users'     => 'users_',
    'node'      => 'content_',
  ],
  'host' => 'db.example.com',
  'port' => '3307',
  'driver' => 'mysql',
];
"""
    test_file_path = "dummy_settings.php"
    with open(test_file_path, "w", encoding="utf-8") as f:
        f.write(dummy_settings_content)

    logging.basicConfig(level=logging.INFO)
    
    # Test with the standard structure
    parsed_config = parse_settings_php(test_file_path)
    if parsed_config:
        print("Parsed Config (standard):", parsed_config)
        assert parsed_config['database'] == 'drupal_db'
        assert parsed_config['username'] == 'drupal_user'
        assert parsed_config['password'] == 'secret_password'
        assert parsed_config['host'] == 'localhost'
        assert parsed_config['port'] == 3306
        assert parsed_config['driver'] == 'mysql'
        assert parsed_config['prefix'] == ''

    # To test the other configurations, you'd modify the dummy_settings_content
    # or have multiple dummy files and call parse_settings_php on each.
    # For example, to test 'another_db':
    # You'd need to change the $databases['default']['default'] line in the dummy file
    # to point to the 'another' configuration or temporarily rename 'another' to 'default'.
    # The current parser specifically looks for $databases['default']['default'].

    # Test complex prefix (current parser gets 'main_')
    dummy_settings_content_complex = dummy_settings_content.replace(
        "$databases['default']['default'] = [", 
        "$databases['default']['default'] = ["
    ).replace(
        """'database' => 'drupal_db',
  'username' => 'drupal_user',
  'password' => 'secret_password',
  'prefix' => '',
  'host' => 'localhost',
  'port' => '3306',
  'namespace' => 'Drupal\\\\Core\\\\Database\\\\Driver\\\\mysql',
  'driver' => 'mysql',
];""",
        """'database' => 'drupal_db_complex',
  'username' => 'user_complex',
  'password' => 'pass_complex',
  'prefix' => [
    'default'   => 'main_',
    'users'     => 'users_',
    'node'      => 'content_',
  ],
  'host' => 'db.example.com',
  'port' => '3307',
  'driver' => 'mysql',
];"""
    )
    test_file_path_complex = "dummy_settings_complex.php"
    with open(test_file_path_complex, "w", encoding="utf-8") as f:
        f.write(dummy_settings_content_complex)
    
    parsed_config_complex = parse_settings_php(test_file_path_complex)
    if parsed_config_complex:
        print("Parsed Config (complex prefix):", parsed_config_complex)
        assert parsed_config_complex['database'] == 'drupal_db_complex'
        assert parsed_config_complex['host'] == 'db.example.com'
        assert parsed_config_complex['port'] == 3307
        assert parsed_config_complex['prefix'] == 'main_' # Correctly extracts 'default' from prefix array

    # Test file not found
    print("\\nTesting non-existent file:")
    parse_settings_php("non_existent_settings.php")

    # Clean up dummy files
    import os
    os.remove(test_file_path)
    os.remove(test_file_path_complex)
    print("\\nCleaned up dummy files.") 