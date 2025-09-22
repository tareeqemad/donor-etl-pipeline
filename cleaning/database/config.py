import os
import sys
from configparser import ConfigParser


def get_resource_path(relative_path):
    """Get the path to a resource, whether running in a bundle or in development."""
    if hasattr(sys, "_MEIPASS"):
        # If running as a bundled executable, use this to find the resource
        return os.path.join(sys._MEIPASS, relative_path)
    else:
        # If running as a script, just use the relative path
        return os.path.join(os.path.dirname(__file__), relative_path)


def load_config(filename="database.ini", section="postgresql"):
    config_path = get_resource_path(filename)

    parser = ConfigParser()
    parser.read(config_path)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {filename} file")

    return config


if __name__ == "__main__":
    config = load_config()
    print(config)
