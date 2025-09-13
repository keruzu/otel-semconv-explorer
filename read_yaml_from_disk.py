import logging
from pathlib import Path
import yaml
from pythonjsonlogger.json import JsonFormatter

# --- Logging Configuration ---
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])

log = logging.getLogger()


# --- YAML Processing Function ---
class SemanticConventionReader(object):
    def __init__(self, log=None):
        self.path_start = -1
        self.log = log

    def from_dir(self, base_path: str) -> dict:
        """
        Reads all YAML files in a given directory and its subdirectories using pathlib.

        Args:
            base_path (str): The root directory to start the search from.
        """
        tree = {}
        start_path = Path(base_path)
        if not start_path.is_dir():
            self.log.error("Directory not found", extra=dict(path=start_path))
            return tree

        self.path_start = -1
        for file_path in start_path.rglob('*.y[a]ml'):
            try:
                content = file_path.read_text(encoding='utf-8')
                yaml_data = yaml.safe_load(content)
                if not isinstance(yaml_data, dict):
                    continue
                data = yaml_data.get('groups', {})
                if not data:
                    continue
                key_name = self.generate_key_name(data, file_path)
                if key_name in tree:
                    self.log.error('Key already exists',
                                   extra=dict(key_name=key_name, old_data=tree[key_name], new_data=data)
                                   )
                else:
                    tree[key_name] = data
                    self.log.debug("Successfully loaded YAML file",
                                   extra=dict(key_name=key_name, file_path=file_path, data=data,
                                              ))
            except yaml.YAMLError as ex:
                self.log.error("Error parsing YAML file", extra=dict(file_path=file_path, error_message=ex))
            except Exception as ex:
                self.log.exception(ex, extra=dict(file_path=file_path))
        return tree

    def generate_key_name(self, data: dict, file_path: Path) -> str:
        prefix = data[0].get('type', '_')
        if self.path_start == -1:
            model_dir = file_path.parts
            if 'model' in model_dir:
                self.path_start = model_dir.index('model') + 1
            else:
                self.path_start = 0
        final = file_path.stem
        parts = list(file_path.parts[self.path_start:-1])
        parts.insert(0, prefix)
        parts.append(final)
        key_name = '.'.join(parts)
        return key_name


if __name__ == '__main__':
    START_PATH = '../semantic-conventions/model'

    reader = SemanticConventionReader(log)
    conventions = reader.from_dir(START_PATH)
    import pdb

    pdb.set_trace()
