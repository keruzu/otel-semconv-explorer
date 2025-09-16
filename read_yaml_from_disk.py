import json
import csv
import logging
from pathlib import Path

import yaml
from pythonjsonlogger.json import JsonFormatter
import kuzu

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
        tree = dict(metric={}, entity={}, span={}, attribute_group={}, event={}, attribute={})
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
                groups = yaml_data.get('groups', {})
                if not groups:
                    continue
                for section in groups:
                    node_type = section.get('type')
                    if node_type in tree:
                        key = section['id']
                        del section['type']
                        tree[node_type][key] = section
                    else:
                        self.add_attribute(tree, section)
            except yaml.YAMLError as ex:
                self.log.error("Error parsing YAML file", extra=dict(file_path=file_path, error_message=ex))
            except Exception as ex:
                self.log.exception(ex, extra=dict(file_path=file_path))
        return tree

    def add_attribute(self, root: dict, attribute: dict):
        key = attribute['id']
        all_attributes = root['attribute']
        if key in all_attributes:
            self.log.error("Attribute key already exists -- skipping", extra=dict(
                attribute_name=key, existing_attribute=all_attributes[key],
                incoming_attribute=attribute
            ))
        else:
            all_attributes[key] = attribute

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


fieldnamesByType = dict(
    attribute_group=['id', 'metric_name', 'extends', 'entity_associations',
                     'annotations', 'stability', 'brief', 'deprecated',
                     'instrument', 'unit', 'note', 'attributes', 'name',
                     'span_kind', 'events', 'display_name', 'body'],
    attribute=['id', 'metric_name', 'extends', 'entity_associations',
               'annotations', 'stability', 'brief', 'deprecated',
               'instrument', 'unit', 'note', 'attributes', 'name',
               'span_kind', 'events', 'display_name', 'body'],
    span=['id', 'metric_name', 'extends', 'entity_associations',
          'annotations', 'stability', 'brief', 'deprecated',
          'instrument', 'unit', 'note', 'attributes', 'name',
          'span_kind', 'events', 'display_name', 'body'],
    event=['id', 'stability', 'brief', 'name', ],
    metric=['id', 'metric_name', 'stability', 'brief',
            'instrument', 'unit', ],

    entity=['id', 'stability', 'brief', ],
)


def save_node_data_csv(node_type: str, nodes: list, fieldnames: list) -> str:
    filename = node_type + 's.csv'
    with open(filename, 'w') as fd:
        writer = csv.DictWriter(fd, dialect='unix',
                                fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(nodes)
    return filename


def save_node_data_json(node_type: str, nodes: list) -> str:
    filename = node_type + 's.json'
    with open(filename, 'w') as fd:
        json.dump(nodes, fd)
    return filename


def save_node_data_kuzu(conn: object, node_type: str, filename: str):
    name2type = dict(attribute_group='AttributeGroup', entity='Entity',
                     metric='Metric', event='Event', span='Span', attribute='Attribute')
    table = name2type.get(node_type)
    statement = f"COPY {table} FROM '{filename}' (ignore_errors=true)"
    conn.execute(statement)


def create_db_conn(path: str = 'db/semantic_conventions.kuzu',
                   model_file: str = 'kuzu_data_model.cypher') -> object:
    db = kuzu.Database(path)
    conn = kuzu.Connection(db)
    if model_file:
        with open(model_file) as fd:
            model_data = fd.read()
        statements = model_data.split(';')
        for statement in statements:
            if statement.strip():
                conn.execute(statement)
    return conn


if __name__ == '__main__':
    START_PATH = '../semantic-conventions/model'

    reader = SemanticConventionReader(log)
    conventions = reader.from_dir(START_PATH)

    conn = create_db_conn()
    for node_type, data in conventions.items():
        filename = save_node_data_json(node_type, list(data.values()))
        save_node_data_kuzu(conn, node_type, filename)
