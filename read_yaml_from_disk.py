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
class SemanticConventions(object):
    def __init__(self, log=None):
        self.path_start = -1
        self.log = log
        self.reset()

    def reset(self):
        self.relations = dict(has_attribute={}, has_event={}, AssociatedWith={})
        self.nodes = dict(Metric={}, Entity={}, Span={}, AttributeGroup={}, Event={}, Attribute={})

    def nodetype2node(self, section: dict) -> str:
        """
        Map OpenTelemetry 'type' field to our Cypher node table name
        :return:
        """
        mappings = dict(metric='Metric', entity='Entity', span='Span', attribute_group='AttributeGroup', event='Event')
        node_type = section.get('type')
        return mappings.get(node_type)

    def from_dir(self, base_path: str):
        """
        Reads all YAML files in a given directory and its subdirectories using pathlib.

        Args:
            base_path (str): The root directory to start the search from.
        """
        start_path = Path(base_path)
        if not start_path.is_dir():
            self.log.error("Directory not found", extra=dict(path=start_path))
            return

        self.path_start = -1
        for file_path in start_path.rglob('*.y[a]ml'):
            try:
                content = file_path.read_text(encoding='utf-8')
                yaml_data = yaml.safe_load(content)
                if not isinstance(yaml_data, dict):
                    continue
                self.add_groups(yaml_data)
            except yaml.YAMLError as ex:
                self.log.error("Error parsing YAML file", extra=dict(file_path=file_path, error_message=ex))
            except Exception as ex:
                self.log.exception(ex, extra=dict(file_path=file_path))

    def add_groups(self, yaml_data):
        """
        Process the 'groups' entry from a YAML semantic convention

        :param yaml_data:
        :return:
        """
        for section in yaml_data.get('groups', []):
            node_type = self.nodetype2node(section)
            if node_type in self.nodes:
                key = section['id']
                del section['type']
                self.nodes[node_type][key] = section
                self.relate2attribute(node_type, key, section.get('attributes', []))
                self.relate2event(node_type, key, section.get('events', []))

                # Special processing
                if 'entity_associations' in section:
                    self.relate2associated_entity(node_type, key, section['entity_associations'])
                if node_type == 'AttributeGroup' and \
                        'display_name' not in section:
                    section['display_name'] = key
            else:
                self.log.error("Unknown semantic convention", extra=dict(data=section, node_type=node_type))

    def relate2attribute(self, node_type: str, node: str, attributes: list):
        rels = self.relations['has_attribute'].setdefault(node_type, [])
        for data in attributes:
            edge_info = {'from': node}
            if 'ref' in data:
                edge_info['to'] = data['ref']
                del data['ref']
                requirement = data.get('requirement_level')
                if isinstance(requirement, dict):
                    if 'conditionally_required' in requirement:
                        data['condition'] = requirement['conditionally_required']
                        data['requirement_level'] = 'conditionally_required'
                    if 'recommended' in requirement:
                        data['condition'] = requirement['recommended']
                        data['requirement_level'] = 'recommended'
                if 'examples' in data:
                    # Sometimes get numeric values in examples
                    data['examples'] = '\n'.join(str(x) for x in data['examples'])
                edge_info.update(data)
            else:
                attribute_name = data['id']
                del data['type']
                self.nodes['Attribute'][attribute_name] = data
                edge_info['to'] = attribute_name
            rels.append(edge_info)

    def relate2associated_entity(self, node_type: str, node: str, entities: list):
        rels = self.relations['AssociatedWith'].setdefault(node_type, [])
        for entity in entities:
            # Evil hack: supposed to search entity table by 'name' or by 'id', which can be different
            # Choose the id, assume that the difference is that the id name starts with 'entity.'
            if not entity.startswith('entity'):
                entity = 'entity.' + entity
            rels.append((node, entity))

    def relate2event(self, node_type: str, node: str, events: list):
        rels = self.relations['has_event'].setdefault(node_type, [])
        for event_name in events:
            rels.append((node, event_name))

    def add_attribute(self, attribute: dict):
        key = attribute['id']
        all_attributes = self.nodes['Attribute']
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


def save_rel_data_csv(rel_type: str, relations: list) -> str:
    filename = f'rel_{rel_type}.csv'
    with open(filename, 'w') as fd:
        writer = csv.writer(fd, dialect='unix')
        writer.writerows(relations)
    return filename


def save_rel_data_json(node_type: str, rel_type: str, relations: list) -> str:
    filename = f'rel_{node_type}_{rel_type}.json'
    with open(filename, 'w') as fd:
        json.dump(relations, fd)
    return filename


def save_node_data_json(node_type: str, nodes: list) -> str:
    filename = node_type + 's.json'
    with open(filename, 'w') as fd:
        json.dump(nodes, fd)
    return filename


def save_node_data_kuzu(conn: object, table: str, filename: str):
    name2type = dict(attribute_group='AttributeGroup', entity='Entity',
                     metric='Metric', event='Event', span='Span', attribute='Attribute')
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

    conventions = SemanticConventions(log)
    conventions.from_dir(START_PATH)

    conn = create_db_conn()
    for node_type, data in conventions.nodes.items():
        filename = save_node_data_json(node_type, list(data.values()))
        save_node_data_kuzu(conn, node_type, filename)

    # statement = 'MATCH (n: Attribute) RETURN n;'
    # result = conn.execute(statement)
    for node_type, relations in conventions.relations['has_attribute'].items():
        filename = save_rel_data_json(node_type, 'attribute', relations)
        statement = f"COPY HasAttribute FROM '{filename}' (from='{node_type}', to='Attribute')"
        try:
            conn.execute(statement)
        except Exception as ex:
            print(f"{ex} {statement}")
            import pdb;

            pdb.set_trace()

    for node_type, relations in conventions.relations['AssociatedWith'].items():
        filename = save_rel_data_csv(node_type + '_entity_association', relations)
        statement = f"COPY AssociatedWith FROM '{filename}' (from='{node_type}', to='Entity')"
        try:
            conn.execute(statement)
        except Exception as ex:
            print(f"{ex} {statement}")
