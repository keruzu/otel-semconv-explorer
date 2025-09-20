"""
Open Telemetry Semantic Conventions

* [Documentation](https://opentelemetry.io/docs/concepts/semantic-conventions/)
* [Definitions of the conventions](https://github.com/open-telemetry/semantic-conventions/tree/main/model)

"""
import json
import logging
from pathlib import Path

import yaml
from pythonjsonlogger.json import JsonFormatter
import kuzu

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])

log = logging.getLogger()


class SemanticConventions(object):
    """
    Open Telemetry Semantic Conventions
    Describes the nodes and relationships in the conventions
    """

    def __init__(self, log=None):
        self.path_start = -1
        self.log = log
        self.reset()

    def reset(self):
        """
        Reset all semantic convention data
        """
        # Use the Cypher table names as indices, so it's easier to relate things
        self.relations = dict(HasAttribute={}, HasEvent={}, AssociatedWith={})
        self.nodes = dict(Metric={}, Entity={}, Span={}, AttributeGroup={}, Event={}, Attribute={})

    def nodetype2node(self, section: dict) -> str:
        """
        Map OpenTelemetry 'type' field to our Cypher node table name

        :return: table name
        """
        mappings = dict(metric='Metric', entity='Entity', span='Span', attribute_group='AttributeGroup', event='Event')
        node_type = section.get('type')
        return mappings.get(node_type)

    def import_conventions_from_dir(self, base_path: str):
        """
        Reads all semantic convention YAML files in a given directory and its subdirectories.
        Assumes the conventions from file structures from

        https://github.com/open-telemetry/semantic-conventions/tree/main/model

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

        :param yaml_data: semantic convention 'groups' entry
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
        """
        Add a relation from a node type and node to an attribute.
        An attribute can either be a reference, or an object to be persisted.

        :param node_type: node table name
        :param node: name of the node
        :param attributes: list of attribute entries (dictionaries)
        """
        rels = self.relations['HasAttribute'].setdefault(node_type, [])
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
        """
        Metrics can have associated entities

        :param node_type: node table name
        :param node: name of the node
        :param entities: list of entity entries (dictionaries)
        """
        rels = self.relations['AssociatedWith'].setdefault(node_type, [])
        for entity in entities:
            # Evil hack: supposed to search entity table by 'name' or by 'id', which can be different
            # Choose the id, assume that the difference is that the id name starts with 'entity.'
            if not entity.startswith('entity'):
                entity = 'entity.' + entity
            edge_info = {'from': node, 'to': entity}
            rels.append(edge_info)

    def relate2event(self, node_type: str, node: str, events: list):
        """
        Spans can have associated events

        :param node_type: node table name
        :param node: name of the node
        :param events: list of events entries (dictionaries)
        """
        rels = self.relations['HasEvent'].setdefault(node_type, [])
        for event_name in events:
            if not event_name.startswith('event'):
                event_name = 'event.' + event_name
            edge_info = {'from': node, 'to': event_name}
            rels.append(edge_info)

    def add_attribute(self, attribute: dict):
        """
        Record the attribute as a first-class node

        :param attribute:
        """
        key = attribute['id']
        all_attributes = self.nodes['Attribute']
        if key in all_attributes:
            self.log.error("Attribute key already exists -- skipping", extra=dict(
                attribute_name=key, existing_attribute=all_attributes[key],
                incoming_attribute=attribute))
        else:
            all_attributes[key] = attribute


class PersistenceKuzu(SemanticConventions):
    """
    Use the Kuzu graph database to persist semantic conventions

    https://kuzudb.com/
    """

    relation2node = {
        'HasAttribute': 'Attribute',
        'HasEvent': 'Event',
        'AssociatedWith': 'Entity',
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = None

    def create_db(self, filename: str = 'db/semantic_conventions.kuzu',
                  schema_file: str = 'kuzu_schema.cypher'):
        """
        Create the database, optionally with a Cypher definition file

        :param filename: path and filename of the Kuzu database file
        :param schema_file: Cypher definition file with CREATE statements
        """
        db = kuzu.Database(filename)
        self.conn = kuzu.Connection(db)
        self.set_schema(schema_file)

    def set_schema(self, schema_file: str = 'kuzu_schema.cypher'):
        """
        Use model file, if specified, to take the Cypher definition to
        define the schema

        :param schema_file: Cypher definition file with CREATE statements
        """
        if schema_file:
            with open(schema_file) as fd:
                model_data = fd.read()
            statements = model_data.split(';')
            for statement in statements:
                if statement.strip():
                    self.execute(statement)

    def persist_nodes(self):
        """
        Save node data in the database
        """
        for node_type, data in self.nodes.items():
            filename = node_type + 's.json'
            self.save_import_file(filename, list(data.values()))
            statement = f"COPY {node_type} FROM '{filename}' (ignore_errors=true)"
            self.execute(statement)

    def persist_relations(self):
        """
        Save relations data in the database
        """
        for rel_name, rel_data in self.relations.items():
            rel_endpoint = self.relation2node.get(rel_name)
            for node_type, relations in rel_data.items():
                filename = f'rel_{node_type}_{rel_name}.json'
                self.save_import_file(filename, relations)
                statement = f"COPY {rel_name} FROM '{filename}' (from='{node_type}', to='{rel_endpoint}')"
                self.execute(statement)

    def save_import_file(self, filename: str, db_objects: list):
        """
        Store the intermediate statements as JSON for later import

        :param filename: importable file name
        :param db_objects: list of objects to persist
        """
        with open(filename, 'w') as fd:
            json.dump(db_objects, fd)

    def execute(self, statement: str, pdb_on_error: bool = False):
        """
        Execute the Cypher statement in the database

        :param statement: Cypher statement
        :param pdb_on_error: On error, run the interactive Python DeBugger (pdb)
        :return:
        """
        try:
            self.conn.execute(statement)
        except Exception as ex:
            print(f"{ex} {statement}")
            if pdb_on_error:
                import pdb
                pdb.set_trace()


if __name__ == '__main__':
    START_PATH = '../semantic-conventions/model'

    conventions = PersistenceKuzu(log)
    conventions.import_conventions_from_dir(START_PATH)
    conventions.create_db()
    conventions.persist_nodes()
    conventions.persist_relations()
