from neo4j import GraphDatabase
import xmltodict as xd


class XMLImporter:
    """
    Class that imports data from an XML file into a Neo4j database.

    Args:
        file (str): Path to the XML file.
        uri (str): URI of the Neo4j database.
        user (str): Username of the Neo4j database.
        password (str): Password of the Neo4j database.
        database (str): Name of the Neo4j database.

    Attributes:
        file (str): Path to the XML file.
        uri (str): URI of the Neo4j database.
        user (str): Username of the Neo4j database.
        password (str): Password of the Neo4j database.
        database (str): Name of the Neo4j database.
        data (dict): Parsed data from the XML file.

    """

    def __init__(self, file: str, uri: str, user: str, password: str, database: str) -> None:
        self.file = file
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database

    def load_file(self) -> None:
        """
        Loads the XML file and parses it using xmltodict.
        """
        with open(self.file, 'r', encoding='utf-8') as xml_file:
            self.data = xd.parse(xml_file.read())

    def open_database(self) -> GraphDatabase.driver:
        """
        Opens a connection to the Neo4j database and returns a driver object.

        Returns:
            GraphDatabase.driver: The driver object.

        """
        driver = GraphDatabase.driver(
            self.uri,
            auth=(
                self.user,
                self.password
            ),
            database=self.database
        )
        return driver

    def close_database(self, driver: GraphDatabase.driver) -> None:
        """
        Closes the connection to the Neo4j database.

        Args:
            driver (GraphDatabase.driver): The driver object.

        """
        driver.close()

    def insert_protein(self) -> None:
        """
        Inserts protein data from the XML file into the Neo4j database.
        """
        driver = self.open_database()

        query = "CREATE (p:Protein {id_protein: $id_protein})"

        with driver.session() as session:
            id_protein = "Q9Y261"
            session.run(query, id_protein=id_protein)

        self.close_database(driver)

    def insert_gene(self) -> None:
        """
        Inserts gene data from the XML file into the Neo4j database.
        """
        driver = self.open_database()

        query = "CREATE (g:Gene {name: $name})"
        query_relationship = """
        MATCH (p:Protein {id_protein: $id_protein})
        MATCH (g:Gene {name: $name})
        CREATE (p)-[:FROM_GENE]->(g)
        """

        with driver.session() as session:
            for gene in self.data['uniprot']['entry']['gene']['name']:
                if (
                    gene['@type'] == 'synonym' and gene['#text'] == 'HNF3B'
                ) or (
                    gene['@type'] == 'primary' and gene['#text'] == 'FOXA2'
                ):
                    name = gene['#text']
                    session.run(
                        query,
                        name=name
                    )
                    session.run(
                        query_relationship,
                        id_protein='Q9Y261',
                        name=name
                    )

        self.close_database(driver)

    def insert_feature(self) -> None:
        """
        Inserts feature data from the XML file into the Neo4j database.
        """
        driver = self.open_database()

        query = "CREATE (f:Feature {name: $name, type: $type})"
        query_relationship = """
        MATCH (p:Protein {id_protein: $id_protein})
        MATCH (f:Feature {name: $name, type: $type})
        CREATE (p)-[:HAS_FEATURE]->(f)
        """

        with driver.session() as session:
            for feature in self.data['uniprot']['entry']['feature']:
                if (feature['location'].get('position', {}).get('@position') == '307'):
                    name = feature['@description']
                    type = feature['@type']

                    if (name == 'Phosphoserine') and (type == 'modified residue'):
                        session.run(query, name=name, type=type)
                        session.run(
                            query_relationship,
                            id_protein='Q9Y261',
                            name=name,
                            type=type
                        )

        self.close_database(driver)

    def insert_reference(self) -> None:
        """
        Inserts reference data from the XML file into the Neo4j database.
        """
        driver = self.open_database()

        query = "CREATE (r:Reference {id: $id, type: $type, name: $name})"
        query_relationship = """
        MATCH (p:Protein {id_protein: $id_protein})
        MATCH (r:Reference {id: $id, type: $type, name: $name})
        CREATE (p)-[:HAS_REFERENCE]->(r)
        """

        with driver.session() as session:
            for reference in self.data['uniprot']['entry']['reference']:

                id = ""
                type = ""
                name = ""

                if '@key' in reference:
                    id = reference['@key']

                if reference.get('citation', {}).get('@type') is not None:
                    type = reference['citation']['@type']

                if reference.get('citation', {}).get('@name') is not None:
                    name = reference['citation']['@name']

                session.run(query, id=id, type=type, name=name)
                session.run(
                    query_relationship,
                    id_protein='Q9Y261',
                    id=id,
                    type=type,
                    name=name
                )

                if 'authorList' in reference['citation']:
                    authorList = reference['citation']['authorList']
                    self.insert_author(authorList, id, type, name)

        self.close_database(driver)

    def insert_author(self, authorList, id, type, author) -> None:
        """
        Inserts author data from the XML file into the Neo4j database.
        """
        driver = self.open_database()

        query = "CREATE (a:Author {name: $name})"
        query_relationship = """
        MATCH (r:Reference {id: $id, type: $type, name: $author})
        MATCH (a:Author {name: $name})
        CREATE (r)-[:HAS_AUTHOR]->(a)
        """

        with driver.session() as session:
            if 'person' in authorList:
                for person in authorList['person']:
                    name = person['@name']
                    session.run(query, name=name)
                    session.run(
                        query_relationship,
                        id=id,
                        type=type,
                        author=author,
                        name=name
                    )

        self.close_database(driver)

    def insert_fullname(self) -> None:
        """
        Inserts fullname data from the XML file into the Neo4j database.
        """
        driver = self.open_database()

        query = "CREATE (f:FullName {name: $name})"
        query_relationship = """
        MATCH (p:Protein {id_protein: $id_protein})
        MATCH (f:FullName {name: $name})
        CREATE (p)-[:HAS_FULL_NAME]->(f)
        """

        with driver.session() as session:
            if (
                self.data['uniprot']['entry']['protein']['recommendedName']['fullName'] == "Hepatocyte nuclear factor 3-beta"
            ):
                name = self.data['uniprot']['entry']['protein']['recommendedName']['fullName']
                session.run(query, name=name)
                session.run(
                    query_relationship,
                    id_protein='Q9Y261',
                    name=name
                )

        self.close_database(driver)

    def insert_organism(self) -> None:
        """
        Inserts organism data from the XML file into the Neo4j database.
        """
        driver = self.open_database()

        query = "CREATE (f:Organism {name: $name, taxonomy_id: $taxonomy_id})"
        query_relationship = """
        MATCH (p:Protein {id_protein: $id_protein})
        MATCH (o:Organism {name: $name, taxonomy_id: $taxonomy_id})
        CREATE (p)-[:IN_ORGANISM]->(o)
        """

        with driver.session() as session:
            for organism in self.data['uniprot']['entry']['organism']['name']:
                if (
                    organism['#text'] == 'Homo sapiens'
                ):
                    name = organism['#text']
                    taxonomy_id = "9606"
                    session.run(query, name=name, taxonomy_id=taxonomy_id)
                    session.run(
                        query_relationship,
                        id_protein='Q9Y261',
                        name=name,
                        taxonomy_id=taxonomy_id
                    )

        self.close_database(driver)

    def run(self) -> None:
        self.load_file()

        self.insert_protein()
        self.insert_gene()
        self.insert_feature()
        self.insert_reference()
        self.insert_fullname()
        self.insert_organism()


file = "./data/Q9Y261.xml"
uri = "bolt://localhost:7687"
user = "neo4j"
password = "123456789"
database = "pixaflow"

importer = XMLImporter(file, uri, user, password, database)
importer.run()
