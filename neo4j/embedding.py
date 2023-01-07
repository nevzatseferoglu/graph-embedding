import logging, json, sys, os

from neo4j import GraphDatabase
from neo4j.exceptions import DriverError, SessionError

class CoraNeo4jConnector:

    def __init__(self, uri, user, password, logfile):
        if os.path.exists(logfile):
            logging.basicConfig(filename=logfile, encoding='utf-8', level=logging.INFO)
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            assert self.__verify_driver_and_connection() == True, "Neo4j connection cannot be verified!"
            logging.info("Driver has been initialized and connected successufully!")
            logging.info(f'''
Server Info:
"Address: {self.driver.get_server_info().address}
"Protocol Version: {self.driver.get_server_info().protocol_version}
"Agent: {self.driver.get_server_info().agent}
''')
        else:
            raise FileNotFoundError("Logger file does not exist!")
    
    def __verify_driver_and_connection(self):
        if self.driver is None:
           return False 
        try:
            self.driver.verify_connectivity()
            return True
        except DriverError as err:
            logging.error(f"Connection cannot be verified!, {err}")
            self.driver.close()
            return False

    def close(self):
        assert self.__verify_driver_and_connection() == True, "Neo4j connection cannot be verified!"
        self.driver.close()
    
    def clear_entire_db(self):
        assert self.__verify_driver_and_connection() == True, "Neo4j connection cannot be verified!"
        try:
            with self.driver.session() as session:
                session.execute_write(self.__clear_entire_db, "MATCH (n) DETACH DELETE n")
            logging.info("Database is cleared successufully!")
        except SessionError as err:
            logging.error(f"cannot clear entire db {err}")

    def __clear_entire_db(self, tx, cypher):
        # all nodes and relationships
        result = tx.run(cypher)
        result.consume()
    
    def load_cora_nodes(self):
        assert self.__verify_driver_and_connection() == True, "Neo4j connection cannot be verified!"
        try:
            with self.driver.session() as session:
                result = session.run("CREATE CONSTRAINT paper_constraint IF NOT EXISTS FOR (p:Paper) REQUIRE p.id IS UNIQUE")
                _ = result.single()
                logging.info("Constraint is created on Paper for unique id!")
                record = session.execute_write(CoraNeo4jConnector.__load_cora_nodes_and_return_node_count) 
                _ = record[0]
                logging.info("Nodes are loaded to database successufully!")
        except IndexError:
            logging.error(f"Record does not have given index, load_cora_nodes!")
            sys.exit(-1)
        except SessionError as err:
            logging.error(f"cannot clear entire db {err}")
    
    @staticmethod
    def __load_cora_nodes_and_return_node_count(tx):

        # idempotent operation
        cypher = """
LOAD CSV WITH HEADERS FROM 'https://cora-dataset.s3.eu-central-1.amazonaws.com/cora-dataset/nodes.csv' AS row
WITH row
MERGE (p:Paper {id: row.id, subject: row.subject, features: row.features})
RETURN COUNT(*)
"""
        result = tx.run(cypher)
        return result.single()

    def load_cora_edges(self):
        try:
            with self.driver.session() as session:
                record = session.execute_write(CoraNeo4jConnector.__load_cora_edges) 
                _ = record[0]
                logging.info("Nodes are loaded to database successufully!")
        except IndexError:
            logging.error(f"Record does not have given index, load_cora_nodes!")
            sys.exit(-1)
        except SessionError as err:
            logging.error(f"cannot clear entire db {err}")
    
#     @staticmethod
#     def __load_cora_edges(tx):
#
#         # idempotent operation
#         cypher = """LOAD CSV WITH HEADERS FROM 'https://cora-dataset.s3.eu-central-1.amazonaws.com/cora-dataset/edges.csv' AS row
#            WITH row
#            MATCH (to:Paper {id: row.target})
#            MATCH (from:Paper {id: row.source})
#            MERGE (from)-[c:CITED]->(to)
#            RETURN COUNT(*)
# """
#         result = tx.run(cypher)
#         return result.single()

    #
    # def create_friendship(self, person1_name, person2_name):
    #     with self.driver.session() as session:
    #         result = session.execute_write(
    #             self._create_and_return_friendship, person1_name, person2_name)
    #         for record in result:
    #             print("Created friendship between: {p1}, {p2}".format(
    #                 p1=record['p1'], p2=record['p2']))
    #
    # @staticmethod
    # def _create_paper_nodes_and_return(tx):
    #     query = (
    #         "CREATE (p1:Person { name: $person1_name }) "
    #         "CREATE (p2:Person { name: $person2_name }) "
    #         "CREATE (p1)-[:KNOWS]->(p2) "
    #         "RETURN p1, p2"
    #     )
    #     result = tx.run(query, person1_name=person1_name, person2_name=person2_name)
    #     try:
    #         return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
    #                 for record in result]
    #     # Capture any errors along with the query and data for traceability
    #     except ServiceUnavailable as exception:
    #         logging.error("{query} raised an error: \n {exception}".format(
    #             query=query, exception=exception))
    #         raise
    #
    # def find_person(self, person_name):
    #     with self.driver.session() as session:
    #         result = session.execute_read(self._find_and_return_person, person_name)
    #         for record in result:
    #             print("Found person: {record}".format(record=record))

    # @staticmethod
    # def _find_and_return_person(tx, person_name):
    #     query = (
    #         "MATCH (p:Person) "
    #         "WHERE p.name = $person_name "
    #         "RETURN p.name AS name"
    #     )
    #     result = tx.run(query, person_name=person_name)
    #     return [record["name"] for record in result]

if __name__ == "__main__":
    with open("auth.json", "r") as auth:
        auth = json.load(auth)
        db = CoraNeo4jConnector(auth["URI"], 
                                  auth["USER"],
                                  auth["PASSWORD"],
                                  auth["LOGFILE"])

        db.clear_entire_db()
        db.load_cora_nodes()
        db.close()

