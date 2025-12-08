from neo4j import GraphDatabase
import dotenv
import os

RAW_BASE = "https://raw.githubusercontent.com/danielee982/diamond-knowledge-graph/main/data/processed"

class GraphDBManager:
    def __init__(self):
        load_status = dotenv.load_dotenv('Neo4j-b9043243-Created-2025-11-16.txt')
        if load_status is False:
            raise RuntimeError("Environment variables not loaded.")
        
        URI = os.getenv("NEO4J_URI")
        AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

        self.DATABASE = os.getenv("NEO4J_DATABASE")
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        self.driver.verify_connectivity()
        print("Connected to Neo4j database successfully.")

    def close(self):
        self.driver.close()

    def create_constraints(self):
        queries = [
            """CREATE CONSTRAINT player_identity_unique IF NOT EXISTS
                FOR (p:Player) REQUIRE (p.name, p.jerseyNumber, p.schoolName) IS UNIQUE;""",
            """CREATE CONSTRAINT coach_identity_unique IF NOT EXISTS
                FOR (c:Coach) REQUIRE (c.name, c.schoolName) IS UNIQUE;""",
            """CREATE CONSTRAINT team_name_unique IF NOT EXISTS
                FOR (t:Team) REQUIRE t.name IS UNIQUE;""",
            """CREATE CONSTRAINT college_name_unique IF NOT EXISTS
                FOR (c:College) REQUIRE c.name IS UNIQUE;""",
            """CREATE CONSTRAINT last_school_name_unique IF NOT EXISTS
                FOR (ls:LastSchool) REQUIRE ls.name IS UNIQUE;""",
            """CREATE CONSTRAINT conference_name_unique IF NOT EXISTS   
                FOR (c:Conference) REQUIRE c.name IS UNIQUE;"""
        ]

        for q in queries:
            self.driver.execute_query(q, database_=self.DATABASE)
        print("Constraints created successfully.")
    
    def add_players(self):
        url = f"{RAW_BASE}/players.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (p:Player {name: row.Name, jerseyNumber: row.Jersey, schoolName: row.College})
            SET p.height = toInteger(row.Height),
                p.weight = toInteger(row.Weight),
                p.year = row.`Class Year`;
        """
        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Players added successfully.")

    def add_positions(self):
        url = f"{RAW_BASE}/positions.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (p:Position {name: row.name})
        """
        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Positions added successfully.")

    def add_coaches(self):
        url = f"{RAW_BASE}/coaches.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (c:Coach {name: row.Name, schoolName: row.College})
            SET c.rawPosition = row.Title,
                c.roleList = row.`Role List`;
        """
        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Coaches added successfully.")

    def add_teams(self):
        url = f"{RAW_BASE}/teams.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (t:Team {name: row.team});
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Teams added successfully.")

    def add_conferences(self):
        url = f"{RAW_BASE}/conferences.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (c:Conference {name: row.name})
            SET c.region = row.region,
                c.abbreviation = row.abbreviation,
                c.foundedYear = row.`founded year`,
                c.numberOfTeams = row.`number of teams`,
                c.headquarters = row.headquarters;
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Conferences added successfully.")

    def add_lastschools(self):
        url = f"{RAW_BASE}/lastschools.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (ls:LastSchool {name: row.name})
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Last Schools added successfully.")
    
    def add_colleges(self):
        url = f"{RAW_BASE}/colleges.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (c:College {name: row.name})
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Colleges added successfully.")

    def add_player_relationships(self):
        url = f"{RAW_BASE}/players.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MATCH (p:Player {name: row.Name, jerseyNumber: row.Jersey, schoolName: row.College}), (t:Team {name: row.Team})
            MERGE (p)-[:PLAYS_FOR]->(t)

            WITH p, row
            MATCH (ls:LastSchool {name: row.`Last School`})
            MERGE (p)-[:ATTENDED]->(ls)

            WITH p, [row.position1, row.position2, row.position3] AS positions
            UNWIND positions as pos

            WITH p, pos
            WHERE pos IS NOT NULL AND pos <> ""

            MATCH (ps: Position {name: pos})
            MERGE (p)-[:HAS_POSITION]->(ps);
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Player relationships added successfully.")
    
    def add_team_relationships(self):
        url = f"{RAW_BASE}/teams.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MATCH (t:Team {name: row.team}), (c:Conference {abbreviation: row.`member of`})
            MERGE (t)-[:MEMBER_OF]->(c)

            WITH t, row
            MATCH (c:College {name: row.college})
            MERGE (t)-[:REPRESENTS]->(c)
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Team relationships added successfully.")

    def add_coach_relationships(self):
        url = f"{RAW_BASE}/coaches.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MATCH (c:Coach {name: row.Name, schoolName: row.College}), (t:Team {name: row.Team})
            MERGE (c)-[:COACHES]->(t)
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Coach relationships added successfully.")

    def delete_all(self):
        query = "MATCH (n) DETACH DELETE n;"
        self.driver.execute_query(query, database_=self.DATABASE)
        print("All nodes and relationships deleted successfully.")

    def load_all(self):
        self.delete_all()
        self.create_constraints()
        self.add_conferences()
        self.add_lastschools()
        self.add_teams()
        self.add_players()
        self.add_coaches()
        self.add_colleges()

        self.add_player_relationships()
        self.add_team_relationships()
        self.add_coach_relationships()
        self.close()

if __name__ == "__main__":
    manager = GraphDBManager()
    manager.load_all()