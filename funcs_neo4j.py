from neo4j import GraphDatabase
import dotenv
import os

RAW_BASE = "https://raw.githubusercontent.com/danielee982/diamond-knowledge-graph/main"

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
                FOR (p:Player) REQUIRE (p.name, p.schoolName) IS UNIQUE;""",
            """CREATE CONSTRAINT coach_identity_unique IF NOT EXISTS
                FOR (c:Coach) REQUIRE (c.name, c.schoolName) IS UNIQUE;""",
            """CREATE CONSTRAINT team_name_unique IF NOT EXISTS
                FOR (t:Team) REQUIRE t.name IS UNIQUE;""",
            """CREATE CONSTRAINT school_name_unique IF NOT EXISTS
                FOR (s:School) REQUIRE s.name IS UNIQUE;""",
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
            MERGE (p:Player {name: row.Name, schoolName: row.School})
            SET p.jerseyNumber = row.Jersey,
                p.position = row.Position,
                p.height = row.Height,
                p.weight = row.Weight,
                p.year = row.`Class Year`,
                p.highSchool = row.`High School`;
        """
        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Players added successfully.")

    def add_coaches(self):
        url = f"{RAW_BASE}/coaches.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (c:Coach {name: row.Name, schoolName: row.School})
            SET c.position = row.Title;
        """
        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Coaches added successfully.")

    def add_teams(self):
        url = f"{RAW_BASE}/teams.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (t:Team {name: row.name});
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

    def add_schools(self):
        url = f"{RAW_BASE}/schools.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (s:School {name: row.name})
            SET s.schoolType = row.`school type`;
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Schools added successfully.")

    def add_player_relationships(self):
        url = f"{RAW_BASE}/players.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MATCH (p:Player {name: row.Name, schoolName: row.School}), (t:Team {name: row.School})
            MERGE (p)-[:PLAYS_FOR]->(t)

            WITH p, row
            MATCH (hs:School {name: row.`High School`})
            MERGE (p)-[:ATTENDED]->(hs)
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Player relationships added successfully.")
    
    def add_team_relationships(self):
        url = f"{RAW_BASE}/teams.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MATCH (t:Team {name: row.name}), (c:Conference {abbreviation: row.`member of`})
            MERGE (t)-[:MEMBER_OF]->(c)

            WITH t, row
            MATCH (s:School {name: row.name})
            MERGE (t)-[:REPRESENTS]->(s)
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Team relationships added successfully.")

    def add_coach_relationships(self):
        url = f"{RAW_BASE}/coaches.csv"

        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MATCH (c:Coach {name: row.Name, schoolName: row.School}), (t:Team {name: row.School})
            MERGE (c)-[:COACHES]->(t)
        """

        self.driver.execute_query(query, url=url, database_=self.DATABASE)
        print("Coach relationships added successfully.")

    def load_all(self):
        self.create_constraints()
        self.add_players()
        self.add_coaches()
        self.add_conferences()
        self.add_teams()
        self.add_schools()

        self.add_player_relationships()
        self.add_team_relationships()
        self.add_coach_relationships()
        self.close()

if __name__ == "__main__":
    manager = GraphDBManager()
    manager.load_all()