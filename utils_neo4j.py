from neo4j import GraphDatabase
import pandas as pd

NEO4J_URI="neo4j+s://b9043243.databases.neo4j.io"
NEO4J_USERNAME="neo4j"
NEO4J_PASSWORD="1234"
NEO4J_DATABASE="neo4j"
AURA_INSTANCEID="b9043243"
AURA_INSTANCENAME="College Baseball"

RAW_BASE = "https://raw.githubusercontent.com/danielee982/diamond-knowledge-graph/main"

class GraphDBManager:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_constraints(self):
        query = [
            """
            CREATE CONSTRAINT conference_name_unique IF NOT EXISTS
            FOR (c:Conference)
            REQUIRE c.name IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT school_name_unique IF NOT EXISTS
            FOR (s:School)
            REQUIRE s.name IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT team_name_unique IF NOT EXISTS
            FOR (t:Team)
            REQUIRE t.name IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT player_identity_unique IF NOT EXISTS
            FOR (p:Player)
            REQUIRE (p.name, p.schoolName) IS UNIQUE;
            """,
            """
            CREATE CONSTRAINT coach_identity_unique IF NOT EXISTS
            FOR (c:Coach)
            REQUIRE (c.name, c.schoolName) IS UNIQUE;
            """,
        ]

        for q in query:
            with self.driver.session(database=NEO4J_DATABASE) as session:
                session.run(q)

        print("Constraints created successfully.")

    def load_conferences(self):
        url = f"{RAW_BASE}/conferences.csv"
        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (c:Conference {name: row.name})
            SET c.region = row.region,
                c.abbreviation = row.abbreviation,
                c.foundedYear = toInteger(row.`founded year`),
                c.numberOfTeams = toInteger(row.`number of teams`),
                c.headquarters = row.headquaters;
        """
        with self.driver.session(database=NEO4J_DATABASE) as session:
            session.run(query, url=url)

        print("Conferences loaded successfully.")

    def load_schools(self):
        url = f"{RAW_BASE}/schools.csv"
        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (s:School {name: row.name})
            SET s.schoolType = row.`school type`;
        """
        with self.driver.session(database=NEO4J_DATABASE) as session:
            session.run(query, url=url)

        print("Schools loaded successfully.")

    def load_teams(self):
        url = f"{RAW_BASE}/teams.csv"
        query = """
            LOAD CSV WITH HEADERS FROM $url AS row
            MERGE (t:Team {name: row.name})
            
            // Link to School using same name
            WITH t, row
            MATCH (s:School {name: row.name})
            MERGE (t)-[:REPRESENTS]->(s)

            // Link to Conference using 'member of' field
            WITH t, row
            MATCH (c:Conference {abbreviation: row.`member of`})
            MERGE (t)-[:MEMBER_OF]->(c);
        """
        with self.driver.session(database=NEO4J_DATABASE) as session:
            session.run(query, url=url)

        print("Teams loaded successfully.")

    def load_players(self):
        url = f"{RAW_BASE}/players.csv"
        query = """
            LOAD CSV WITH HEADERS FROM $url AS row

            MERGE (p:Player {
                name:       row.Name,
                schoolName: row.School
            })
            SET p.jersey    = row.Jersey,
                p.position  = row.Position,
                p.classYear = row.`Class Year`,
                p.height    = row.Height,
                p.weight    = row.Weight

            // Player belongs to a university
            WITH p, row
            MATCH (u:School {name: row.School, schoolType: "university"})
            MERGE (p)-[:PLAYS_FOR]->(u)

            // Player attended a high school
            WITH p, row
            WHERE row.`High School` IS NOT NULL AND row.`High School` <> 'N/A'
            MATCH (h:School {name: row.`High School`, schoolType: "high school"})
            MERGE (p)-[:ATTENDED]->(h)
        """
        with self.driver.session(database=NEO4J_DATABASE) as session:
            session.run(query, url=url)

        print("Players loaded successfully.")

    def load_coaches(self):
        url = f"{RAW_BASE}/coaches.csv"
        query = """
            LOAD CSV WITH HEADERS FROM $url AS row

            MERGE (c:Coach {
                name:       row.Name,
                schoolName: row.School
            })
            SET c.position  = row.Title

            // Coach belongs to a university
            WITH c, row
            MATCH (t:Team {name: row.School})
            MERGE (c)-[:Coaches]->(t)
        """
        with self.driver.session(database=NEO4J_DATABASE) as session:
            session.run(query, url=url)

        print("Coaches loaded successfully.")

def main():
    graph_db = GraphDBManager(NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD)
    graph_db.create_constraints()
    graph_db.load_conferences()
    graph_db.load_schools()
    graph_db.load_teams()
    graph_db.load_players()
    graph_db.load_coaches()
    graph_db.close()

if __name__ == "__main__":
    main()