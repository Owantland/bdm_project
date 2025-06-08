from session_helper_neo4j import create_session, clean_session

# Neo4J processes
# Load Nodes
def load_bcn_zips(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/barcelona_codes.csv' AS line
        CREATE (:ZIPCODE{
            ZIPCODE: line.POSTAL_CODE,
            CITY: "Barcelona"
        })
        """
    )

def load_mad_zips(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/madrid_codes.csv' AS line
        CREATE (:ZIPCODE{
            ZIPCODE: line.POSTAL_CODE,
            CITY: "Madrid"
        })
        """
    )

def load_prs_zips(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/paris_codes.csv' AS line
        CREATE (:ZIPCODE{
            ZIPCODE: line.POSTAL_CODE,
            CITY: "Paris"
        })
        """
    )

def load_node_accommodations(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/accommodation.csv' AS line
        CREATE (:ACCOMMODATION {
            ID: line.property_id,
            NAME: line.property_name,
            X_POS: line.property_longitude,
            Y_POS: line.property_latitude,
            CITY: line.city_code_name
        })
        """
    )

# Load Edges
def load_rel_bcn_zip(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/barcelona_distances.csv' AS line
        MATCH (p1:ZIPCODE {ZIPCODE: line.COD_POSTAL_1})
        MATCH (p2:ZIPCODE {ZIPCODE: line.COD_POSTAL_2})
        CREATE (p1)-[:ROUTE {
            Distance: toFloat(line.distance)
        }]->(p2)
        """
    )

def load_rel_mad_zip(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/madrid_distances.csv' AS line
        MATCH (p1:ZIPCODE {ZIPCODE: line.COD_POSTAL_1})
        MATCH (p2:ZIPCODE {ZIPCODE: line.COD_POSTAL_2})
        CREATE (p1)-[:ROUTE {
            Distance: toFloat(line.distance)
        }]->(p2)
        """
    )

def load_rel_prs_zip(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/paris_distances.csv' AS line
        MATCH (p1:ZIPCODE {ZIPCODE: line.COD_POSTAL_1})
        MATCH (p2:ZIPCODE {ZIPCODE: line.COD_POSTAL_2})
        CREATE (p1)-[:ROUTE {
            Distance: toFloat(line.distance)
        }]->(p2)
        """
    )

def load_rel_acom_zip(session):
    session.run(
        """
        LOAD CSV WITH HEADERS FROM 'file:///data/accommodation_zips.csv' AS line
        MATCH (p1:ACCOMMODATION {ID: line.property_id})
        MATCH (p2:ZIPCODE {ZIPCODE: line.property_postalcode})
        CREATE (p1)-[:LOCATEDIN]->(p2)
        """
    )

def prepare_csv():
    session = create_session()
    session = clean_session(session)

    print("Loading nodes...")
    session.execute_write(load_node_accommodations)
    session.execute_write(load_bcn_zips)
    session.execute_write(load_mad_zips)
    session.execute_write(load_prs_zips)

    print("Loading edges...")
    session.execute_write(load_rel_bcn_zip)
    session.execute_write(load_rel_mad_zip)
    session.execute_write(load_rel_prs_zip)
    session.execute_write(load_rel_acom_zip)

    session.close()

def main():
    prepare_csv()

if __name__ == '__main__':
    main()