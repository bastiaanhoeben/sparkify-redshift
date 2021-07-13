import configparser
import psycopg2
import pandas as pd
from aws_config import get_cluster_endpoint


# Query top 10 artists play during evening hours (6pm - 12pm)
top_artists = ("""
SELECT a.name, count(*) AS songplays
FROM songplays AS s
INNER JOIN artists AS a
ON s.artist_id = a.artist_id
INNER JOIN time AS t
ON s.start_time = t.start_time
WHERE t.hour BETWEEN 18 AND 23
GROUP BY a.name
ORDER BY 2 DESC
LIMIT 10;
""")

# Compare number of paid user songplays split up for male and female users
level_plays = ("""
SELECT u.gender, s.level, count(*) AS songplays
FROM songplays AS s
INNER JOIN users AS u
ON s.user_id = u.user_id
GROUP BY 1, 2
ORDER BY 1, 2;
""")

# List of queries to be run against database
analytics_queries = [top_artists, level_plays]


def run_analytics_queries(cur, conn):
    """Successively runs analytic queries."""
    
    output = []

    for query in analytics_queries:
        cur.execute(query)
        records = cur.fetchall()
        column_names = list(map(lambda x: x[0], cur.description))
        output.append(pd.DataFrame(records, columns=column_names))
    
    for table in output:
        print(table, end='\n\n')


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY, SECRET, REGION = config['AWS'].values()
    DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')

    DB_NAME, DB_USER, DB_PASSWORD, DB_PORT = config['DB'].values()

    host = get_cluster_endpoint(REGION, KEY, SECRET, DWH_CLUSTER_IDENTIFIER)

    conn = psycopg2.connect(f"host={host} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} port={DB_PORT}")
    cur = conn.cursor()
    
    run_analytics_queries(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()