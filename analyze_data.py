import pandas as pd
import sqlite3
from pathlib import Path

def setup_database():
    # Create connection to in-memory SQLite database
    conn = sqlite3.connect(':memory:')
    
    # Read CSV files
    recordings_df = pd.read_csv('sample_data/recordings_sample.csv')
    users_df = pd.read_csv('sample_data/users_sample.csv')
    
    # Clean up any potential issues with the data
    # Convert timestamps to proper format
    timestamp_columns = ['created_at', 'updated_at']
    for col in timestamp_columns:
        if col in recordings_df.columns:
            recordings_df[col] = pd.to_datetime(recordings_df[col])
        if col in users_df.columns:
            users_df[col] = pd.to_datetime(users_df[col])
    
    # Import dataframes to SQLite
    recordings_df.to_sql('recordings', conn, index=False)
    users_df.to_sql('users', conn, index=False)
    
    # Create additional required tables
    conn.execute('''
        CREATE TABLE follows (
            follower_id INTEGER,
            followee_id INTEGER,
            created_at TIMESTAMP
        )
    ''')
    
    conn.execute('''
        CREATE TABLE avoids (
            avoider_id INTEGER,
            avoided_id INTEGER,
            created_at TIMESTAMP
        )
    ''')
    
    return conn

def run_high_follower_query(conn):
    query = """
    SELECT
        R.user_id,
        COUNT(DISTINCT R.event_id) AS events_attended,
        U.username,
        U.goal,
        U.occupation_description,
        U.interests,
        U.occupation_status
    FROM
        recordings AS R
        LEFT JOIN users AS U ON R.user_id = U.id
    WHERE
        R.event_id IS NOT NULL
    GROUP BY
        R.user_id,
        U.username,
        U.goal,
        U.occupation_description,
        U.interests,
        U.occupation_status
    HAVING
        COUNT(DISTINCT R.event_id) > 2
    ORDER BY
        events_attended DESC;
    """
    
    return pd.read_sql_query(query, conn)

def run_user_info_query(conn):
    query = """
    WITH ParticipationData AS (
        SELECT 
            user_id, 
            COUNT(user_id) AS total_participation, 
            U.username, 
            U.goal, 
            U.occupation_description, 
            U.interests, 
            U.occupation_status
        FROM recordings
        LEFT JOIN users AS U ON recordings.user_id = U.id
        GROUP BY 
            user_id, 
            U.username, 
            U.goal, 
            U.occupation_description, 
            U.interests, 
            U.occupation_status
        HAVING COUNT(user_id) > 2
    ),
    QuartileData AS (
        SELECT 
            user_id, 
            total_participation, 
            username, 
            goal, 
            occupation_description, 
            interests, 
            occupation_status,
            NTILE(4) OVER (ORDER BY total_participation DESC) AS quartile
        FROM ParticipationData
    )
    SELECT * 
    FROM QuartileData
    WHERE quartile = 1;
    """
    
    return pd.read_sql_query(query, conn)

def main():
    # Setup database
    conn = setup_database()
    
    try:
        # Run queries and store results
        print("Running high follower query...")
        high_follower_results = run_high_follower_query(conn)
        print("\nHigh follower query results:")
        print(high_follower_results)
        
        print("\nRunning user info query...")
        user_info_results = run_user_info_query(conn)
        print("\nUser info query results:")
        print(user_info_results)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # Close connection
        conn.close()

if __name__ == "__main__":
    main()