import requests
import sqlite3
import pandas as pd
from collections import defaultdict
import time

############################################
# CONFIGURATION
############################################
LOCATIONIQ_BASE_URL = "https://us1.locationiq.com/v1/nearby.php"
LOCATIONIQ_API_KEY = "pk.c39371c749182d352d8aa26bea763d20"
# Radii (in meters) for POI queries around each stop.
# Average walking speed ~ 5 km/h = 83.3 m/min
# 5 min -> ~416m, 10 min -> ~833m, 15 min -> ~1250m (approx)
POI_RADII = [416, 833, 1250]
DB_NAME = "project_data.sqlite3"
CALCULATED_DATA_FILE = "calculated_data.txt"

# To limit how many items we insert per run, define a batch size.
BATCH_SIZE = 25

# Types of stops to gather from SEPTA
STOP_TYPES = ["bus_stops", "trolley_stops", "rail_stations"]

############################################
# DATABASE SETUP
############################################
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    # pois table: store POIs from LocationIQ
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pois (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stop_id INTEGER,
        distance INTEGER,
        category TEXT,
        name TEXT,
        FOREIGN KEY (stop_id) REFERENCES stops(id)
    );
    """)
    conn.commit()
    conn.close()

############################################
# DATA GATHERING FUNCTIONS
############################################
def fetch_locationiq_pois(lat, lon, radius):
    """
    Fetch POIs from LocationIQ.
    Returns a list of dicts with keys: category, name, lat, lon
    """
    params = {
        "key": LOCATIONIQ_API_KEY,
        "lat": lat,
        "lon": lon,
        "tag": "atm",
        "radius": radius,
        "limit": 50,
        "format": "json"
    }
    time.sleep(1.1)
    r = requests.get(LOCATIONIQ_BASE_URL, params=params)
    if r.status_code == 200:
        try:
            data = r.json()
            # data format is a list of POIs
            return data
        except:
            return []
    return []


def store_pois():
    """
    For each stop in DB without enough POIs, fetch and store POIs.
    Limit per run: Up to BATCH_SIZE total POIs across all stops.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    # Find POIs for each stop
    cur.execute("SELECT id, lat, lon, name FROM stops")
    all_stops = cur.fetchall()

    # Attempt to find the second-to-last stop_id from the pois table
    cur.execute("""
        SELECT DISTINCT stop_id FROM pois
        ORDER BY stop_id DESC
        LIMIT 2
    """)
    pois_stop_ids = [row[0] for row in cur.fetchall()]

    second_to_last_stop_id = None
    if len(pois_stop_ids) == 2:
        # The second element in pois_stop_ids is the second-to-last stop_id
        # because we ordered by DESC
        second_to_last_stop_id = pois_stop_ids[1]

    # If we have a second-to-last stop_id, reorder all_stops so iteration begins there
    if second_to_last_stop_id is not None:
        # Find the index of this stop_id in all_stops
        indices = [i for i, (sid, slat, slon, n) in enumerate(all_stops) if sid == second_to_last_stop_id]
        if indices:
            start_index = indices[0]
            # Reorder all_stops to start from second_to_last_stop_id
            all_stops = all_stops[start_index:] + all_stops[:start_index]

    inserted_count = 0
    for stop_id, slat, slon, n in all_stops:
        if inserted_count >= BATCH_SIZE:
            break
        # Check if we already have POIs for this stop
        cur.execute("SELECT COUNT(*) FROM pois WHERE stop_id=?", (stop_id,))
        count_for_radius = cur.fetchone()[0]
        if count_for_radius < 50: # 50 is the maximum number of POIs that can be fetched.
            pois_data = fetch_locationiq_pois(slat, slon, POI_RADII[2])
            for pd in pois_data:
                pdist = pd.get("distance")
                pname = pd.get("name","Unknown Name")
                pcat = pd.get("class","Unknown Category")
                cur.execute("SELECT * FROM pois WHERE stop_id=? AND distance=? AND category=? AND name=?",
                            (stop_id, pdist, pcat, pname))
                if len(cur.fetchall()) == 0:
                    # Insert new POI
                    cur.execute("""
                        INSERT INTO pois (stop_id, distance, name, category) VALUES (?,?,?,?)""",
                        (stop_id, pdist, pname, pcat))
                    conn.commit()
                    inserted_count += 1
                    if inserted_count >= BATCH_SIZE:
                        break
    conn.close()
    return inserted_count


############################################
# MAIN EXECUTION
############################################
if __name__ == "__main__":
    # Initialize DB if not present
    init_db()
    # fetch_locationiq_pois(39.9525020, -75.1652980, 1000)

    # Run data gathering multiple times (run the script multiple times without code change)
    # Store POIs (multiple runs needed until ~100 stops and their POIs are fetched)
    store_pois()
