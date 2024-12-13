import requests
import sqlite3

############################################
# CONFIGURATION
############################################
SEPTA_BASE_URL = "https://www3.septa.org/hackathon/locations/get_locations.php"
PHILLY_LAT = 39.9526
PHILLY_LON = -75.1652
RADIUS_MILES = 100  # for SEPTA stops
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
    # Create tables if they do not exist
    # stops table: store SEPTA stops
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stop_type TEXT,
        stop_id TEXT,
        name TEXT,
        lat REAL,
        lon REAL,
        UNIQUE(stop_type, stop_id)
    );
    """)
    conn.commit()
    conn.close()


############################################
# DATA GATHERING FUNCTIONS
############################################
def fetch_septa_stops(stop_type, lat=PHILLY_LAT, lon=PHILLY_LON, radius=RADIUS_MILES):
    """
    Fetch SEPTA stops of a given type from the SEPTA API.
    Returns a list of dicts with stop info.
    """
    params = {
        "type": stop_type,
        "lat": lat,
        "lon": lon,
        "radius": radius
    }
    r = requests.get(SEPTA_BASE_URL, params=params)
    data = r.json() if r.status_code == 200 else []
    # Expected data format: a list of dicts with keys like:
    # {
    #    "location_id": "1392",
    #    "location_name": "15th St Station",
    #    "location_lat": "39.9525020",
    #    "location_lon": "-75.1652980",
    #    ...
    # }
    return data

def store_septa_stops():
    """
    Fetch and store SEPTA stops into the database.
    Limit to BATCH_SIZE new stops per run per stop type.
    """
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    beeg_data = { STOP_TYPES[0]: fetch_septa_stops("bus_stops"),
                  STOP_TYPES[1]: fetch_septa_stops("trolley_stops"),
                  STOP_TYPES[2]: fetch_septa_stops("rail_stations") }

    num_inserted = 0
    while num_inserted < BATCH_SIZE or len(beeg_data) == 0:
        if len(beeg_data) == 0:
            break
        item_num = num_inserted % len(beeg_data)
        stype = list(beeg_data.keys())[item_num]
        while len(beeg_data[stype]) > 0:
            # Filter out those already in the DB
            stop_id = beeg_data[stype][0].get("location_id")
            name = beeg_data[stype][0].get("location_name")
            lat = beeg_data[stype][0].get("location_lat")
            lon = beeg_data[stype][0].get("location_lon")
            # Check if already exists
            cur.execute("SELECT 1 FROM stops WHERE stop_type=? AND stop_id=?", (stype, stop_id))
            # If the query returns None, then we can add it to the DB
            if cur.fetchone() is None:
                cur.execute("INSERT INTO stops (stop_type, stop_id, name, lat, lon) VALUES (?,?,?,?,?)",
                                (stype, stop_id, name, lat, lon))
                conn.commit()
                # Remove from the list
                beeg_data[stype].pop(0)
                # If the list is now empty, remove the key
                if len(beeg_data[stype]) == 0:
                    beeg_data.pop(stype)
                # keep track of how many we've inserted
                num_inserted += 1
                break
            # If the query returns something, then we can skip the item and remove the duplicate
            else:
                beeg_data[stype].pop(0)

    conn.close()

############################################
# MAIN EXECUTION
############################################
if __name__ == "__main__":
    # Initialize DB if not present
    init_db()

    # Run data gathering multiple times (run the script multiple times without code change)
    # Store SEPTA stops
    store_septa_stops()