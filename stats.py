import sqlite3
import pandas as pd
from collections import defaultdict
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import csv
import mpld3
import contextily as ctx

import septa
import pois

############################################
# CONFIGURATION
############################################
PHILLY_LAT = 39.9526
PHILLY_LON = -75.1652
# Radii (in meters) for POI queries around each stop.
# Average walking speed ~ 5 km/h = 83.3 m/min
# 5 min -> ~416m, 10 min -> ~833m, 15 min -> ~1250m (approx)
POI_RADII = [416, 833, 1250]
# default "project_data.sqlite3"
DB_NAME = "project_data.sqlite3"
CALCULATED_DATA_FILE = "calculated_data.txt"
# default 6
NUM_SEPTA_RUNS = 6
# default 24
NUM_POI_RUNS = 24

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

    # pois table: store POIs from LocationIQ
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pois (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stop_id INTEGER,
        radius INTEGER,
        category TEXT,
        name TEXT,
        lat REAL,
        lon REAL,
        FOREIGN KEY (stop_id) REFERENCES stops(id)
    );
    """)

    conn.commit()
    conn.close()

############################################
# DATA CALCULATION AND VISUALIZATION
############################################
def calculate_data():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    results = {r: {} for r in POI_RADII}
    
    for radius in POI_RADII:
        query = f"""
            SELECT s.stop_type, s.name, COUNT(p.id) AS poi_count
            FROM stops s
            LEFT JOIN pois p ON s.id = p.stop_id AND p.distance <= ?
            GROUP BY s.id
        """
        
        cur.execute(query, (radius,))
        rows = cur.fetchall()
        
        # Organize data by stop_type
        data_by_type = {}
        for stop_type, name, poi_count in rows:
            if stop_type not in data_by_type:
                data_by_type[stop_type] = []
            data_by_type[stop_type].append((name, poi_count))
        
        # Deduplicate by stop name: if duplicates occur, pick one.
        # For example, pick the first occurrence or the one with the largest poi_count.
        for stype, stops_list in data_by_type.items():
            unique_dict = {}
            for name, poi_count in stops_list:
                # If you want to always pick the stop with the highest poi_count when duplicates arise:
                if name not in unique_dict or poi_count > unique_dict[name]:
                    unique_dict[name] = poi_count
            # Convert back to list
            data_by_type[stype] = [(n, c) for n, c in unique_dict.items()]
        
        # Sort each stop_type's list by poi_count ascending
        for stype in data_by_type:
            data_by_type[stype].sort(key=lambda x: x[1])  # ascending by poi_count
        
        # Determine top_5 and least_5
        for stype in STOP_TYPES:
            stops_list = data_by_type.get(stype, [])
            top_5 = stops_list[-5:] if len(stops_list) >= 5 else stops_list
            least_5 = stops_list[:5]
            
            results[radius][stype] = {
                'most': top_5,
                'least': least_5
            }
    
    # Write out results
    with open('calculated_data.txt', 'w') as f:
        for radius in POI_RADII:
            f.write(f"Radius: {radius}\n")
            for stype in STOP_TYPES:
                f.write(f"  Stop Type: {stype}\n")
                most_list = results[radius][stype]['most']
                most_list.reverse()
                least_list = results[radius][stype]['least']
                f.write("    Top 5 stops with the MOST ATMs:\n")
                for name, poi_count in most_list:
                    f.write(f"      {name}: {poi_count} ATMs\n")
                f.write("    Top 5 stops with the LEAST ATMs:\n")
                for name, poi_count in least_list:
                    f.write(f"      {name}: {poi_count} ATMs\n")
            f.write("\n")
    conn.close()

# get data
def load_data_from_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    
    # data structure:
    # data[radius][stop_type] = {'most': [(name, count), ...], 'least': [(name, count), ...]}
    data = {r: {} for r in POI_RADII}
    
    for radius in POI_RADII:
        query = """
            SELECT s.stop_type, s.name, COUNT(p.id) AS poi_count
            FROM stops s
            LEFT JOIN pois p ON s.id = p.stop_id AND p.distance <= ?
            GROUP BY s.id;
        """
        cur.execute(query, (radius,))
        rows = cur.fetchall()
        
        # Organize data by stop_type
        data_by_type = {}
        for stop_type, name, poi_count in rows:
            if stop_type not in data_by_type:
                data_by_type[stop_type] = []
            data_by_type[stop_type].append((name, poi_count))
        
        # Remove duplicates by name if necessary (optional, only if duplicates occur)
        for stype in data_by_type:
            unique_dict = {}
            for nm, cnt in data_by_type[stype]:
                if nm not in unique_dict or cnt > unique_dict[nm]:
                    unique_dict[nm] = cnt
            data_by_type[stype] = [(n, c) for n, c in unique_dict.items()]
        
        # Sort ascending by poi_count
        for stype in data_by_type:
            data_by_type[stype].sort(key=lambda x: x[1])
        
        # Assign the processed data back to the main data structure
        data[radius] = data_by_type
    
    cur.execute("SELECT name, stop_type, lat, lon FROM stops")
    rows = cur.fetchall()
    stop_dict = {}
    for name, stype, lat, lon in rows:
        stop_dict[name] = (stype, lat, lon)
    
    conn.close()
    return (data, stop_dict)

### visualizations
def visualization(data, stop_dict):
    radius_labels = {
        416: "5 min (~400m)",
        833: "10 min (~800m)",
        1250: "15 min (~1250m)"
    }
    stop_type_labels = {
        "bus_stops": "Bus Stop",
        "trolley_stops": "Trolley Stop",
        "rail_stations": "Railway Stop"
    }
    palette = {
        "Bus Stop": "#356cea",
        "Trolley Stop": "#0bb756",
        "Railway Stop": "#f36126"
    }
    
    sns.set_theme(style="whitegrid", context="talk")
    # Plot 1: ATMs per stop per type per radius
    # Mapping for display labels
    fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=True)
    for i, radius in enumerate([416, 833, 1250]):
        # Calculate total ATMs per stop type for this radius
        plot_data = []
        for stype in STOP_TYPES:
            if stype in data[radius]:
                total_atms = sum(cnt for (name, cnt) in data[radius][stype])
            else:
                total_atms = 0
            plot_data.append((stype, total_atms))
        df_counts = pd.DataFrame(plot_data, columns=["Stop_Type", "Total_ATMs"])
        # Replace the internal type with the user-friendly label
        df_counts["Stop_Type"] = df_counts["Stop_Type"].map(stop_type_labels)
        sns.barplot(data=df_counts, x="Stop_Type", y="Total_ATMs", hue="Stop_Type", ax=axes[i], palette="Set2", legend=False)
        axes[i].set_title(f"Total ATMs at {radius_labels[radius]}", fontsize=16)
        axes[i].set_xlabel("", fontsize=14)
        if i == 0:
            axes[i].set_ylabel("Total ATM Count", fontsize=14)
        else:
            axes[i].set_ylabel("")
        axes[i].tick_params(axis='x', rotation=45)
    plt.tight_layout()
    plt.show()

    # Plot 2: Average ATM Counts for Bus Stops by Radius
    plot_data = []
    for r in data:
        if 'bus_stops' in data[r] and data[r]['bus_stops']:
            atm_counts = [atm for (name, atm) in data[r]['bus_stops']]
            avg_atm = sum(atm_counts)/len(atm_counts)
            plot_data.append((radius_labels[r], avg_atm))

    df_bus = pd.DataFrame(plot_data, columns=["Radius_Desc", "Avg_ATM_Count"])
    plt.figure(figsize=(8,6))
    sns.barplot(data=df_bus, x="Avg_ATM_Count", y="Radius_Desc", color="skyblue", orient="h")
    plt.title("Average ATM Counts for Bus Stops by Walking Distance", fontsize=18)
    plt.xlabel("Average ATM Count", fontsize=14)
    plt.ylabel("Walking Distance", fontsize=14)
    plt.tight_layout()
    plt.show()

    # Plot 3: Heatmap of Average ATM Counts by Stop Type and Radius
    # Compute average ATM counts across all stops of each type and radius
    plot_data = []
    for r in data:
        for stype in data[r]:
            atm_counts = [cnt for (n, cnt) in data[r][stype]]
            avg_atm = sum(atm_counts)/len(atm_counts)
            plot_data.append((radius_labels[r], stype, avg_atm))

    df_heat = pd.DataFrame(plot_data, columns=["Radius_Desc", "Stop_Type", "Avg_ATMs"])
    df_heat_pivot = df_heat.pivot(index="Stop_Type", columns="Radius_Desc", values="Avg_ATMs")

    # Define desired order of columns
    desired_order = ["5 min (~400m)", "10 min (~800m)", "15 min (~1250m)"]
    df_heat_pivot = df_heat_pivot[desired_order]

    plt.figure(figsize=(6,4))
    sns.heatmap(df_heat_pivot, annot=True, cmap="YlGnBu", fmt=".2f", cbar_kws={'label': 'Avg ATMs'})
    plt.title("Average ATM Counts by Stop Type and Walking Distance", fontsize=16)
    plt.xlabel("Walking Distance", fontsize=14)
    plt.ylabel("Stop Type", fontsize=14)
    plt.tight_layout()
    plt.show()
    
    # Plot 4: Interactive Map (exported as 'interactive_plot.html'; open this in your browser!)
    # Choose a bounding box that covers all data points
    west = PHILLY_LON - 0.1
    east = PHILLY_LON + 0.1
    south = PHILLY_LAT - 0.1
    north = PHILLY_LAT + 0.1

    # Create the map image
    fig_map, ax_map = plt.subplots(figsize=(6,6))
    ax_map.set_xlim(west, east)
    ax_map.set_ylim(south, north)
    ax_map.set_axis_off()
    # Use OpenStreetMap Mapnik tiles
    ctx.add_basemap(ax_map, crs="EPSG:4326", source=ctx.providers.OpenStreetMap.Mapnik)
    plt.savefig("philly_map.png", dpi=300, bbox_inches='tight', pad_inches=0)
    plt.close(fig_map)

    sns.set_theme(style="white", context="talk")

    fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharex=True, sharey=True)

    for i, radius in enumerate(POI_RADII):
        plot_data = []
        for stype in data[radius]:
            for (sname, poi_count) in data[radius][stype]:
                stype_orig, lat, lon = stop_dict[sname]
                x = lon - PHILLY_LON
                y = lat - PHILLY_LAT
                readable_type = stop_type_labels[stype_orig]
                plot_data.append((sname, readable_type, x, y, poi_count))

        df_plot = pd.DataFrame(plot_data, columns=["Stop_Name", "Stop_Type", "X", "Y", "POI_Count"])

        # Show the map image as background with the same extent
        axes[i].imshow(plt.imread('philly_map.png'), 
                    extent=[-0.1, 0.1, -0.1, 0.1], 
                    origin='upper', 
                    zorder=0,
                    alpha=0.7)

        scatter = sns.scatterplot(
            data=df_plot,
            x="X",
            y="Y",
            hue="Stop_Type",
            size="POI_Count",
            palette=palette,
            sizes=(20, 300),
            alpha=0.7,
            legend=False,
            ax=axes[i]
        )

        axes[i].set_xlim(-0.1, 0.1)
        axes[i].set_ylim(-0.1, 0.1)
        axes[i].grid(False)
        axes[i].set_facecolor('white')

        axes[i].set_title(f"POIs at {radius_labels[radius]}", fontsize=16)
        axes[i].set_xlabel("Longitude offset", fontsize=12)
        if i == 0:
            axes[i].set_ylabel("Latitude offset", fontsize=12)
        else:
            axes[i].set_ylabel("")

        # Tooltips
        labels = [f"Type: {row['Stop_Type']}\nName: {row['Stop_Name']}\nPOIs: {row['POI_Count']}"
                for _, row in df_plot.iterrows()]
        points = axes[i].collections[-1]
        tooltip = mpld3.plugins.PointLabelTooltip(points, labels=labels)
        mpld3.plugins.connect(fig, tooltip)

    handles, lbls = [], []
    for key, color in palette.items():
        handles.append(plt.Line2D([0], [0], marker='o', color='w', label=key,
                                markerfacecolor=color, markersize=10))
        lbls.append(key)

    fig.legend(handles, lbls, title="Stop Types", bbox_to_anchor=(1.05, 1), loc='upper left')

    # Add a global title and adjust layout so title is visible
    fig.suptitle("Points of Interest Around City Hall at Different Walking Distances", fontsize=20, y=1.02)

    plt.subplots_adjust(top=0.85)  # Adjust top to create space for the title if needed
    plt.tight_layout()

    mpld3.save_html(fig, "interactive_plot.html")
    mpld3.display()


############################################
# MAIN EXECUTION
############################################
if __name__ == "__main__":
    # Initialize DB if not present
    septa.init_db()
    pois.init_db()
    a = 25
    # store septa stops; each call stores 25 new stops
    # we can have up to 50 stops per stop type, which would be 150 total.
    # this would mean we would have to run this up to 6 times, which we do here.
    for i in range(NUM_SEPTA_RUNS):
        print(f"Fetching SEPTA stops {a-25} to {a}...")
        septa.store_septa_stops()
        a += 25
    a = 25
    # store POIs; each call stores 25 new POIs
    # we could run this up to 24 times, but that would take a long time, so 10-15 would be a good start.
    # the rule of thumb is to run this about 4 times more than the previous function.

    # note this function takes a long time since it waits ~1.1 seconds between each request, and there may be over 25 requests per call.
    total_pois = 0
    for i in range(NUM_POI_RUNS):
        print(f"Fetching POIs {a-25} to {a}...")
        b = pois.store_pois()
        total_pois += b
        a += 25
        if b < 25:
            print("No more POIs to fetch.")
            break
    print(f"Total POIs fetched: {total_pois}")

    # After enough data is collected (run script multiple times), run calculations:
    calculate_data()

    # Visualize data
    data, stop_dict = load_data_from_db()
    visualization(data, stop_dict)