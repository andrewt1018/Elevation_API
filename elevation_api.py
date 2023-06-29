import pickle
import math
import psycopg2
import subprocess
from subprocess import PIPE, Popen
from pathlib import Path
from fastapi import FastAPI

app = FastAPI()

# Local credentials
hostname = "localhost"
db = "postgres"
port = 5432
username = "postgres"
password = "Volley&drums2"

@app.get("/elevation_api")
async def query(lat: float, long: float):
    coord = (lat, long)
    file = open("fl_files.pkl", "rb")
    files = pickle.load(file)
    projs = find_project(coord)
    for proj in projs:
        file_list = order_files(coord, proj[0], files)
        for file in file_list:
            elevation = query_elevation(coord, file[0])
            if elevation > 0:
                return {"File": file[0], "Elevation: ": elevation}

# All of the helper functions to query the elevation of a
# given coordinate (if exists in database)
# Follows the following steps:
#     
#     1. Function to locate which specific project contains the coordinate on a database level --> Project name
#     2. Function to locate which specific TIF file contains the coordinate on a project level --> TIF file
#     3. Check if the TIF file already exists inside our database. If exist, jump to step 6
#     4. Function to return the presigned URL of the TIF file through aws cli --> presigned URL
#     5. Function to load the specified TIF file into our database
#     6. Script to interact with postgresql and query the elevation --> elevation
#     

def find_project(coord: tuple) -> str:
    """Find the project that a coordinate belongs to from the USGS database"""
    query = ("SELECT usgs_shape_file.project FROM usgs_shape_file "\
          + "WHERE ST_Intersects(usgs_shape_file.geom, ST_SetSRID(ST_MakePoint{}, 4326));").format(coord)
    connection = psycopg2.connect(user=username, password=password, host=hostname, port=str(port), database=db)
    cursor = connection.cursor()
    try:  
        # Executing query
        cursor.execute(query)
        results = cursor.fetchall()
        
        if len(results) == 0:
            print("No projects contain the coordinate")
            return -1
        return results
        
    except (psycopg2.Error) as error:
        print("Error occured when querying: ", error)
        return -2

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()

def calc_distance(coord1: tuple, coord2: tuple) -> float:
    """Calculates the distance between two coordinates"""
    return math.sqrt( (coord1[0]-coord2[0]) ** 2 + (coord1[1]-coord2[1]) ** 2 )

def order_projs(coord: tuple, projects) -> list:
    """
    Returns a list of all projects ordered by how close each project's centroid is to the given coordinate.
    """
    ret = [(projects.iloc[0]["Project"], calc_distance(projects.iloc[0]["Centroid"], coord))]
    for i in range(1, len(projects)):
        proj = projects.iloc[i]["Project"]
        distance = calc_distance(projects.iloc[i]["Centroid"], coord)
        for i in range(len(ret)):
            if ret[i][1] < distance:
                if i == (len(ret) - 1):
                    ret.append((proj, distance))
                    break
                continue
            else:
                ret.insert(i, (proj, distance))
                break
    return ret
        
def order_files(coord: tuple, proj, projects) -> list:
    """
    Returns a list of all files in a given project ordered by 
    how close each file's centroid is to the given coordinate.
    """
    files = projects.loc[projects["Project"] == proj]
    if len(files) == 0:
        print("Project {} cannot be found".format(proj))
        return -1
    ret = [(files.iloc[0]["File"], calc_distance((files.iloc[0]["Lat"], files.iloc[0]["Long"]), coord))]
    for i in range(1, len(files)):
        file = files.iloc[i]["File"]
        distance = calc_distance((files.iloc[i]["Lat"], files.iloc[i]["Long"]), coord)
        for i in range(len(ret)):
            if ret[i][1] < distance:
                if i == (len(ret) - 1):
                    ret.append((file, distance))
                    break
                continue
            else:
                ret.insert(i, (file, distance))
                break
    return ret
    
def get_presigned_url(file: str) -> str:
    """
    Returns the presigned url of a given file name from our "rasters-for-outdb" s3 bucket
    """
    command = ("""aws s3 presign s3://rasters-for-outdb/{} """\
            + """--expires-in 604800 --profile andrew.tan@tmhighland.com""").format(file)
    ret = subprocess.check_output(command, shell=True).decode('utf-8')
    return ret

def exists_in_database(file: str):
    file = file.replace(".tif", "").replace(".TIF", "").lower()
    query = "SELECT * FROM pg_catalog.pg_tables WHERE schemaname='public'"
    query = "SELECT table_name FROM information_schema.tables WHERE table_type='BASE TABLE' AND table_schema='public';"
    
    connection = psycopg2.connect(user=username, password=password, host=hostname, port=str(port), database=db)
    cursor = connection.cursor()
    try:  
        # Executing query
#         print("Querying tablenames from {} database...".format(db))
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Return a boolean saying whether a file is in the list of tables in the database
        if len(results) == 0:
            return False
        for deets in results:
            if file in deets:
                return True
        return False

    except (psycopg2.Error) as error:
        print("Error occured when querying tablenames")
        return -2

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()

def upload_outdb_raster(presigned_url: str, table_name: str, hostname: str, db: str, username: str, port: int, password: str):
    """
    Takes in a presigned url of a raster file as well as credentials to a postgreSQL database
    Creates a .sql file containing the commands to upload the raster file as a table into the database
    Deletes the .sql file
    """
    # Preparing raster for upload
    print("Table name: ", table_name)
    out_file = "out.sql"
    raster_cmd = (r"""raster2pgsql -F -I -C -s 26916 -t auto -R /vsicurl/{} {} > """\
               + r"""{}""").format(make_raw(presigned_url), table_name, out_file)
    
    # Running command to produce the .sql file
    proc = subprocess.Popen(raster_cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out = b'output init'
    err = b'error init'
    
    try:
        print("Making .sql file...")
        out, err = proc.communicate(timeout=300)
    except:
        print("Timed out. Aborting upload.")
        return
    print("out: {}, err: {}".format(out.decode('utf-8'), err.decode('utf-8')))
    
    if not Path(out_file).is_file():
        with open("out.sql", "w") as f:
            f.write(out.decode('utf-8'))
            f.close()
    
    # Connecting to database and uploading file
    try:
        # Connecting to the database
        print("Connecting to {} database".format(db))
        connection = psycopg2.connect(user=username, password=password, host=hostname, port=str(port), database=db)
        
        # Executing the .sql file
        with connection.cursor() as cursor:
            print("Uploading raster...")
            cursor.execute(open(out_file, "r").read())
            print("Uploaded successfully!")
        
    except (Exception, psycopg2.Error) as error:
        print("Error: ", error)
        print("Aborting upload.")
        return
    
    finally:
        # Deleting the .sql file
        proc_del = subprocess.Popen("del {}".format(out_file), shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        print("Deleted the temporary .sql file")
        
        # Closing database connection.
        if connection:
            cursor.close()
            connection.close()

def make_raw(string: str):
    return string.replace('&', '^&').replace('=', '^=')

def add_slashes(string: str):
    return string.replace('/', '//')

def send_elevation_query(query: str, hostname: str, db: str, username: str, port, password: str) -> float:
    """
    Given an elevation query as well as a postgreSQL server credentials, 
    execute the query and return the elevation if obtained, else return -1
    """
    
    connection = psycopg2.connect(user=username, password=password, host=hostname, port=str(port), database=db)
    cursor = connection.cursor()
    try:  
        # Executing query
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Return the first element of the results <class 'list'>
        if len(results) == 0:
            return -1
        return results[0][0]

    except (Exception, psycopg2.Error) as error:
        print("Error: ", error)
        return -2

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()

def query_elevation(coordinates: tuple, file: str) -> float:
    """
    Query the elevation of a given coordinate and a given TIF file
    If no elevation data is found for the coord, return -1
    """
    table_name = file.replace(".tif", "")
    
    # STEP 3
    if not exists_in_database(file):
        print("File does not exist in {} database yet".format(db))
        # STEP 4
        # Interacting with AWS cli to grab a presigned url for the tif file we need
        presigned = get_presigned_url(file).replace("https", "http")

        # STEP 5
        # Uploading outdb file to postgres DB inside schema = "rasters"
        upload_outdb_raster(presigned, table_name, hostname, db, username, port, password)
    print("{} is in {} database".format(file, db))
    
    # STEP 6
    # Sending the query
    x, y = coordinates
    query = ("SELECT ST_Value(rast, ST_Transform(ST_SetSRID(ST_MakePoint({},{}),4269), "\
          + "(SELECT ST_SRID(rast) FROM {} LIMIT 1)))*3.28084 FROM {} "\
          + "WHERE ST_Intersects(rast, "\
          + "ST_Transform(ST_SetSRID(ST_MakePoint({},{}), 4269),26916));").format(x, y, table_name, table_name, x, y)
    elevation = send_elevation_query(query, hostname, db, username, port, password)
    
    if elevation == -1:
        print("No elevation data available")
    elif elevation == -2:
        print("Error occured when querying the database")
    return elevation
