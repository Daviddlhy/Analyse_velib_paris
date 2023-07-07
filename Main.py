import os
import mysql.connector
from datetime import datetime
from sqlalchemy import create_engine
import velib_func
import yaml
from yaml.loader import SafeLoader
import logging
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S',
                    level=logging.INFO,filename="/Users/daviddelhaye/Documents/Github/Analyse_velib_paris/velib.log")


# Lecture du fichier de config
with open('Config.yaml') as f:
    data_config = yaml.load(f, Loader=SafeLoader)
    f.close()

logging.info("Début du script.")
# lecture des dates from api Velib
url_data_status = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
url_data_informations = "https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"
df_stations_status, lastUpdatedOther_Station_statuts = velib_func.load_data(url_data_status)
df_station_information, lastUpdatedOther_Station_INFO = velib_func.load_data(url_data_informations)

# creation column to collect launch date with code
df_stations_status['running_code'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df_stations_status['last_update_table'] = lastUpdatedOther_Station_statuts

df_station_information.rental_methods = df_station_information.rental_methods.fillna('')
df_station_information.rental_methods = df_station_information.rental_methods.astype('str')

# Part to construct database
if all(df_stations_status['num_bikes_available'] == df_stations_status['numBikesAvailable']) & all(
        df_stations_status.num_docks_available == df_stations_status.numDocksAvailable):
    logging.info("Dim du df créé %s",df_stations_status.shape)
    df_stations_status.drop(columns=["numBikesAvailable", "numDocksAvailable"], inplace=True)
else:
    logging.ERROR("Error not sames columns")
    print("Error not sames columns")

df_stations_status.num_bikes_available_types = df_stations_status.num_bikes_available_types.astype('str')
df_stations_status['test1'] = df_stations_status.num_bikes_available_types.apply(velib_func.find_num_bikes)
df_stations_status[['Nb_bikes_mechanical', 'Nb_bikes_ebike']] = df_stations_status.test1.str.split(",", expand=True)
df_stations_status.drop(columns="test1", inplace=True)
df_stations_status.Nb_bikes_ebike = df_stations_status.Nb_bikes_ebike.astype('int')
df_stations_status.Nb_bikes_mechanical = df_stations_status.Nb_bikes_mechanical.astype('int')

assert all(df_stations_status.num_bikes_available == df_stations_status.Nb_bikes_mechanical +
           df_stations_status.Nb_bikes_ebike), "Erreur pas égalité"
df_stations_status.drop(columns=['num_bikes_available_types'], inplace=True)
df_stations_status['last_reported'] = df_stations_status.last_reported.apply(
    lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S"))

logging.info("La date de MAJ min est: %s", min(df_stations_status.last_reported))
logging.info("La date de MAJ max est: %s",max(df_stations_status.last_reported))
print(f"La date de MAJ min est: {min(df_stations_status.last_reported)}")
print(f"La date de MAJ max est: {max(df_stations_status.last_reported)}")

# Connexion on MySQL on Local
logging.info("Connexion à la base de donnée.")
db = mysql.connector.connect(
    host="localhost",
    user=data_config['user'],
    password=data_config['mdp_mysql'],
    database=data_config['bdd']
)

query_sql = """
CREATE TABLE IF NOT EXISTS station_information (
station_id BIGINT PRIMARY KEY  NOT NULL,
name VARCHAR(256),
lat VARCHAR(100),
lon VARCHAR(100),
capacity BIGINT,
stationCode VARCHAR(200),
rental_methods VARCHAR(200)
)
"""
with db.cursor() as c:
    c.execute(query_sql)

query_sql2 = """
TRUNCATE TABLE `station_information`
"""

with db.cursor() as c:
    c.execute(query_sql2)

cols = "`,`".join([str(i) for i in df_station_information.columns.tolist()])

print(cols)

with db.cursor() as c:
    for i, row in df_station_information.iterrows():
        sql = "INSERT INTO `station_information` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
        c.execute(sql, tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our changes
        db.commit()
    c.close()
    print("MySQL connection is closed")

# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user=data_config['user'],
                               pw=data_config['mdp_mysql'],
                               db=data_config['bdd']))

del df_stations_status['last_update_table']
# Insert whole DataFrame into MySQL
df_stations_status.to_sql('stations_statuts', con=engine, if_exists='append', chunksize=1000, index=False)
logging.info("Script done")
print("Script Done=============")
