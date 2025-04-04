import os
import base64
import pymysql
import pandas as pd
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta
from dotenv import load_dotenv
import tempfile

from meteostat import Daily,Point,Hourly

# Charger les variables d'environnement depuis le fichier .env
load_dotenv()

# Fonction pour ouvrir le tunnel SSH
def ouvrir_tunnel():
    ssh_host = '35.182.184.19'
    ssh_username = 'ec2-user'
    
    # Récupérer la clé SSH depuis les variables d'environnement
    pem_base64 = os.getenv('GEOPROJECTION_PEM')
    if pem_base64:
        # Décoder la clé Base64
        pem_key = base64.b64decode(pem_base64)
        
        # Créer un fichier temporaire pour la clé PEM
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            ssh_key_path = temp_file.name
            temp_file.write(pem_key)
    else:
        raise ValueError("La variable d'environnement GEOPROJECTION_PEM n'est pas définie")
    
    local_port = 23306
    remote_port = 3306
    
    # Création du tunnel SSH
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_username,
        ssh_pkey=ssh_key_path,
        remote_bind_address=(os.getenv("REMOTE_BIND_ADDRESS"), remote_port),
        local_bind_address=('0.0.0.0', local_port)
    )
    
    # Démarrer le tunnel
    tunnel.start()
    
    return tunnel


# Fonction d'extraction de données
def fetch_data_raw(tunnel, db_name, tb_name, debut, fin):
    db_host = '127.0.0.1'
    db_user = os.getenv('DB_USER')  # Récupérer le user de la base de données
    db_password = os.getenv('DB_PASSWORD')  # Récupérer le password depuis l'environnement
    local_port = tunnel.local_bind_port

    # Connexion à la base de données via le tunnel SSH
    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        db=db_name,
        port=local_port
    )

    try:
        # Exécution de la requête et extraction des données
        with connection.cursor() as cursor:
            
            sql_query = f"""
            SELECT *
            FROM {tb_name}
            WHERE date_creation BETWEEN '{debut.strftime("%Y-%m-%d %H:%M:%S")}' AND '{fin.strftime("%Y-%m-%d %H:%M:%S")}'
            """
            cursor.execute(sql_query)
            data = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

        # Conversion des données en DataFrame
        df = pd.DataFrame(data, columns=columns)
    finally:
        connection.close()
    
    return df


## fonction d'enregistrement des données brutes
def load_data_raw(db_name, tb_name, debut, fin, data_raw_path):
    tunnel = ouvrir_tunnel()

    # Appel de la fonction d'extraction
    data_raw = fetch_data_raw(tunnel, db_name, tb_name, debut, fin)


    # Dossier de sauvegarde (ex Data/raw/saint_esprit)
    data_raw_db_name_path=  os.path.join(data_raw_path, db_name)
    print('data_raw_db_name_path:', data_raw_db_name_path)
    
    # Création du dossier s'il n'existe pas
    os.makedirs(data_raw_db_name_path, exist_ok=True)

    # Génération du nom du fichier avec la période d'extraction
    nom_fichier = f"{tb_name}_{debut.strftime('%Y%m%d')}_{fin.strftime('%Y%m%d')}.csv"
    print("nom_fichier :",  nom_fichier)
    
    # Chemin complet du fichier
    chemin_fichier = os.path.join(data_raw_db_name_path, nom_fichier)


    # Sauvegarde du DataFrame en CSV
    data_raw.to_csv(chemin_fichier, index=False, encoding='utf-8')

    print(f"Données sauvegardées dans {chemin_fichier}")
    
    print(data_raw)

    # Fermeture du tunnel SSH
    tunnel.close()
    
    
#---------------------------------------------------------------------------------------------------------------# 
# Extraction des données de stesp_p_usine_fl et stockage dans                                                   #
# "C:\Users\ymarega\NORDIKeau\INO-PartenariatNordikeauINRSeau - INO-25-0001-Projet[Nordikeau-INRSeau]\Data\raw" #
#---------------------------------------------------------------------------------------------------------------#

# Définition du chemin de sauvegarde
data_raw_path = r"C:\Users\ymarega\NORDIKeau\INO-PartenariatNordikeauINRSeau - INO-25-0001-Projet[Nordikeau-INRSeau]\Data\raw"   
 
## Nom de la base de donnée     
#db_name = 'saint_esprit'

## Nom de la table
#tb_name = 'stesp_p_usine_fl'

### Extraction et sauvegarde des données de 2019-08-08 à 2021-01-15
#debut = datetime(2019, 8, 8, 10, 5)
#fin = datetime(2021, 1, 15, 14, 38, 11)
#load_data_raw(db_name, tb_name, debut, fin, data_raw_path)

### Extraction et sauvegarde des données de 2021-01-15 à 2025-03-13
#debut = datetime(2021, 1, 15, 14, 38, 11)
#fin = datetime(2025, 3, 13, 14, 6, 11)
#load_data_raw(db_name, tb_name, debut, fin, data_raw_path)

#---------------------------------------------------------------------------------------------------------------# 
# Extraction des données de saint_alexis.stale_p_usine_fl                                                       #
#                                                                                                               #
# "C:\Users\ymarega\NORDIKeau\INO-PartenariatNordikeauINRSeau - INO-25-0001-Projet[Nordikeau-INRSeau]\Data\raw" #
#---------------------------------------------------------------------------------------------------------------#

## Nom de la base de donnée        
#db_name = 'saint_alexis'

## Nom de la table
#tb_name = 'stale_p_usine_fl'

### Extraction et sauvegarde des données de 2020-03-11 à 2025-03-13
#debut = datetime(2020, 3, 11, 11, 15,31)
#fin = datetime(2025, 3, 13, 14, 6, 11)
#load_data_raw(db_name, tb_name, debut, fin, data_raw_path)

#---------------------------------------------------------------------------------------------------------------# 
# Extraction des données de saint_barthelemy.stbar_p_usine_fl                                                     #
#                                                                                                               #
# "C:\Users\ymarega\NORDIKeau\INO-PartenariatNordikeauINRSeau - INO-25-0001-Projet[Nordikeau-INRSeau]\Data\raw" #
#---------------------------------------------------------------------------------------------------------------#

## Nom de la base de donnée        
#db_name = 'saint_barthelemy'

## Nom de la table
#tb_name = 'stbar_p_usine_fl'

### Extraction et sauvegarde des données de 2021-08-09 à 2025-03-13
#debut = datetime(2021, 8, 9, 13, 44, 4)
#fin = datetime(2025, 3, 13, 14, 6, 11)
#load_data_raw(db_name, tb_name, debut, fin, data_raw_path)

#---------------------------------------------------------------------------------------------------------------# 
# Extraction des données de sainte_elisabeth.steel_p_usine_fl                                                   #
#                                                                                                               #
# "C:\Users\ymarega\NORDIKeau\INO-PartenariatNordikeauINRSeau - INO-25-0001-Projet[Nordikeau-INRSeau]\Data\raw" #
#---------------------------------------------------------------------------------------------------------------#

## Nom de la base de donnée        
#db_name = 'sainte_elisabeth'

## Nom de la table
#tb_name = 'steel_p_usine_fl'

### Extraction et sauvegarde des données de 2021-08-09 à 2025-03-13
#debut = datetime(2021, 8, 9, 13, 27, 4)
#fin = datetime(2025, 3, 13, 14, 6, 11)
#load_data_raw(db_name, tb_name, debut, fin, data_raw_path)

#---------------------------------------------------------------------------------------------------------------# 
# Extraction des données de lanoraie.lanor_p_usine_fl                                                  #
#                                                                                                               #
# "C:\Users\ymarega\NORDIKeau\INO-PartenariatNordikeauINRSeau - INO-25-0001-Projet[Nordikeau-INRSeau]\Data\raw" #
#---------------------------------------------------------------------------------------------------------------#

## Nom de la base de donnée        
#db_name = 'lanoraie'

## Nom de la table
#tb_name = 'lanor_p_usine_fl'

### Extraction et sauvegarde des données de 2021-10-05 à 2025-03-13
#debut = datetime(2021, 10, 5, 14, 35, 14)
#fin = datetime(2025, 3, 13, 14, 6, 11)
#load_data_raw(db_name, tb_name, debut, fin, data_raw_path)

#---------------------------------------------------------------------------------------------------------------# 
# Extraction des données de saint_esprit.stesp_u_vezina_fl et stockage dans                                                   #
# "C:\Users\ymarega\NORDIKeau\INO-DivisionInnovation - Data - RSDE\raw"                                         #
#---------------------------------------------------------------------------------------------------------------#


# Définition du chemin de sauvegarde
data_raw_path = r"C:\Users\ymarega\NORDIKeau\INO-DivisionInnovation - Data - RSDE\raw" 
 
## Nom de la base de donnée     
db_name = 'saint_esprit'

## Nom de la table
tb_name = 'stesp_u_vezina_fl'

### Extraction et sauvegarde des données de 2019-08-08 à 2021-01-15
debut = datetime(2023, 6, 19, 0, 0,1)
fin = datetime(2025, 3, 25, 3, 36, 11)
load_data_raw(db_name, tb_name, debut, fin, data_raw_path)


