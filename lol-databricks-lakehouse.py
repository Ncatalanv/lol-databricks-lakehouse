# Databricks notebook source
# MAGIC %md
# MAGIC # **Capa Bronze** 
# MAGIC ## **Raw Data - Datos crudos e inmutables**

# COMMAND ----------

#Capa Bronze para analizar partidas de mejores jugadores de LAS
import requests
import json
import time
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp

#Configuración
API_KEY = dbutils.secrets.get(scope="riot_api", key="api_key")
LIMITE_JUGADORES = 200
PARTIDAS_POR_JUGADOR = 50

#Funciones de extracción con rate limiting
def get_riot_data(url):
    #Hace petición a la API y maneja el rate limiting (429)
    while True:
        respuesta = requests.get(url)
        if respuesta.status_code == 200:
            return respuesta.json()
        elif respuesta.status_code == 429:
            tiempo_espera = int(respuesta.headers.get("Retry-After", 10))
            print(f"Límite alcanzado. Pausando extracción por {tiempo_espera} segundos")
            time.sleep(tiempo_espera)
        else:
            print(f"Error {respuesta.status_code} en URL: {url}")
            return None

#Pipeline de extracción masiva
print("Obteniendo lista de jugadores Challenger en LAS...")
url_challenger = f"https://la2.api.riotgames.com/lol/league/v4/challengerleagues/by-queue/RANKED_SOLO_5x5?api_key={API_KEY}"
challenger_data = get_riot_data(url_challenger)

#Validar que me entregó la API   
if challenger_data is None or 'entries' not in challenger_data:
    print(f"Error crítico o la API no devolvió jugadores. Respuesta {challenger_data}")
    puuids = []
else:
    #Extraer puuid 
    puuids = [jugador['puuid'] for jugador in challenger_data['entries']][:LIMITE_JUGADORES]

datos_crudos = []
partidas_procesadas = set() #Para no duplicar partidas si es que jugaron juntos

print(f"Iniciando extracción de partidas para {len(puuids)} jugadores...")

for idx, puuid in enumerate(puuids):
    #Obtener las últimas 50 partidas del jugador
    url_matches = f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids?start=0&count={PARTIDAS_POR_JUGADOR}&api_key={API_KEY}"
    match_ids = get_riot_data(url_matches)

    if not match_ids: continue

    print(f"[{idx+1}/{len(puuids)}] Extrayendo {len(match_ids)} partidas de PUUID: {puuid[:8]}...")

    #Obtener el detalle de cada partida
    for match_id in match_ids:
        #Evitar extraer la misma partida
        if match_id in partidas_procesadas: continue

        url_match = f"https://americas.api.riotgames.com/lol/match/v5/matches/{match_id}?api_key={API_KEY}"
        match_detail = get_riot_data(url_match)

        if match_detail:
            #Guardar el JSON crudo
            datos_crudos.append((match_id, json.dumps(match_detail)))
            partidas_procesadas.add(match_id)
        
        #Pausa preventiva de 1.2s para no ahogar la API de desarrollo (100req / 120s)
        time.sleep(1.2)

print(f"Extracción finalizada. {len(datos_crudos)} partidas únicas listas para ingesta.")

#Ingesta a la Capa Bronze
if datos_crudos:
    schema_bronze = StructType([
        StructField("match_id", StringType(), True),
        StructField("raw_json", StringType(), True)
    ])

    df_bronze = spark.createDataFrame(datos_crudos, schema=schema_bronze)
    df_bronze = df_bronze.withColumn("ingestion_timestamp", current_timestamp())

    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("bronze_lol_matches")
    
    print("Datos guardados en Delta Lake (Tabla: bronze_lol_matches)")
else:
    print("No se extrajeron datos...")


# COMMAND ----------

# MAGIC %md
# MAGIC # **Capa Silver**
# MAGIC ## Datos Curados

# COMMAND ----------

from pyspark.sql.types import ArrayType, StructType, StructField, StringType, IntegerType, BooleanType
from pyspark.sql.functions import col, get_json_object, from_json, explode, when, round

#Leer Capa Bronze
df_bronze = spark.read.table("bronze_lol_matches")

#Definir el esquema --> estructura exacta del diccionario del jugador dentro del JSON
participant_schema = ArrayType(StructType([
    StructField("puuid", StringType(), True),
    StructField("riotIdGameName", StringType(), True),
    StructField("riotIdTagline", StringType(), True),
    StructField("championName", StringType(), True),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("totalDamageDealtToChampions", IntegerType(), True),
    StructField("win", BooleanType(), True)
]))

#Transformación (aplanar JSON)

#Extraer soo el arreglo "participants" del JSON crudo
df_parsed = df_bronze.withColumn("participants_array_str", get_json_object(col("raw_json"), "$.info.participants"))

#Convertir ese texto a un Array real de Spark y lo 'exploto' (1 fila --> 10 filas)
df_exploded = df_parsed.withColumn("jugador", explode(from_json(col("participants_array_str"), participant_schema)))

#Seleccionar las columnas de forma limpia
df_silver_base = df_exploded.select(
    col("match_id"),
    col("jugador.puuid").alias("puuid"),
    col("jugador.riotIdGameName").alias("game_name"),
    col("jugador.riotIdTagline").alias("tagline"),
    col("jugador.championName").alias("champion"),
    col("jugador.kills").alias("kills"),
    col("jugador.deaths").alias("deaths"),
    col("jugador.assists").alias("assists"),
    col("jugador.totalDamageDealtToChampions").alias("total_damage"),
    col("jugador.win").alias("win")
)

#Lógica de negocio (calcular KDA)
#KDA = (kills + assists) / deaths (si es 0, se divide por 1 para evitar errores)
df_silver = df_silver_base.withColumn(
    "kda",
    round(col("kills") + col("assists") / when(col("deaths") == 0, 1).otherwise(col("deaths")), 2)
)

#Guardar en Capa Silver
#Guardar como tabla gestionada en formato Delta
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_lol_matches")

print("Capa Silver procesada y guardada con éxito!")
display(spark.sql("SELECT * FROM silver_lol_matches"))


# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # **Capa Gold**
# MAGIC ## Métricas

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg, round, col

#Leer Capa Silver
df_silver = spark.read.table("silver_lol_matches")

#Meta de campeones --> agrupar por campeón para ver cual domina
df_gold_champions = df_silver.groupBy("champion").agg(
    count("*").alias("partidas_jugadas"),
    #Conversión de 'win' a 1 o 0 para trabjar con el
    round((sum(col("win").cast("int")) / count("*")) * 100, 2).alias("win_rate_pct"),
    round(avg("kda"), 2).alias("kda_promedio"),
    round(avg("total_damage"), 0).alias("dano_promedio_partida")
).orderBy(col("partidas_jugadas").desc(), col("win_rate_pct").desc())

#Guardar la primera tabla Gold
df_gold_champions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_champion_meta")

#Rendimiento de jugadores --> agrupar por jugador para crear ranking
df_gold_players = df_silver.groupBy("puuid", "game_name", "tagline").agg(
    count("*").alias("total_partidas"),
    sum(col("win").cast("int")).alias("victorias"),
    round((sum(col("win").cast("int")) /count("*")) * 100, 2).alias("win_rate_pct"),
    round(avg("kda"), 2).alias("kda_promedio")
    #Mínimo de partidas
).filter(col("total_partidas") >= 20) \
.orderBy(col("total_partidas").desc(), col("win_rate_pct").desc(), col("kda_promedio").desc())

#Guadar la segunda tabla Gold
df_gold_players.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_player_stats")

print("Tabla Gold generada exitosamente!")

display(spark.sql("SELECT * FROM gold_champion_meta"))
display(spark.sql("SELECT * FROM gold_player_stats"))