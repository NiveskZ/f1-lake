# %%
import os
import dotenv
dotenv.load_dotenv()

import nekt

nekt.data_access_token = os.getenv('NEKT_ACCESS_TOKEN')

# Iniciando Sessão Spark
spark = nekt.get_spark_session()

# %%
# Carregando as tabelas da query
(nekt.load_table(layer_name="Silver", table_name="fs_f1_driver_all")
     .createOrReplaceTempView("fs_f1_driver_all"))

(nekt.load_table(layer_name="Silver", table_name="f1_champions")
     .createOrReplaceTempView("f1_champions"))

query = """
WITH tb_abt AS (
  SELECT t1.*,
        coalesce(t2.rankdriver,0) AS flchampion
  FROM `fs_f1_driver_all` AS t1

  LEFT JOIN `f1_champions` AS t2
  ON t1.driverid_life = t2.driverid
  AND EXTRACT(YEAR FROM t1.dtref_life) = t2.Year

  WHERE t1.dtref_life >= date('2000-01-01')
  AND t1.dtref_life < date('2026-01-01')
)

SELECT * FROM tb_abt
"""

df = spark.sql(query).toPandas()

df.to_csv("../data/abt_f1_drivers_champion.csv", index = False, sep=";")
# %%
