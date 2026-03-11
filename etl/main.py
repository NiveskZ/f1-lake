# %%
import os
import dotenv
dotenv.load_dotenv()

import nekt

from tqdm import tqdm

# %%

nekt.data_access_token = os.getenv("NEKT_ACCESS_TOKEN")
# %%
# Custom imports
import nekt

query_dates = """
SELECT
  DISTINCT date(date) AS dt_ref
FROM `f1_results`
WHERE date = '{year}'
Order by 1
"""

# Feature Store 
query = """

WITH 
  results_until_date AS (
    SELECT * 
    FROM `f1_results`
    WHERE 
      date(date) <= date('{date}')
    ORDER BY
      date Desc
  ),
  drivers_selected AS (
    SELECT DISTINCT
      DriverId
    From
      results_until_date
    WHERE
      YEAR >= (
        SELECT
          MAX(YEAR) - 2
        FROM
          results_until_date
      )
  ),
  tb_result AS (
    SELECT t1.*
    FROM 
      results_until_date AS t1
    INNER JOIN
      drivers_selected AS t2
    ON t1.DriverId = t2.DriverId
    ORDER BY Year
  ),
  tb_life AS (
    SELECT
      DriverId,
      COUNT(DISTINCT Year) AS qtde_seasons,
      COUNT(*) AS qtde_sessions,
      SUM(
        CASE WHEN status = 'Finished' OR status LIKE '+%' THEN 1 ELSE 0 END
      ) AS qtde_sessions_finished,
      SUM(
        CASE
          WHEN MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtde_race,
      SUM(
        CASE WHEN (status = 'Finished' OR status LIKE '+%') AND MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtde_sessions_finished_race,
      SUM(
        CASE
          WHEN MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtde_sprint,
      SUM(
        CASE WHEN (status = 'Finished' OR status LIKE '+%') AND MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtde_sessions_finished_sprint,
      SUM(
        CASE 
          WHEN position = 1 THEN 1 ELSE 0 END
      ) AS qtde_1pos,
      SUM(
        CASE 
          WHEN position = 1 AND MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtde_1pos_race,
      SUM(
        CASE 
          WHEN position = 1 AND MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtde_1pos_sprint,
      SUM(
        CASE 
          WHEN position <= 3 THEN 1 ELSE 0 END
      ) AS qtde_podios,
      SUM(
        CASE 
          WHEN position <= 3 AND MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtde_podios_race,
      SUM(
        CASE 
          WHEN position <= 3 AND MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtde_podios_sprint,
      SUM(
        CASE 
          WHEN position <= 5 THEN 1 ELSE 0 END
      ) AS qtde_pos5,
      SUM(
        CASE 
          WHEN position <= 5 AND MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtde_pos5_race,
      SUM(
        CASE 
          WHEN position <= 5 AND MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtde_pos5_sprint,
      SUM(
        CASE 
          WHEN gridposition <= 5 THEN 1 ELSE 0 END
      ) AS qtde_gridpos5,
      SUM(
        CASE 
          WHEN gridposition <= 5 AND MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtde_gridpos5_race,
      SUM(
        CASE 
          WHEN gridposition <= 5 AND MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtde_gridpos5_sprint,
      SUM(
        points
      ) AS qtde_points,
      SUM(
        CASE 
          WHEN MODE = 'Race' THEN points END
      ) AS qtde_points_race,
      SUM(
        CASE 
          WHEN MODE = 'Sprint' THEN points END
      ) AS qtde_points_sprint,
      AVG(
        gridposition
      ) AS avg_gridposition,
      AVG(
        CASE 
          WHEN MODE = 'Race' THEN gridposition END
      ) AS avg_gridposition_race,
      AVG(
        CASE 
          WHEN MODE = 'Sprint' THEN gridposition END
      ) AS avg_gridposition_sprint,
      AVG(
        position
      ) AS avg_position,
      AVG(
        CASE 
          WHEN MODE = 'Race' THEN position END
      ) AS avg_position_race,
      AVG(
        CASE 
          WHEN MODE = 'Sprint' THEN position END
      ) AS avg_position_sprint,
      SUM(
        CASE
          WHEN gridposition = 1 THEN 1 ELSE 0 END
      ) AS qtde_1_gridposition,
      SUM(
        CASE
          WHEN gridposition = 1 AND MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtde_1_gridposition_race,
      SUM(
        CASE
          WHEN gridposition = 1 AND MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtde_1_gridposition_sprint,
      SUM(
        CASE
          WHEN gridposition = 1 AND position = 1 THEN 1 ELSE 0 END
      ) AS qtde_pole_win,
      SUM(
        CASE
          WHEN gridposition = 1 AND position = 1 AND MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtde_pole_win_race,
      SUM(
        CASE
          WHEN gridposition = 1 AND position = 1 AND MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtde_pole_win_sprint,
      SUM(
        CASE 
          WHEN points > 0 THEN 1 ELSE 0 END
      ) AS qtd_sessions_with_points,
      SUM(
        CASE 
          WHEN MODE = 'Race' AND points > 0 THEN 1 ELSE 0 END
      ) AS qtd_sessions_with_points_race,
      SUM(
        CASE 
          WHEN MODE = 'Sprint' AND points > 0 THEN 1 ELSE 0 END
      ) AS qtd_sessions_with_points_sprint,
      SUM(
        CASE 
          WHEN position < gridposition THEN 1 ELSE 0 END
      ) AS qtd_sessions_with_overtake,
      SUM(
        CASE 
          WHEN position < gridposition AND MODE = 'Race' THEN 1 ELSE 0 END
      ) AS qtd_sessions_with_overtake_race,
      SUM(
        CASE 
          WHEN position < gridposition AND MODE = 'Sprint' THEN 1 ELSE 0 END
      ) AS qtd_sessions_with_overtake_sprint,
      AVG(
        gridposition - position
      ) AS avg_overtake,
      AVG(
        CASE 
          WHEN MODE = 'Race' THEN gridposition - position END
      ) AS avg_overtake_race,
      AVG(
        CASE
          WHEN MODE = 'Sprint' THEN gridposition - position END
      ) AS avg_overtake_sprint
      
    FROM
      tb_result
    GROUP BY
      DriverId
  )
SELECT 
  date('{date}') AS dtRef,
  *
FROM tb_life
ORDER BY DriverId

"""

# Carregamento da tabela
(nekt.load_table(layer_name="Bronze", table_name="f1_results")
     .createOrReplaceTempView("f1_results"))

# Sessão Spark
spark = nekt.get_spark_session()

# Listando todos os anos para fazer o union
years = list(range(1990,2025))

# Selecionando todas as datas
for y in years:
    dates = spark.sql(query_dates.format(year=y)).toPandas()["dt_ref"].astype(str).tolist()
    df_all = spark.sql(query.format(date=dates.pop(0)))

    for dt in tqdm(dates):
        df_all = df_all.union(spark.sql(query.format(date=dt)))

    # Salva o dataframe
    nekt.save_table(
      df = df_all,
      layer_name="Silver",
      table_name="fs_f1_driver_life",
      folder_name="f1" 
    )


