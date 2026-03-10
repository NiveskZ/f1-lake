WITH year_driver_points AS (
  SELECT
    CAST(YEAR AS INT64) AS Year,
    DriverId,
    SUM(Points) AS totalPoints
  FROM `f1_2_bronze`.`f1_results`
  WHERE mode IN ('Race', 'Sprint')
  GROUP BY DriverId, Year
  ORDER BY Year, totalPoints DESC
),

rn_year_driver AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY Year ORDER BY totalPoints DESC) AS rankDriver
  FROM year_driver_points
)

SELECT *
FROM rn_year_driver
WHERE rankDriver = 1