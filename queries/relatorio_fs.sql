WITH fs_life AS(
  Select DISTINCT dtref AS dt_ref_life
  FROM `f1_2_silver`.`fs_f1_driver_life`
  order by 1
),

fs_last_10 AS(
  Select DISTINCT dtref AS dt_ref_10
  FROM `f1_2_silver`.`fs_f1_driver_last_10`
  order by 1
),

fs_last_20 AS(
  Select DISTINCT dtref AS dt_ref_20
  FROM `f1_2_silver`.`fs_f1_driver_last_20`
  order by 1
),

fs_last_40 AS(
  Select DISTINCT dtref AS dt_ref_40
  FROM `f1_2_silver`.`fs_f1_driver_last_40`
  order by 1
)

SELECT *
FROM fs_life AS t1
LEFT JOIN fs_last_10 AS t2 on t1.dt_ref_life = t2.dt_ref_10
LEFT JOIN fs_last_20 AS t3 on t1.dt_ref_life = t3.dt_ref_20
LEFT JOIN fs_last_40 AS t4 on t1.dt_ref_life = t4.dt_ref_40