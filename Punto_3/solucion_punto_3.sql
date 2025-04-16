/*revision datos*/
describe work_sas.stiv_pruebaid_rachas;
describe work_sas.stiv_pruebaid_retiros;

WITH corte_mes_distintas as (
    SELECT distinct cast(to_timestamp (corte_mes,'yyyy-MM-dd')as date) as corte_mes
    FROM work_sas.stiv_pruebaid_rachas
),
identificacion_distintos as (
    SELECT distinct identificacion
    FROM work_sas.stiv_pruebaid_rachas
),
combinaciones as (
    SELECT distinct corte_mes, identificacion 
    FROM corte_mes_distintas b1
    LEFT JOIN identificacion_distintos b2
    ON 1=1
),
primera_corte_mes_observada as (
    SELECT identificacion, min(cast(to_timestamp (corte_mes,'yyyy-MM-dd')as date)) as minima_corte_mes_observacion
    FROM work_sas.stiv_pruebaid_rachas
    GROUP BY identificacion
),
bd_retiro as(
	Select identificacion, cast(to_timestamp (fecha_retiro,'yyyy-MM-dd')as date) as fecha_retiro
	from work_sas.stiv_pruebaid_retiros
),
bd_historia_completa as (
    SELECT 
    b1.corte_mes as corte_mes,
    b1.identificacion, 
    (case 
    when b2.saldo is null then 'N0'
    when b2.saldo <300000 then 'N0'
    when b2.saldo >=300000 and b2.saldo <1000000 then 'N1' 
    when b2.saldo >=1000000 and b2.saldo <3000000 then 'N2' 
    when b2.saldo >=3000000 and b2.saldo <5000000 then 'N3' 
    when b2.saldo >=5000000 then 'N4'
    else 'N0'
    end) as saldo
    FROM combinaciones b1
    LEFT JOIN work_sas.stiv_pruebaid_rachas b2
    ON b1.corte_mes = cast(to_timestamp (b2.corte_mes,'yyyy-MM-dd')as date)
        AND b1.identificacion = b2.identificacion
    LEFT JOIN primera_corte_mes_observada b3
    ON b1.identificacion = b3.identificacion
    LEFT JOIN bd_retiro b4
    on b1.identificacion = b4.identificacion
    WHERE 1=1
    AND b1.corte_mes >= b3.minima_corte_mes_observacion and b1.corte_mes <= b4.fecha_retiro
      /*  AND b1.corte_mes <= '2023-03-05' FILTRO MENOR A corte_mes*/
),
bd_racha AS (
    SELECT *, LAG(saldo, 1) OVER(PARTITION BY identificacion ORDER BY corte_mes ASC) AS saldo_p1
    FROM bd_historia_completa
),
bd_regla_gap AS (
    SELECT *,
    CASE 
    WHEN saldo_p1 is null THEN 1
    WHEN saldo = saldo_p1 THEN 1
    ELSE 0 
    END as binaria
    FROM bd_racha 
),
bd_numerador_rachas AS (
    SELECT 
    identificacion,
    corte_mes,
    saldo,
    binaria,
    (ROW_NUMBER() OVER (PARTITION BY identificacion ORDER BY corte_mes ASC) - SUM(binaria) OVER (PARTITION BY identificacion ORDER BY corte_mes ASC)) as ranking_rachas
    FROM bd_regla_gap
),
bd_conteo_rachas as (
    SELECT identificacion, ranking_rachas, saldo, 
    min(corte_mes) as min_corte_mes, max(corte_mes) as max_corte_mes, count(*) as conteo 
    FROM bd_numerador_rachas
    GROUP BY identificacion, ranking_rachas, saldo
),
bd_orden_rachas as (
    SELECT *, 
    row_number() over(partition by identificacion order by conteo desc, max_corte_mes desc) as orden_racha 
    FROM bd_conteo_rachas  
)
SELECT identificacion, conteo as racha, min_corte_mes as fecha_inicio, max_corte_mes as fecha_fin, saldo as nivel 
FROM bd_orden_rachas
WHERE orden_racha=1
