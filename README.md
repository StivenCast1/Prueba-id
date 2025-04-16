# Prueba-id

## Paso a Paso para el Punto 3

### Utilización de la clase `df_upload_sql`

En el punto 2, se utilizó la clase `df_upload_sql` para cargar las dos hojas del Excel a la bodega en la librería `work_sas`

### 1. Crear tablas temporales con datos distintos

```sql
WITH corte_mes_distintas AS (
    SELECT DISTINCT CAST(TO_TIMESTAMP(corte_mes, 'yyyy-MM-dd') AS DATE) AS corte_mes
    FROM work_sas.stiv_pruebaid_rachas
),
identificacion_distintos AS (
    SELECT DISTINCT identificacion
    FROM work_sas.stiv_pruebaid_rachas
)
```

### 2. Generar combinaciones de corte_mes e identificacion

```sql
combinaciones AS (
    SELECT DISTINCT corte_mes, identificacion 
    FROM corte_mes_distintas b1
    LEFT JOIN identificacion_distintos b2 ON 1=1
)
```

### 3. Obtener la primera fecha de corte_mes observada por identificacion

```sql
primera_corte_mes_observada AS (
    SELECT identificacion, MIN(CAST(TO_TIMESTAMP(corte_mes, 'yyyy-MM-dd') AS DATE)) AS minima_corte_mes_observacion
    FROM work_sas.stiv_pruebaid_rachas
    GROUP BY identificacion
)
```

### 4. Obtener la fecha de retiro por identificacion

```sql
bd_retiro AS (
    SELECT identificacion, CAST(TO_TIMESTAMP(fecha_retiro, 'yyyy-MM-dd') AS DATE) AS fecha_retiro
    FROM work_sas.stiv_pruebaid_retiros
)
```

### 5. Crear la tabla bd_historia_completa con las combinaciones y condiciones

```sql
bd_historia_completa AS (
    SELECT 
        b1.corte_mes AS corte_mes,
        b1.identificacion, 
        CASE 
            WHEN b2.saldo IS NULL THEN 'N0'
            WHEN b2.saldo < 300000 THEN 'N0'
            WHEN b2.saldo >= 300000 AND b2.saldo < 1000000 THEN 'N1' 
            WHEN b2.saldo >= 1000000 AND b2.saldo < 3000000 THEN 'N2' 
            WHEN b2.saldo >= 3000000 AND b2.saldo < 5000000 THEN 'N3' 
            WHEN b2.saldo >= 5000000 THEN 'N4'
            ELSE 'N0'
        END AS saldo
    FROM combinaciones b1
    LEFT JOIN work_sas.stiv_pruebaid_rachas b2 ON b1.corte_mes = CAST(TO_TIMESTAMP(b2.corte_mes, 'yyyy-MM-dd') AS DATE) AND b1.identificacion = b2.identificacion
    LEFT JOIN primera_corte_mes_observada b3 ON b1.identificacion = b3.identificacion
    LEFT JOIN bd_retiro b4 ON b1.identificacion = b4.identificacion
    WHERE 1=1
    AND b1.corte_mes >= b3.minima_corte_mes_observacion AND b1.corte_mes <= b4.fecha_retiro
)
```

### 6. Crear la tabla bd_racha con el saldo anterior

```sql
bd_racha AS (
    SELECT *, LAG(saldo, 1) OVER(PARTITION BY identificacion ORDER BY corte_mes ASC) AS saldo_p1
    FROM bd_historia_completa
)
```

### 7. Crear la tabla bd_regla_gap con la columna binaria

```sql
bd_regla_gap AS (
    SELECT *,
    CASE 
        WHEN saldo_p1 IS NULL THEN 1
        WHEN saldo = saldo_p1 THEN 1
        ELSE 0 
    END AS binaria
    FROM bd_racha 
)
```

### 8. Crear la tabla bd_numerador_rachas con el ranking de rachas

```sql
bd_numerador_rachas AS (
    SELECT 
        identificacion,
        corte_mes,
        saldo,
        binaria,
        (ROW_NUMBER() OVER (PARTITION BY identificacion ORDER BY corte_mes ASC) - SUM(binaria) OVER (PARTITION BY identificacion ORDER BY corte_mes ASC)) AS ranking_rachas
    FROM bd_regla_gap
)
```

### 9. Crear la tabla bd_conteo_rachas con el conteo de rachas

```sql
bd_conteo_rachas AS (
    SELECT identificacion, ranking_rachas, saldo, 
        MIN(corte_mes) AS min_corte_mes, MAX(corte_mes) AS max_corte_mes, COUNT(*) AS conteo 
    FROM bd_numerador_rachas
    GROUP BY identificacion, ranking_rachas, saldo
)
```

### 10. Crear la tabla bd_orden_rachas con el orden de rachas

```sql
bd_orden_rachas AS (
    SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY identificacion ORDER BY conteo DESC, max_corte_mes DESC) AS orden_racha 
    FROM bd_conteo_rachas  
)
```

### 11. Seleccionar la racha más larga por identificacion

```sql
SELECT identificacion, conteo AS racha, min_corte_mes AS fecha_inicio, max_corte_mes AS fecha_fin, saldo AS nivel 
FROM bd_orden_rachas
WHERE orden_racha = 1
```
