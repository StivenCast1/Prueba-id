Describe work_sas.stiv_pruebaid_clientes;
Describe work_sas.stiv_pruebaid_transacciones;
Describe work_sas.stiv_pruebaid_categorias_consumo;


/*exploracion*/   
select identificacion, count (distinct nombre) as cantidad
from work_sas.stiv_pruebaid_clientes
group by 1; /*Encontramos identificaciones con diferentes nombres. Revisaremos si son diferes tipo_documento*/
with revision_unicos as(
	select identificacion, tipo_documento, count (distinct nombre) as cantidad
	from work_sas.stiv_pruebaid_clientes
	group by 1,2
)
select max(cantidad) as cantidad
from revision_unicos; /*Se valida que no hay duplicidad en identificacion por tipo de documento*/
select distinct tipo_documento 
from work_sas.stiv_pruebaid_clientes;
select distinct clasificacion 
from work_sas.stiv_pruebaid_clientes;
select distinct tipo_tarjeta 
from work_sas.stiv_pruebaid_clientes;
select max(fecha_apertura_tarjeta) as maxima_fecha, min(fecha_apertura_tarjeta) as minima_fecha 
from work_sas.stiv_pruebaid_clientes;
select distinct estado_tarjeta 
from work_sas.stiv_pruebaid_clientes;

/*correcciones*/
select distinct lower(tipo_documento) as tipo_documento 
from work_sas.stiv_pruebaid_clientes; /*CORRECCION*/
select 
	case 
		when lower(estado_tarjeta) = 'açtiva' then 'Activa'
		when lower(estado_tarjeta) = 'inativa' then 'Inactiva'
		else estado_tarjeta
	end as estado_tarjeta
from	work_sas.stiv_pruebaid_clientes; /*CORRECCION*/

/*revision de nulos*/
select 
	count (*) as total, 
	count(nombre) as nombre, 
	count(identificacion) as tarjetas, 
	count(tipo_documento) as tipo_documento,
	count(clasificacion) as clasificacion,
	count(tipo_tarjeta) as tipo_tarjeta,
	count(fecha_apertura_tarjeta) as fecha_apertura_tarjeta, 
	count(estado_tarjeta) as estado_tarjeta 
from work_sas.stiv_pruebaid_clientes


/* tabla de transacciones*/
select  max(fecha_transaccion) as maxima_fecha_transaccion, min(fecha_transaccion) as minima_fecha_transaccion 
from work_sas.stiv_pruebaid_transacciones;
/*Encontramos una fecha en 0*/
Select distinct estado as estados 
from work_sas.stiv_pruebaid_transacciones;
select max(valor_compra) as maximo_valor, min(valor_compra) as minimo_valor 
from work_sas.stiv_pruebaid_transacciones;
select count(id_transaccion) as transacciones, count(distinct id_transaccion) as diferentes_transacciones
from work_sas.stiv_pruebaid_transacciones

/*revision de nulos*/
Select 
	count (*) as total,
	count(identificacion) as identificacion,
	count(id_transaccion) as id_transaccion, 
	count(fecha_transaccion) as fecha_transaccion,
	count(codigo_categoria) as codigo_categoria,
	count(estado) as estado,
	count(valor_compra) as valor_compra
from work_sas.stiv_pruebaid_transacciones;

/*revision categorias_consumo*/
select distinct (nombre_categoria) as nombre_categoria 
from work_sas.stiv_pruebaid_categorias_consumo;
select distinct (ciudad) as ciudad 
from work_sas.stiv_pruebaid_categorias_consumo;
select distinct (departamento) as departamento 
from work_sas.stiv_pruebaid_categorias_consumo;
with bd_categorias_rev as(
	select codigo_categoria, count(distinct nombre_categoria) as cantidad_codigos
	from  work_sas.stiv_pruebaid_categorias_consumo
	group by 1 
)
select max(cantidad_codigos) as cantidad
from bd_categorias_rev


/*revision de nulos*/
Select
count(*) as total,
count(codigo_categoria) as codigo_categoria,
count(nombre_categoria) as nombre,
count(ciudad) as ciudad,
count(departamento) as departamento
from work_sas.stiv_pruebaid_categorias_consumo;

/*SOLUCION PUNTO 1*/

With bd_clientes AS(
	select nombre, identificacion, lower(tipo_documento) as tipo_documento
	from work_sas.stiv_pruebaid_clientes
),
bd_transacciones as(
	select identificacion, fecha_transaccion,id_transaccion, codigo_categoria, valor_compra
	from	work_sas.stiv_pruebaid_transacciones
	/*where fecha_transaccion >= '5/12/2022' and fecha_transaccion<='25/12/2024' DEFINIR FECHAS DE INTERES*/
),
bd_categorias_consumo as(
	select codigo_categoria, nombre_categoria,ciudad
	from  work_sas.stiv_pruebaid_categorias_consumo
),
cruce as(
	Select bd_clientes.*, fecha_transaccion,id_transaccion, valor_compra, nombre_categoria, ciudad
	from bd_clientes
	left join
		bd_transacciones
	on bd_clientes.identificacion = bd_transacciones.identificacion
	left join
		bd_categorias_consumo
	on bd_transacciones.codigo_categoria =  bd_categorias_consumo.codigo_categoria
),
conteo as(
	select nombre, identificacion, tipo_documento,nombre_categoria,count(id_transaccion) as cantidad_compras, max(fecha_transaccion) as maxima_fecha_transaccion
	from cruce
	group by 1,2,3,4
),
rankeo as(
	select *, row_number () over (partition by identificacion,tipo_documento order by cantidad_compras desc, maxima_fecha_transaccion desc) as ranking
	from conteo
)
Select *
from rankeo
where ranking <= 2 /* CAMBIAR EL 2 POR LAS N CATEGORIAS QUE SE QUIEREN CONSULTAR CAMBIANDO EL IGUAL POR <= o >=*/
order by identificacion, ranking






