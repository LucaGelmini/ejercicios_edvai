-- Ejercicio 1
SELECT DISTINCT category_name 
FROM categories c 

-- Ejercicio 2
SELECT DISTINCT region 
FROM customers c

-- Ejercicio 3
SELECT DISTINCT  contact_title 
FROM customers c 

-- Ejercicio 4
SELECT *
FROM customers c
ORDER BY country 

-- Ejercicio 5
SELECT  *
FROM orders o 
ORDER BY employee_id , order_date

-- Ejercicio 6
INSERT INTO customers (customer_id, company_name, contact_name,contact_title,address,city,region,postal_code,country,phone,fax)
VALUES ('NUEVO', '-', '-','-','-','-','-','-','-','-','-')

-- Ejercicio 7
INSERT INTO region (region_id,region_description)
VALUES (5,'LATAM');

-- Ejercicio 8
SELECT *
FROM customers c
WHERE region ISNULL 

-- Ejercicio 9
SELECT product_name , COALESCE(unit_price,10)  
FROM products p 

-- Ejericio 10
SELECT c.company_name, c.contact_name, order_date 
FROM orders o
INNER JOIN customers c 
ON c.customer_id = o.customer_id;

-- Ejercicio 11
SELECT od.order_id, p.product_name, od.discount 
FROM order_details od 
LEFT JOIN products p
ON od.product_id = p.product_id 

-- Ejercicio 12
SELECT c.customer_id ,c.company_name ,o.order_id ,o.order_date 
FROM  customers c 
LEFT JOIN orders o 
ON o.customer_id = c.customer_id 

-- Ejercicio 13
/*
 * Obtener el identificador del empleados, apellido, identificador de territorio y descripci�n
del territorio de todos los empleados y aquellos que hagan match en territorios:
 */
SELECT e.employee_id ,e.last_name, et.territory_id ,t.territory_description 
FROM employees e 
LEFT JOIN employee_territories et 
ON et.employee_id = e.employee_id
LEFT JOIN territories t 
ON t.territory_id = et.territory_id 

/*
 * 14. Obtener el identificador de la orden y el nombre de la empresa de todos las �rdenes y
 * aquellos clientes que hagan match:
*/
SELECT o.order_id , c.company_name 
FROM orders o 
LEFT JOIN customers c 
ON c.customer_id = o.customer_id 

/*
 * 15. Obtener el identificador de la orden, y el nombre de la compa��a de todas las �rdenes y
 * aquellos clientes que hagan match:
 */
SELECT o.order_id ,c.company_name  
FROM orders o 
RIGHT JOIN customers c 
ON c.customer_id = o.customer_id 


/*
 * 16. Obtener el nombre de la compa��a, y la fecha de la orden de todas las �rdenes y
 * aquellos transportistas que hagan match. Solamente para aquellas ordenes del a�o
 * 1996:
*/
SELECT c.company_name, o.order_date 
FROM orders o 
LEFT JOIN customers c 
ON c.customer_id = o.customer_id 
WHERE o.order_date BETWEEN '01-01-1996' AND '12-31-1996'

/* 17. Obtener nombre y apellido del empleados y el identificador de territorio, de todos los
empleados y aquellos que hagan match o no de employee_territories:
*/
SELECT e.first_name ,e.last_name, et.territory_id 
FROM employees e 
FULL JOIN employee_territories et 
ON et.employee_id = e.employee_id 

/* 18. Obtener el identificador de la orden, precio unitario, cantidad y total de todas las
 * �rdenes y aquellas �rdenes detalles que hagan match o no
 */
SELECT o.order_id, od.unit_price , od.quantity, od.unit_price * od.quantity AS total
FROM orders o
FULL JOIN order_details od 
ON o.order_id = od.order_id 

/* 19. Obtener la lista de todos los nombres de los clientes y los nombres de los proveedores:*/
SELECT c.company_name  AS nombre
FROM customers c
UNION
SELECT s.company_name  FROM suppliers s  

/* 20. Obtener la lista de los nombres de todos los empleados y los nombres de los gerentes
 * de departamento:
 */
SELECT e.first_name AS nombre, e2.first_name AS supervisor
FROM employees e
LEFT JOIN employees e2 
ON e2.employee_id = e .reports_to

-- 21. Obtener los productos del stock que han sido vendidos:
SELECT p.product_name 
FROM products p
LEFT JOIN (
	SELECT DISTINCT od.product_id 
	FROM order_details od 
) AS vendidos
ON p.product_id = vendidos.product_id 

-- 22. Obtener los clientes que han realizado un pedido con destino a Argentina:
SELECT DISTINCT  c.company_name
FROM orders o
LEFT JOIN customers c
ON c.customer_id = o.customer_id
WHERE o.ship_country ='Argentina'

-- 23. Obtener el nombre de los productos que nunca han sido pedidos por clientes de Francia:


SELECT DISTINCT p.product_name
FROM orders o
LEFT JOIN order_details od 
ON od.order_id = o.order_id
LEFT JOIN products p 
ON p.product_id = od.product_id 
WHERE o.customer_id  NOT IN (
	SELECT DISTINCT  customer_id 
	FROM customers c 
	WHERE country = 'France')
	
SELECT DISTINCT p.product_name
FROM orders o
LEFT JOIN order_details od 
ON od.order_id = o.order_id
LEFT JOIN products p 
ON p.product_id = od.product_id
LEFT JOIN customers c ON c.customer_id  =o.customer_id 
WHERE c.country != 'France'

-- de las dos maneras que lo hice me da igual pero es distinto de la imagen de ejemplo

-- 24. Obtener la cantidad de productos vendidos por identificador de orden:
SELECT o.order_id, sum(od.quantity)
FROM orders o
LEFT JOIN order_details od 
ON od.order_id = o.order_id 
GROUP BY o.order_id 

-- 25. Obtener el promedio de productos en stock por producto
-- no entiendo la consigna, la tabla de productos no repite elementos e informa del stock. Entonces, �promedio de que?
SELECT product_name , avg(units_in_stock) promedio_stock_por_producto
FROM products p 
GROUP BY product_id 

SELECT  avg(units_in_stock) AS promedio_de_stock
FROM products p 


-- 26. Cantidad de productos en stock por producto, donde haya m�s de 100 productos en stock
-- no entiendo que es lo que quiere decir con productos en stock por producto. Yo interpreto que es la columna de stock en la tabla producto, pero no
SELECT product_name , avg(units_in_stock) promedio_stock_por_producto
FROM products p 
GROUP BY product_id
HAVING units_in_stock > 100

/*
27. Obtener el promedio de frecuencia de pedidos por cada compa��a y solo mostrar
aquellas con un promedio de frecuencia de pedidos superior a 10:
-- la consigna no es clara y voy a suponer que se refiere a frecuencia por productos, podria ser frecuencia mensual
*/
SELECT f.customer_id, avg(f.products_by_customer)
FROM (SELECT DISTINCT  o.customer_id, od.product_id ,count(od.product_id) OVER (PARTITION BY o.customer_id, od.product_id) AS products_by_customer
FROM orders o
LEFT JOIN order_details od ON od.order_id = o.order_id) f
GROUP BY f.customer_id
HAVING  avg(f.products_by_customer)>2


-- Si hablamos de frecuencia mensual
SELECT f.customer_id, avg(f.products_by_customer)
FROM (
	SELECT DISTINCT o.customer_id,
	EXTRACT (MONTH FROM o.order_date) AS month,
	count( EXTRACT (MONTH FROM o.order_date)) OVER (PARTITION BY o.customer_id,  EXTRACT (MONTH FROM o.order_date)) AS products_by_customer
	FROM orders o
) f
GROUP BY f.customer_id
HAVING  avg(f.products_by_customer)>2

-- anual
SELECT f.customer_id, avg(f.products_by_customer)
FROM (
	SELECT DISTINCT o.customer_id,
	EXTRACT (YEAR FROM o.order_date) AS month,
	count( EXTRACT (year FROM o.order_date)) OVER (PARTITION BY o.customer_id,  EXTRACT (YEAR FROM o.order_date)) AS products_by_customer
	FROM orders o
) f
GROUP BY f.customer_id
HAVING  avg(f.products_by_customer)>10

/* 28. Obtener el nombre del producto y su categor�a, pero muestre "Discontinued" en lugar
del nombre de la categor�a si el producto ha sido descontinuado*/
SELECT p.product_name,
CASE 
	WHEN p.discontinued = 0
	THEN c.category_name
	ELSE 'Discontinued'
	END AS product_category
FROM products p
LEFT JOIN categories c ON c.category_id = p.category_id 

/* 29. Obtener el nombre del empleado y su t�tulo, pero muestre "Gerente de Ventas" en lugar
del t�tulo si el empleado es un gerente de ventas (Sales Manager):*/
SELECT 
	first_name, 
	last_name,
	CASE 
		WHEN title = 'Sales Manager' THEN 'Gerente de Ventas'
		ELSE title
	END job_title
FROM employees

