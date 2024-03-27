WITH customers AS (

  SELECT * 
  
  FROM {{ ref('customers')}}

),

orders AS (

  SELECT * 
  
  FROM {{ ref('orders')}}

),

by_customer_id AS (

  SELECT 
    orders.order_id AS order_id,
    customers.first_name AS first_name,
    customers.last_name AS last_name,
    orders.amount AS amount,
    customers.customer_id AS customer_id
  
  FROM orders
  INNER JOIN customers
     ON orders.customer_id = customers.customer_id

),

Reformat_1 AS (

  SELECT 
    customer_id AS customer_id,
    CONCAT(first_name, ' ', last_name) AS full_name,
    order_id AS order_id,
    amount AS amount
  
  FROM by_customer_id AS in0

),

order_count_by_customer AS (

  SELECT 
    COUNT(order_id) AS order_count,
    SUM(amount) AS sales_total
  
  FROM Reformat_1 AS in0
  
  GROUP BY 
    customer_id, full_name

),

order_count_by_customer_by_sales_total AS (

  SELECT * 
  
  FROM order_count_by_customer AS in0
  
  ORDER BY sales_total DESC NULLS LAST

)

SELECT *

FROM order_count_by_customer_by_sales_total
