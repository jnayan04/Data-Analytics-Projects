CREATE DATABASE Pizzahut;

-- for larger data sets,it becomes difficult to import the whole data set,thus in such cases we need to create tables by ourselves
USE Pizzahut;

-- creating orders table
create table orders(
order_id int not null,
order_date date not null,
order_time time not null,
primary key(order_id)
);
-- creating order_details table
create table order_details(
order_details_id int not null,
order_id int not null,
pizza_id text not null,
quantity int not null,
primary key(order_details_id)
);

-- >> Q1.Retrieve the total number of orders placed.

select count(order_id)  as total_orders from orders;

-- >> Q2.Calculate the total revenue generated from pizza sales

SELECT 
    ROUND(SUM(p.price * od.quantity), 2) AS total_sales
FROM
    pizzas p
        INNER JOIN
    order_details od ON p.pizza_id = od.pizza_id;

-- >> Q3.Identify the highest-priced pizza.

SELECT 
    pt.name AS Pizza, p.price AS Price
FROM
    pizza_types pt
        INNER JOIN
    pizzas p ON pt.pizza_type_id = p.pizza_type_id
ORDER BY price DESC
LIMIT 1;

 -- Q4.Identify the most common pizza size ordered.
 
SELECT 
    p.size, COUNT(od.order_details_id) AS cnt
FROM
    pizzas p
        INNER JOIN
    order_details od ON p.pizza_id = od.pizza_id
GROUP BY size
ORDER BY cnt DESC
LIMIT 1;

 -- Q5.List the top 5 most ordered pizza types along with their quantities.

SELECT 
    pt.name AS Pizza_type, SUM(od.quantity) AS qty
FROM
    pizza_types pt
        INNER JOIN
    pizzas p ON pt.pizza_type_id = p.pizza_type_id
        INNER JOIN
    order_details od ON od.pizza_id = p.pizza_id
GROUP BY Pizza_type
ORDER BY qty DESC
LIMIT 5;

-- >>Q6.Join the necessary tables to find the total quantity of each pizza category ordered.

SELECT 
    pt.category, SUM(od.quantity) AS total_cat_cnt
FROM
    pizza_types pt
        INNER JOIN
    pizzas p ON pt.pizza_type_id = p.pizza_type_id
        INNER JOIN
    order_details od ON od.pizza_id = p.pizza_id
GROUP BY pt.category
ORDER BY total_cat_cnt DESC;

-- >>Q7.Determine the distribution of orders by hour of the day.

SELECT 
    HOUR(order_time) AS hrs, COUNT(order_id) AS cnt
FROM
    orders
GROUP BY hrs
ORDER BY cnt DESC;

-- >>Q8.Join relevant tables to find the category-wise distribution of pizzas.

SELECT 
    category, COUNT(name) as cnt
FROM
    pizza_types
GROUP BY category
ORDER BY cnt DESC;

-- >>Q9.Group the orders by date and calculate the average number of pizzas ordered per day. 

SELECT 
    ROUND(AVG(qty), 0) AS avg_orders
FROM
    (SELECT 
        o.order_date, SUM(quantity) AS qty
    FROM
        orders o
    INNER JOIN order_details od ON o.order_id = od.order_id
    GROUP BY order_date) a;

-- >>Q10.Determine the top 3 most ordered pizza types based on revenue.

SELECT 
    pt.name AS Pizza, SUM(od.quantity * p.price) AS revenue
FROM
    pizza_types pt
        INNER JOIN
    pizzas p ON pt.pizza_type_id = p.pizza_type_id
        INNER JOIN
    order_details od ON od.pizza_id = p.pizza_id
GROUP BY pt.name
ORDER BY revenue DESC
LIMIT 3;
    
    
   -- >>Q11.Calculate the percentage contribution of each pizza type to total revenue.

SELECT 
    pt.category AS Pizza,(SUM(od.quantity * p.price) /(SELECT 
    ROUND(SUM(p.price * od.quantity), 2) AS total_sales
FROM
    pizzas p
        INNER JOIN
    order_details od ON p.pizza_id = od.pizza_id))*100 as revenue
FROM
    pizza_types pt
        INNER JOIN
    pizzas p ON pt.pizza_type_id = p.pizza_type_id
        INNER JOIN
    order_details od ON od.pizza_id = p.pizza_id
GROUP BY pt.category
ORDER BY revenue DESC;

 -- >>Q12.Analyze the cumulative revenue generated over time.

select order_date,sum(revenue) over(order by order_date) as cum_revenue from(
 select o.order_date,sum(p.price*od.quantity) as revenue
 from order_details od
 inner join pizzas p on od.pizza_id=p.pizza_id
 inner join orders o on o.order_id=od.order_id
 group by  o.order_date)a;
 
  -- >>Q13.Determine the top 3 most ordered pizza types based on revenue for each pizza category.
  
  with cte as(
  select *
  ,rank() over(partition by category order by revenue desc) as rnk from(
  SELECT 
   pt.category,pt.name,sum(p.price*od.quantity) as revenue
FROM
    pizza_types pt
        INNER JOIN
    pizzas p ON pt.pizza_type_id = p.pizza_type_id
        INNER JOIN
    order_details od ON od.pizza_id = p.pizza_id
    group by pt.category,pt.name)a
     )
  
select * from cte  where rnk<=3;


  
  
  
  