-- customer brand relation based on amount of spending and repeat buys
select customer_id,
customer_name,
email_address,
brand,
max(order_date) as last_purchase_date,
count(*) as purchase_count,
max(sale_price) as max_amount_spent from denormalized_product_customer_order
where
order_date > dateadd(month, -24, current_date)
--and customer_name ilike 'Daniel Huber'
group by
brand, customer_id, customer_name, email_address
order by purchase_count desc;
--------------------------------
--repeat buyers
select customer_name, count(*)
from denormalized_product_customer_order
group by customer_name
having count(*) > 1;
--------------------------------
select brand, count(*), sum(sale_price) as total_sales
from denormalized_product_customer_order
where order_date > dateadd(month, -24, current_date)
group by brand
order by total_sales;
----------------------------------