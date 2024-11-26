WITH aggregated_sales AS (
    SELECT 
        product_id,
        SUM(revenue) AS total_revenue,
        SUM(quantity) AS total_quantity
    FROM 
        {{ ref("stg_raw__sales") }}
    GROUP BY 
        product_id)

with source as (
        select 
        sales.product_id, 
        sales.revenue, 
        sales.quantity,
        product.*
        from {{ref("stg_raw__sales")}} as sales
        join {{ref("stg_raw__product")}} as product
        using (product_id)
    ) 
select 
    product_id, 
    total_revenue, 
    total_quantity, 
    purchase_price,
    quantity*purchase_price AS purchase_cost,
    revenue-purchase_cost AS margin,
from source