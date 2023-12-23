-- DASHBOARD

-- Transactions
SELECT 
count(transaction_date)
FROM Transactions1998

-- Revenue
SELECT sum(quantity * product_retail_price) as total_revenue
FROM transactions1998 as trans98
LEFT JOIN products p 
ON trans98.product_id = p.product_id

-- Profit
SELECT sum(quantity*(product_retail_price - product_cost)) as total_profit
FROM transactions1998 as trans98
LEFT JOIN products p 
ON Trans98.product_id = p.product_id

-- return rate
SELECT FORMAT(
    (SELECT sum(quantity) FROM returns WHERE year(return_date)= 1998) *1.0
    / 
    (SELECT sum(quantity) FROM Transactions1998) 
    ,'p'
) as return_rate

-- revenue target
With cte as( 
    SELECT 
        (SELECT sum(quantity * product_retail_price) as total_revenue
            FROM transactions1998 as trans98
            LEFT JOIN products p 
            ON trans98.product_id = p.product_id
            WHERE month(transaction_date) = 11)* 1.05 as target
        ,(SELECT sum(quantity * product_retail_price) as total_revenue
            FROM transactions1998 as trans98
            LEFT JOIN products p 
            ON trans98.product_id = p.product_id
            WHERE month(transaction_date) = 12) as revenue
)
SELECT *
    , FORMAT((revenue-target)/target,'p') pct
FROM cte


-- revenue by month for line chart
SELECT month(transaction_date) as month 
    , SUM(quantity* product_retail_price) as revenue
FROM transactions1998 as trans98
LEFT JOIN products p 
ON trans98.product_id = p.product_id
GROUP BY month(transaction_date)
ORDER BY month

-- revenue by store type
SELECT store_type
    , SUM(quantity* product_retail_price) as revenue
FROM transactions1998 as trans98
LEFT JOIN stores s
ON trans98.store_id = s.store_id
LEFT JOIN products p 
ON trans98.product_id = p.product_id
GROUP BY store_type
ORDER BY revenue desc

-- total transaction of regions
SELECT sales_region
    , sales_district
    , count(transaction_date) total_transactions
FROM transactions1998 as trans98
LEFT JOIN stores s
ON trans98.store_id = s.store_id
LEFT JOIN regions r
ON s.region_id = r.region_id
GROUP BY sales_region, sales_district
ORDER BY sales_region, sales_district


-- top 30 product brands by profit margin (profit/revenue)
WITH cte AS(
    SELECT product_brand
        , count(transaction_date) as total_transactions
        , sum(trans98.quantity * product_retail_price) as total_revenue
        , sum(trans98.quantity*(product_retail_price - product_cost)) as total_profit
        , sum(trans98.quantity) as total_sold
        , sum(r.quantity) as total_returned
    FROM transactions1998 as trans98
    LEFT JOIN products p 
    ON trans98.product_id = p.product_id
    LEFT JOIN (SELECT * from Returns WHERE year(return_date) = 1998) as r
    ON p.product_id = r.product_id
    GROUP BY product_brand
)
SELECT product_brand
    , total_revenue
    , FORMAT(total_profit*1.0 / total_revenue,'p') as profit_margin
    , FORMAT(total_returned *1.0 / total_sold,'p') as return_rate
FROM cte
ORDER BY FORMAT(total_profit*1.0 / total_revenue,'p') desc
    
-- CUSTOMER DETAIL

-- Total customer
SELECT count(distinct customer_id) total_customers
FROM transactions1998

-- revenue per customer
SELECT sum(quantity * product_retail_price) / count(distinct customer_id) total_customers
FROM transactions1998 t
LEFT JOIN products p 
ON t.product_id = p.product_id

-- total customers by month of 1998
SELECT month(transaction_date) as month 
    , count(distinct customer_id) as total_customers
FROM transactions1998 
GROUP BY month(transaction_date)
ORDER BY month

-- customer retention
WITH fact_table AS(
    SELECT customer_id
        , MIN(month(transaction_date)) OVER (PARTITION BY customer_id) AS firstmonth
        , MONTH(transaction_date) - MIN(MONTH(transaction_date)) OVER (PARTITION BY customer_id) AS subsequent_month
    FROM Transactions1998
)
, fact2 AS(
    SELECT DISTINCT firstmonth
    , subsequent_month
    , COUNT( DISTINCT customer_id) AS retained_cus
    FROM fact_table
    GROUP BY firstmonth, subsequent_month
    -- ORDER BY firstmonth, subsequent_month
)
SELECT *
, FIRST_VALUE (retained_cus) OVER ( PARTITION BY firstmonth ORDER BY subsequent_month ASC ) AS original_customers
, FORMAT ( retained_cus * 1.0 /MAX (retained_cus) OVER ( PARTITION BY firstmonth ) , 'p') AS pct
INTO #retentioncus
FROM fact2
ORDER BY firstmonth, subsequent_month

SELECT firstmonth, original_customers
, "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"
FROM (
    SELECT firstmonth, subsequent_month, original_customers, pct -- STRING
    FROM #retentioncus
    ) AS fact2
PIVOT (
    MIN (pct)
    FOR subsequent_month IN ( "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11" )
    ) AS pivot_logic
ORDER BY firstmonth

-- customer segmentation
WITH rfm_table as(
    SELECT customer_id
    , DATEDIFF(day, MAX(transaction_date), '1998-12-31') AS recency
    , COUNT(transaction_date) AS frequency
    , CONVERT(int,(SUM(quantity * product_retail_price)))AS monetary
    FROM transactions1998 fact
    LEFT JOIN Products p 
    ON fact.product_id = fact.product_id
    GROUP BY customer_id
)
, table_rank AS (
SELECT *
, PERCENT_RANK() OVER ( ORDER BY recency ASC ) r_rank
, PERCENT_RANK() OVER ( ORDER BY frequency DESC ) f_rank
, PERCENT_RANK() OVER ( ORDER BY monetary DESC ) m_rank
FROM rfm_table
)
, table_tier AS (
SELECT *
, CASE WHEN r_rank > 0.75 THEN 4
WHEN r_rank > 0.5 THEN 3
WHEN r_rank > 0.25 THEN 2
ELSE 1 END r_tier
, CASE WHEN f_rank > 0.75 THEN 4
WHEN f_rank > 0.5 THEN 3
WHEN f_rank > 0.25 THEN 2
ELSE 1 END f_tier
, CASE WHEN m_rank > 0.75 THEN 4
WHEN m_rank > 0.5 THEN 3
WHEN m_rank > 0.25 THEN 2
ELSE 1 END m_tier
FROM table_rank
)
, table_score AS (
SELECT *
, CONCAT (r_tier, f_tier, m_tier) AS rfm_score
FROM table_tier
)
, table_segment AS (
SELECT *
, CASE
WHEN rfm_score = 111 THEN 'Best Customers' -- KH tốt nhất
WHEN rfm_score LIKE '[3-4][3-4][1-4]' THEN 'Lost Bad Customer' -- KH rời bỏ mà còn siêu tệ (F <= ??? )
WHEN rfm_score LIKE '[3-4]2[1-4]' THEN 'Lost Customers' -- KH cũng rời bỏ nhưng có valued (F = ... )
WHEN rfm_score LIKE '21[1-4]' THEN 'Almost Lost' -- sắp lost những KH này
WHEN rfm_score LIKE '11[2-4]' THEN 'Loyal Customers'
WHEN rfm_score LIKE '[1-2][1-3]1' THEN 'Big Spenders' -- chi nhiều tiền
WHEN rfm_score LIKE '[1-2]4[1-4]' THEN 'New Customers' -- KH mới nên là giao dịch ít
WHEN rfm_score LIKE '[3-4]1[1-4]' THEN 'Hibernating' -- ngủ đông (trc đó từng rất là tốt )
WHEN rfm_score LIKE '[1-2][2-3][2-4]' THEN 'Potential Loyalists' -- có tiềm năng
ELSE 'unknown'
END segment_label
FROM table_score
)
SELECT segment_label
, COUNT(customer_id) number_cus
FROM table_segment
GROUP BY segment_label
ORDER BY number_cus DESC
-- SELECT ts.customer_id
--     , CONCAT(first_name, ' ', last_name) as Customer
--     , recency
--     , frequency
--     , monetary
-- FROM table_segment ts 
-- LEFT JOIN Customers c 
-- ON c.customer_id = ts.customer_id
-- WHERE segment_label = 'Best Customers'


-- PRODUCT

-- Detail product of product brands
with cte as(
    SELECT t.product_id, product_brand, product_name, total_returned
        , count(t.transaction_date) as transactions
        , sum((product_retail_price - product_cost) * t.quantity) as profit 
        , sum(quantity) as total_sold 
    FROM transactions1998 t 
    JOIN  Products p
    ON p.product_id = t.product_id
    JOIN 
    (SELECT product_id, sum(quantity) as total_returned
        FROM returns 
        WHERE year(return_date)= 1998
        GROUP BY product_id) as rt 
    ON rt.product_id = t.product_id
    GROUP BY t.product_id, product_brand, product_name, total_returned
)
SELECT product_brand, product_name,transactions, profit
    , FORMAT(total_returned *1.0/ total_sold, 'p') as return_rate
FROM cte
