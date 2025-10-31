CREATE DATABASE `taobao`;

SHOW DATABASES;
USE `taobao`;

SHOW TABLES;
SELECT * FROM userbehavior;
DESC userbehavior;

ALTER TABLE  userbehavior
MODIFY COLUMN dates DATE;

ALTER TABLE  userbehavior
MODIFY COLUMN times DATETIME;



SELECT user_id,product_id,times,dates
FROM userbehavior
GROUP BY user_id,product_id,times,dates
HAVING COUNT(*)>1
LIMIT 10;



create table df_pv_uv 
(
  dates date,
  PV int,
  UV int,
  PVUV decimal(10,2)
);
drop table df_pv_uv;

INSERT into df_pv_uv 
select 
dates,
count(if(action_type='pv',1,null)) as PV,
count(distinct user_id) as UV, 
round(count(if(action_type='pv',1,null))/count(distinct user_id),2) as 'PVUV'
from userbehavior
group by dates;

select * from df_pv_uv;

CREATE TABLE df_retention_1 (dates CHAR(10), retention_1 FLOAT);
INSERT INTO df_retention_1 SELECT
  ub1.dates,
  count(ub2.user_id) / count(ub1.user_id) AS retention_1
FROM
  (SELECT DISTINCT user_id, dates FROM userbehavior) AS ub1
  LEFT JOIN (SELECT DISTINCT user_id, dates FROM userbehavior) AS ub2 
  ON ub1.user_id = ub2.user_id
  AND ub2.dates = DATE_ADD(ub1.dates, INTERVAL 1 DAY)
GROUP BY
  ub1.dates;
  
SELECT * FROM df_retention_1 ;


create table df_retention_3
(
  dates char(10),
  retention_3 FLOAT
  );
  
INSERT into df_retention_3
select ub1.dates,count(ub2.user_id)/count(ub1.user_id) as retention_3
FROM
  (SELECT DISTINCT user_id, dates FROM userbehavior) AS ub1
left join 
(select DISTINCT user_id,dates 
from userbehavior) as ub2 
on ub1.user_id = ub2.user_id 
and ub2.dates = DATE_ADD(ub1.dates,INTERVAL 3 day)
group by ub1.dates;

SELECT * FROM df_retention_3;

CREATE TABLE df_timeseries (
  dates VARCHAR (10),
  hours INT,
  PV INT,
  CHRT INT,
  FAV INT,
  BUY INT 
);
INSERT INTO df_timeseries SELECT
  dates,
  hours,
  count(IF(action_type = 'pv', 1, NULL)) AS PV,
  count(IF(action_type = 'cart', 1, NULL)) AS CART,
  count(IF(action_type = 'fav', 1, NULL)) AS FAV,
  count(IF(action_type = 'buy', 1, NULL)) AS BUY
FROM
  userbehavior
GROUP BY
  dates,
  hours
ORDER BY
  dates ASC,
  hours ASC;
  
select * from df_timeseries;
DESC df_timeseries;

CREATE TABLE path AS 
WITH ubt AS (
  SELECT
    user_id,
    category_id,
    COUNT(IF(action_type = 'pv', 1, NULL)) AS PV,
    COUNT(IF(action_type = 'fav', 1, NULL)) AS FAV,
    COUNT(IF(action_type = 'cart', 1, NULL)) AS CART,
    COUNT(IF(action_type = 'buy', 1, NULL)) AS BUY
  FROM
   userbehavior
  GROUP BY
    user_id,
    category_id
),
ifubt AS (
  SELECT
    user_id,
    category_id,
    IF(PV > 0, 1, 0) AS ifpv,
    IF(FAV > 0, 1, 0) AS iffav,
    IF(CART > 0, 1, 0) AS ifcart,
    IF(BUY > 0, 1, 0) AS ifbuy
  FROM
    ubt
  GROUP BY
    user_id,
    category_id
),
user_path AS (
SELECT user_id, category_id, CONCAT(ifpv, iffav, ifcart, ifbuy) AS path 
FROM ifubt) 

SELECT
  user_id,
  category_id,
  path,
  CASE
    WHEN path = 1101 THEN
      'PV-FAV-/-BUY'
    WHEN path = 1011 THEN
      'PV-/-CART-BUY'
    WHEN path = 1111 THEN
      'PV-FAV-CART-BUY'
    WHEN path = 1001 THEN
      'PV-/-/-BUY'
    WHEN path = 1010 THEN
      'PV-/-CART-/'
    WHEN path = 1100 THEN
      'PV-FAV-/-/'
    WHEN path = 1110 THEN
      'PV-FAV-BUY-/'
    ELSE
      'PV-/-/-/'
  END AS buy_path
FROM
  user_path
WHERE
  path REGEXP '^1'; -- Only choose when PV==1
  
SELECT * FROM path;

create table funnel as 
SELECT 
  dates,
  COUNT(DISTINCT CASE WHEN action_type = 'pv' THEN user_id END) AS pv_num,
  COUNT(DISTINCT CASE WHEN action_type = 'cart' THEN user_id END) + 
  COUNT(DISTINCT CASE WHEN action_type = 'fav' THEN user_id END) AS cart_fav_num,
  COUNT(DISTINCT CASE WHEN action_type = 'buy' THEN user_id END) AS buy_num
FROM  userbehavior
GROUP BY dates;

select * from funnel;

CREATE TABLE df_rfc AS
WITH 
r AS (
    SELECT 
        user_id,
        MAX(dates) AS recency  
    FROM  userbehavior
    WHERE action_type = 'buy'
    GROUP BY user_id
),
f AS (
    SELECT 
        user_id,
        COUNT(*) AS frequency  
    FROM  userbehavior
    WHERE action_type = 'buy'
    GROUP BY user_id
),
c AS (
    SELECT 
        user_id,
        COUNT(*) AS cart_fav_count  
    FROM  userbehavior
    WHERE action_type IN ('cart', 'fav')  -- count cart&Fav behavior
    GROUP BY user_id
),
rfc_base AS (
    SELECT 
        r.user_id,
        r.recency,
        f.frequency,
        c.cart_fav_count
    FROM r
    LEFT JOIN f ON r.user_id = f.user_id
    LEFT JOIN c ON r.user_id = c.user_id
),
rfc_scores AS (
    SELECT 
        user_id,
        recency,
        CASE 
            WHEN recency = '2017-12-03' THEN 100
            WHEN recency IN ('2017-12-02', '2017-12-01') THEN 80
            WHEN recency IN ('2017-11-30', '2017-11-29') THEN 60
            WHEN recency IN ('2017-11-28', '2017-11-27') THEN 40
            ELSE 20 
        END AS r_score,
        frequency,
        CASE 
            WHEN frequency > 15 THEN 100
            WHEN frequency BETWEEN 12 AND 14 THEN 90
            WHEN frequency BETWEEN 9 AND 11 THEN 70
            WHEN frequency BETWEEN 6 AND 8 THEN 50 
            WHEN frequency BETWEEN 3 AND 5 THEN 30
            ELSE 10 
        END AS f_score,
        cart_fav_count,
        CASE 
            WHEN cart_fav_count > 20 THEN 100
            WHEN cart_fav_count BETWEEN 16 AND 20 THEN 85
            WHEN cart_fav_count BETWEEN 11 AND 15 THEN 70
            WHEN cart_fav_count BETWEEN 6 AND 10 THEN 55 
            WHEN cart_fav_count BETWEEN 1 AND 5 THEN 40
            ELSE 20 
        END AS c_score
    FROM rfc_base
)

SELECT 
    t1.user_id,
    recency,
    r_score,
    avg_r,
    frequency,
    f_score,
    avg_f,
    cart_fav_count,
    c_score,
    avg_c,
    CASE    
        WHEN (f_score >= avg_f AND r_score >= avg_r AND c_score >= avg_c) THEN 'Premium Users'    
        WHEN (f_score >= avg_f AND r_score >= avg_r AND c_score < avg_c) THEN 'Potential Users'    
        WHEN (f_score >= avg_f AND r_score < avg_r AND c_score >= avg_c) THEN 'Active Users'    
        WHEN (f_score >= avg_f AND r_score < avg_r AND c_score < avg_c) THEN 'Retained Users'    
        WHEN (f_score < avg_f AND r_score >= avg_r AND c_score >= avg_c) THEN 'Growing Users'    
        WHEN (f_score < avg_f AND r_score >= avg_r AND c_score < avg_c) THEN 'New Users'    
        WHEN (f_score < avg_f AND r_score < avg_r AND c_score >= avg_c) THEN 'Interested Users'    
        ELSE 'Churn-risk Users'    
    END AS user_class     
FROM rfc_scores AS t1
LEFT JOIN 
(
    SELECT    
        user_id,    
        AVG(r_score) OVER() AS avg_r,    
        AVG(f_score) OVER() AS avg_f,
        AVG(c_score) OVER() AS avg_c    
    FROM    
        rfc_scores
) AS t2 ON t1.user_id = t2.user_id;

select * from df_rfc;

create table df_rfc_count as 
select user_class,count(*) as user_class_num
from df_rfc 
group by user_class;

select * from df_rfc_count;


create table product_buy_hot as 
select product_id,
count(if(action_type = 'buy',1,null)) as product_buy
from userbehavior
group by product_id 
order by product_buy  desc 
limit 1000;

select * from product_buy_hot;

create table category_buy_hot as 
select category_id,
count(if(action_type = 'buy',1,null)) as category_buy
from userbehavior
group by category_id 
order by category_buy  desc 
limit 100;

select * from category_buy_hot;

CREATE TABLE category_pv_buy_time AS
WITH 
-- filter the pair user_category with bahavior 'Buy'
bought_categories AS (
    SELECT DISTINCT 
        user_id, 
        category_id
    FROM userbehavior
    WHERE action_type = 'buy'
),


-- calculate the first PV time of each use_category pair
first_pv AS (
    SELECT 
        t.user_id,
        t.category_id,
        MIN(t.times) AS first_pv_time
    FROM userbehavior t
    JOIN bought_categories bc     
        ON t.user_id = bc.user_id AND t.category_id = bc.category_id
    WHERE 
        t.action_type = 'pv'
    GROUP BY 
        t.user_id, 
        t.category_id
),

-- calculate the first BUY time of each use_category pair
first_buy AS (
    SELECT 
        t.user_id,
        t.category_id,
        MIN(t.times) AS first_buy_time
    FROM userbehavior t
    JOIN bought_categories bc 
        ON t.user_id = bc.user_id AND t.category_id = bc.category_id
    WHERE 
        t.action_type = 'buy'
    GROUP BY 
        t.user_id, 
        t.category_id
),

-- calculate the conversion time of each use_category pair
user_category_conversion AS (
    SELECT 
        p.user_id,
        p.category_id,
        p.first_pv_time,
        b.first_buy_time,
        TIMESTAMPDIFF(SECOND, p.first_pv_time, b.first_buy_time) AS conversion_seconds
    FROM first_pv p
    INNER JOIN first_buy b  -- only keep 'Buy' record
        ON p.user_id = b.user_id 
        AND p.category_id = b.category_id
        AND p.first_pv_time < b.first_buy_time  -- make sure PV is before BUY
),


-- calculate average conversion of each category
category_avg_conversion AS (
    SELECT 
        category_id,
        AVG(conversion_seconds) AS avg_conversion_seconds,
        AVG(conversion_seconds) / 3600 AS avg_conversion_hours  -- 转换为小时
    FROM user_category_conversion
    GROUP BY category_id
)



SELECT 
    ucc.user_id,
    ucc.category_id,
    ucc.first_pv_time,
    ucc.first_buy_time,
    ucc.conversion_seconds / 3600 AS conversion_hours,  -- convert to hour
    cat.avg_conversion_hours,
    (ucc.conversion_seconds / 3600) - cat.avg_conversion_hours AS hours_deviation  -- 与品类平均值的偏差
FROM user_category_conversion ucc
JOIN category_avg_conversion cat 
    ON ucc.category_id = cat.category_id
WHERE 
    ucc.first_pv_time > '2017-11-25'
ORDER BY 
    ucc.category_id,
    ucc.user_id;
    
select * from category_pv_buy_time;

create table category_hours_flow
select category_id,hours,
    sum(if(action_type='pv',1,0)) as pv,
    sum(if(action_type='cart',1,0)) as cart,
    sum(if(action_type='fav',1,0)) as fav,
    sum(if(action_type='buy',1,0)) as buy
from  userbehavior 
group by category_id,hours
order by category_id,hours;

select * from category_hours_flow;

create table category_daily_flow
select category_id,dates,
sum(if(action_type='pv',1,0)) as pv,
sum(if(action_type='cart',1,0)) as cart,
sum(if(action_type='fav',1,0)) as fav,
sum(if(action_type='buy',1,0)) as buy
from userbehavior 
group by category_id,dates
order by category_id,dates;

select * from category_daily_flow;



create table category_feature
select category_id,
        count(if(action_type='pv',1,null)) as PV,
        count(if(action_type='fav',1,null)) as FAV, 
        count(if(action_type='cart',1,null)) as CART, 
        count(if(action_type='buy',1,null)) as BUY
from userbehavior 
group by category_id;

create table category_feature
select category_id,
        count(if(action_type='pv',1,null)) as PV,
        count(if(action_type='fav',1,null)) as FAV, 
        count(if(action_type='cart',1,null)) as CART, 
        count(if(action_type='buy',1,null)) as BUY
from userbehavior 
group by category_id


