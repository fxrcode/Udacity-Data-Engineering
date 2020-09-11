/*****
 --- Subquery Mania
 ******/

-- Provide the name of the sales_rep in each region with the largest amount of total_amt_usd sales.
SELECT t3.rep_name,
    t3.region_name,
    t3.total_amt
FROM(
        SELECT region_name,
            MAX(total_amt) total_amt
        FROM(
                SELECT s.name rep_name,
                    r.name region_name,
                    SUM(o.total_amt_usd) total_amt
                FROM sales_reps s
                    JOIN accounts a ON a.sales_rep_id = s.id
                    JOIN orders o ON o.account_id = a.id
                    JOIN region r ON r.id = s.region_id
                GROUP BY 1,
                    2
            ) t1
        GROUP BY 1
    ) t2
    JOIN (
        SELECT s.name rep_name,
            r.name region_name,
            SUM(o.total_amt_usd) total_amt
        FROM sales_reps s
            JOIN accounts a ON a.sales_rep_id = s.id
            JOIN orders o ON o.account_id = a.id
            JOIN region r ON r.id = s.region_id
        GROUP BY 1,
            2
        ORDER BY 3 DESC
    ) t3 ON t3.region_name = t2.region_name
    AND t3.total_amt = t2.total_amt;

-- For the region with the largest (sum) of sales total_amt_usd, how many total (count) orders were placed?
select r.name,
    count(o.total) total_orders
from sales_reps sr
    join region r on r.id = sr.region_id
    join accounts a on sr.id = a.sales_rep_id
    join orders o on o.account_id = a.id
group by 1
having sum(o.total_amt_usd) = (
        select max(sub.total_amt)
        from (
                select r."name" region_name,
                    SUM(o.total_amt_usd) total_amt
                from sales_reps sr
                    join region r on r.id = sr.region_id
                    join accounts a on sr.id = a.sales_rep_id
                    join orders o on o.account_id = a.id
                group by 1
            ) sub
    );

-- How many accounts had more total purchases than the account name which has bought the most standard_qty paper throughout their lifetime as a customer?
SELECT COUNT(*)
FROM (
        SELECT a.name
        FROM orders o
            JOIN accounts a ON a.id = o.account_id
        GROUP BY 1
        HAVING SUM(o.total) > (
                SELECT total
                FROM (
                        SELECT a.name act_name,
                            SUM(o.standard_qty) tot_std,
                            SUM(o.total) total
                        FROM accounts a
                            JOIN orders o ON o.account_id = a.id
                        GROUP BY 1
                        ORDER BY 2 DESC
                        LIMIT 1
                    ) inner_tab
            )
    ) counter_tab;

-- For the customer that spent the most (in total over their lifetime as a customer) total_amt_usd, how many web_events did they have for each channel?
SELECT a.name,
    w.channel,
    COUNT(*)
FROM accounts a
    JOIN web_events w ON a.id = w.account_id
    AND a.id = (
        select t1.id
        from (
                select a.id,
                    a.name,
                    SUM(o.total_amt_usd) life_total
                from accounts a
                    join web_events we on a.id = we.account_id
                    join orders o on a.id = o.account_id
                group by 1,
                    2
                order by 3 desc
                limit 1
            ) t1
    )
group by 1,
    2
order by 3 desc;

-- What is the lifetime average amount spent in terms of total_amt_usd for the top 10 total spending accounts?
select avg(t1.life_total)
from (
        select a.id,
            a.name,
            SUM(o.total_amt_usd) life_total
        from accounts a --join web_events we
            --on a.id = we.account_id
            join orders o on a.id = o.account_id
        group by 1,
            2
        order by 3 desc
        limit 10
    ) t1;

-- What is the lifetime average amount spent in terms of total_amt_usd, including only the companies that spent more per order, on average, than the average of all orders.
select
	avg(avg_amt)
from
	(
	select
		o2.account_id , avg(o2.total_amt_usd) avg_amt
	from
		orders o2
	group by
		1
	having
		avg(o2.total_amt_usd) > (
		select
			AVG(o.total_amt_usd)
		from
			orders o)) tmp;

/*****
 --- Subquery Mania using WITH
 ******/

-- Provide the name of the sales_rep in each region with the largest amount of total_amt_usd sales.
WITH t1 as (
    select s.name as sr_name, r.name as r_name, SUM(total_amt_usd) as total_amt
    from sales_reps s
             join region r on s.region_id = r.id
             join accounts a on s.id = a.sales_rep_id
             join orders o on a.id = o.account_id
    group by 1, 2
    order by 3 desc),

     t2 as (
         select r_name, max(total_amt) as total_amt
         from t1
         group by 1
     )

select t1.sr_name, t1.r_name, t1.total_amt
from t2
         join t1
              on t1.total_amt = t2.total_amt and t1.r_name = t2.r_name;

-- For the region with the largest sales total_amt_usd, how many total orders were placed?
with t1 as (
    select r.name r_name, o.total_amt_usd, o.total
    from region r
             join sales_reps sr on r.id = sr.region_id
             join accounts a on sr.id = a.sales_rep_id
             join orders o on a.id = o.account_id
),
     t2 as (
         select r_name, sum(total_amt_usd)
         from t1
         group by 1
         order by 2 desc
         limit 1
     )

select t1.r_name, count(t1.total)
from t1
join t2 on t1.r_name = t2.r_name
group by 1;

-- How many accounts had more total purchases than the account name which has bought the most standard_qty paper throughout their lifetime as a customer?
with t1 as (
    select a.name, sum(o.standard_qty), sum(o.total) total
    from accounts a
             join orders o on a.id = o.account_id
    group by 1
    order by 2 desc
    limit 1
),
     t2 as (
         select a.name
         from accounts a
                  join orders o2 on a.id = o2.account_id
         group by 1
         having sum(o2.total) > (select total from t1)
     )

select *
from t2;

-- For the customer that spent the most (in total over their lifetime as a customer) total_amt_usd, how many web_events did they have for each channel?
with t1 as (
    select a.id, a.name, sum(o.total_amt_usd) total_amt
    from accounts a
    join orders o on a.id = o.account_id
    group by 1,2
    order by 3 desc
    limit 1
)

select t1.name, channel, count(*) as events
from web_events
join t1 on t1.id = web_events.account_id
group by 1, 2
order by 3 desc;


-- What is the lifetime average amount spent in terms of total_amt_usd for the top 10 total spending accounts?
with t1 as (
    select a.id, sum(total_amt_usd) total_amt
    from accounts a
    join orders o on a.id = o.account_id
    group by 1
    order by 2 desc
    limit 10
)
select avg(t1.total_amt)
from t1

-- What is the lifetime average amount spent in terms of total_amt_usd, including only the companies that spent more per order, on average, than the average of all orders.
WITH t1 AS (
   SELECT AVG(o.total_amt_usd) avg_all
   FROM orders o
   JOIN accounts a
   ON a.id = o.account_id),
t2 AS (
   SELECT o.account_id, AVG(o.total_amt_usd) avg_amt
   FROM orders o
   GROUP BY 1
   HAVING AVG(o.total_amt_usd) > (SELECT * FROM t1))
SELECT AVG(avg_amt)
FROM t2;