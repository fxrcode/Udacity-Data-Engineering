/**
Lesson 3 - SQL Aggregation
*/

/*****
    --- GROUP
******/

/*
Which account (by name) placed the earliest order? Your solution should have the account name and the date of the order.
*/
SELECT a.name, o.occurred_at
FROM accounts a
JOIN orders o
ON a.id = o.account_id
ORDER BY occurred_at
LIMIT 1;

/*
Find the total sales in usd for each account. You should include two columns - the total sales for each company's orders in usd and the company name.
*/
SELECT a.name, SUM(total_amt_usd) total_sales
FROM orders o
JOIN accounts a
ON a.id = o.account_id
GROUP BY a.name;

/*
Via what channel did the most recent (latest) web_event occur, which account was associated with this web_event? Your query should return only three values - the date, channel, and account name.
*/
SELECT w.occurred_at, w.channel, a.name
FROM web_events w
JOIN accounts a
ON w.account_id = a.id
ORDER BY w.occurred_at DESC
LIMIT 1;

/*
Find the total number of times each type of channel from the web_events was used. Your final table should have two columns - the channel and the number of times the channel was used.
*/
SELECT w.channel, COUNT(*)
FROM web_events w
GROUP BY w.channel

/*
Who was the primary contact associated with the earliest web_event?
*/
SELECT a.primary_poc
FROM web_events w
JOIN accounts a
ON a.id = w.account_id
ORDER BY w.occurred_at
LIMIT 1;

/*
What was the smallest order placed by each account in terms of total usd. Provide only two columns - the account name and the total usd. Order from smallest dollar amounts to largest.
- Sort of strange we have a bunch of orders with no dollars. We might want to look into those.
*/

SELECT a.name, MIN(total_amt_usd) smallest_order
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY a.name
ORDER BY smallest_order;


/*
Find the number of sales reps in each region. Your final table should have two columns - the region and the number of sales_reps. Order from fewest reps to most reps.
*/
SELECT r.name, COUNT(*) num_reps
FROM region r
JOIN sales_reps s
ON r.id = s.region_id
GROUP BY r.name
ORDER BY num_reps;


/*****
    --- HAVING
******/

-- How many of the sales reps have more than 5 accounts that they manage?
SELECT s.id, s.name, COUNT(*) num_accounts
FROM accounts a
JOIN sales_reps s
ON s.id = a.sales_rep_id
GROUP BY s.id, s.name
HAVING COUNT(*) > 5
ORDER BY num_accounts desc;

-- How many accounts have more than 20 orders?
select a.id, COUNT(o.id)
from accounts a
join orders o
on a.id = o.account_id
group by a.id
HAVING COUNT(o.id) > 20
order by COUNT(o.id) desc

-- Which account has the most orders?
SELECT a.id, a.name, COUNT(*) num_orders
FROM accounts a
JOIN orders o
ON a.id = o.account_id
GROUP BY a.id, a.name
ORDER BY num_orders DESC
LIMIT 1;

-- Which accounts spent more than 30,000 usd total across all orders?
select a.id, SUM(o.total_amt_usd)
from accounts a
join orders o
on a.id = o.account_id
group by a.id
HAVING SUM(o.total_amt_usd) > 30000
order by SUM(o.total_amt_usd) desc

-- Which accounts spent less than 1,000 usd total across all orders?
select a.id, SUM(o.total_amt_usd)
from accounts a
join orders o
on a.id = o.account_id
group by a.id
HAVING SUM(o.total_amt_usd) <1000
order by SUM(o.total_amt_usd) desc

-- Which account has spent the most with us?


-- Which account has spent the least with us?


-- Which accounts used facebook as a channel to contact customers more than 6 times?
-- Which account used facebook most as a channel?
select a.id, COUNT(w.id)
from accounts a
join web_events w
on a.id = w.account_id
GROUP by a.id
HAVING COUNT(w.id) > 6 and w.channel = 'facebook'
order by COUNT(w.id) desc


-- Which channel was most frequently used by most accounts?
SELECT a.id, a.name, w.channel, COUNT(*) use_of_channel
FROM accounts a
JOIN web_events w
ON a.id = w.account_id
GROUP BY a.id, a.name, w.channel
ORDER BY use_of_channel DESC
LIMIT 10;