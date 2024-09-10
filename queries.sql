-- 1. How many orders are in the dataset?
SELECT COUNT(*) AS total_orders FROM orders;

-- How many orders were from each channel
SELECT
    o.channel AS channel,
    COUNT(i.item_id) AS items_sold
FROM
    orders o
JOIN
    baskets b ON b.order_id = o.order_id
JOIN
    items i ON i.basket_id = b.basket_id
GROUP BY
    channel

-- 3. How many items were sold for each hour of the day for each tenant?
SELECT
    o.tenant_id,
    EXTRACT(HOUR FROM TO_TIMESTAMP(o.created_at / 1000)) AS hour_of_day,
    COUNT(i.item_id) AS items_sold
FROM
    orders o
JOIN
    baskets b ON b.order_id = o.order_id
JOIN
    items i ON i.basket_id = b.basket_id
GROUP BY
    o.tenant_id,
    hour_of_day
ORDER BY
    o.tenant_id,
    hour_of_day;

-- 4. What were the top 5 items sold for each tenant?
WITH ItemSales AS (
    SELECT
        o.tenant_id AS tenant_id,
        i.name AS item_name,
        COUNT(i.item_id) AS items_sold
    FROM
        orders o
    JOIN
        baskets b ON b.order_id = o.order_id
    JOIN
        items i ON i.basket_id = b.basket_id
    GROUP BY
        o.tenant_id,
        i.name
),
RankedItems AS (
    SELECT
        tenant_id,
        item_name,
        items_sold,
        ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY items_sold DESC) AS rank
    FROM
        ItemSales
)
SELECT
    tenant_id,
    item_name,
    items_sold,
    rank
FROM
    RankedItems
WHERE
    rank <= 5
ORDER BY
    tenant_id,
    rank;

-- 5. What were the items for each tenant that were sold more than 5 of?
SELECT
    o.tenant_id AS tenant_id,
    i.name AS item_name,
    COUNT(i.item_id) AS items_sold
FROM
    orders o
JOIN
    baskets b ON b.order_id = o.order_id
JOIN
    items i ON i.basket_id = b.basket_id
GROUP BY
    o.tenant_id,
    i.name
HAVING
    COUNT(i.item_id) > 5
ORDER BY
    o.tenant_id,
    items_sold DESC;
    
-- 6. Which order UUIDs had multiples of the same bundle?
SELECT
    o.order_id,
    b.name,
    COUNT(b.bundle_id) AS bundle_count
FROM orders o
JOIN bundles b ON o.order_id = b.order_id
GROUP BY o.order_id, b.name
HAVING COUNT(b.bundle_id) > 1;