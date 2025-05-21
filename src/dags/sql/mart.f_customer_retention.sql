-- 1. new_customers_count — кол-во новых клиентов (тех, которые сделали только один
-- заказ за рассматриваемый промежуток времени).
-- 2. returning_customers_count — кол-во вернувшихся клиентов (тех,
-- которые сделали только несколько заказов за рассматриваемый промежуток времени).
-- 3. refunded_customer_count — кол-во клиентов, оформивших возврат за
-- рассматриваемый промежуток времени.
-- 4. period_name — weekly.
-- 5. period_id — идентификатор периода (номер недели или номер месяца).
-- 6. item_id — идентификатор категории товара.
-- 7. new_customers_revenue — доход с новых клиентов.
-- 8. returning_customers_revenue — доход с вернувшихся клиентов.
-- 9. customers_refunded — количество возвратов клиентов.

truncate mart.f_customer_retention;

WITH weekly_customer_orders_by_item AS (SELECT customer_id,
                                               item_id,
                                               EXTRACT(WEEK FROM date_time) as week_number,
                                               COUNT(*)                     as orders_in_week,
                                               SUM(payment_amount)          as weekly_revenue
                                        FROM staging.user_order_log
                                        WHERE status = 'shipped'
                                        GROUP BY customer_id,
                                                 item_id,
                                                 EXTRACT(WEEK FROM date_time)),
     -- Общее кол-во заказов по клиентам
     customer_total_orders AS (SELECT customer_id,
                                      COUNT(*) as total_orders
                               FROM staging.user_order_log
                               WHERE status = 'shipped'
                               GROUP BY customer_id),
     -- Классификация клиентов, если у него 1 заказ за выбранную неделю
     customer_types AS (SELECT wco.week_number,
                               wco.item_id,
                               wco.customer_id,
                               wco.weekly_revenue,
                               CASE
                                   WHEN cto.total_orders = 1 THEN 'new'
                                   ELSE 'returning'
                                   END as customer_type
                        FROM weekly_customer_orders_by_item wco
                                 JOIN customer_total_orders cto ON wco.customer_id = cto.customer_id),
     -- Клиенты, которые отказывали заказы
     refunded_customers AS (SELECT EXTRACT(WEEK FROM date_time) as week_number,
                                   COUNT(DISTINCT customer_id)  as refunded_count
                            FROM staging.user_order_log
                            WHERE status = 'refunded'
                            GROUP BY week_number)

INSERT
INTO mart.f_customer_retention
SELECT 'weekly'                                                                   as period_name,
       ct.week_number                                                             as period_id,
       ct.item_id,
       COUNT(DISTINCT CASE WHEN customer_type = 'new' THEN customer_id END)       as new_customers_count,
       COUNT(DISTINCT CASE WHEN customer_type = 'returning' THEN customer_id END) as returning_customers_count,
       COALESCE(rc.refunded_count, 0)                                             as refunded_customer_count,
       SUM(CASE WHEN customer_type = 'new' THEN weekly_revenue ELSE 0 END)        as new_customers_revenue,
       SUM(CASE WHEN customer_type = 'returning' THEN weekly_revenue ELSE 0 END)  as returning_customers_revenue,
       COALESCE(rc.refunded_count, 0)                                             as customers_refunded
FROM customer_types ct
         LEFT JOIN refunded_customers rc
                   on ct.week_number = rc.week_number
GROUP BY ct.week_number,
         ct.item_id,
         rc.refunded_count;
