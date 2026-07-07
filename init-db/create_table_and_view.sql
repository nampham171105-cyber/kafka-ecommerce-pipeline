CREATE TABLE IF NOT EXISTS kafka_events_silver (
    event_id UUID PRIMARY KEY,
    customer_id TEXT,
    event_type TEXT,
    amount NUMERIC,
    currency TEXT,
    event_timestamp TIMESTAMP
);

CREATE OR REPLACE VIEW daily_customer_revenue AS
SELECT
    customer_id,
    DATE(event_timestamp) AS event_date,
    SUM(amount) AS total_revenue,
    COUNT(*) FILTER (WHERE event_type = 'PURCHASE') AS purchase_count
FROM kafka_events_silver
WHERE event_type = 'PURCHASE'
GROUP BY customer_id, DATE(event_timestamp);
 
CREATE OR REPLACE VIEW event_funnel AS
SELECT
    DATE(event_timestamp) AS event_date,
    COUNT(*) FILTER (WHERE event_type = 'PAGE_VIEW') AS page_views,
    COUNT(*) FILTER (WHERE event_type = 'ADD_TO_CART') AS add_to_cart,
    COUNT(*) FILTER (WHERE event_type = 'PURCHASE') AS purchases
FROM kafka_events_silver
GROUP BY DATE(event_timestamp);