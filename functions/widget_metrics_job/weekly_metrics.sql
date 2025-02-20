WITH weekly_metrics AS (
	SELECT widget_id,
		COUNT(*) AS total_launches,
		COUNT(DISTINCT user_id) AS unique_views
	FROM widget_launches
	WHERE timestamp >= NOW() - INTERVAL '7 days'
	GROUP BY widget_id
)
INSERT INTO widget_metrics (widget_id, timeframe_type, timeframe_start, total_launches, unique_launches)
SELECT widget_id, 
	'weekly', 
	NOW() - INTERVAL '7 days', 
	total_launches, 
	unique_views
FROM weekly_metrics;

WITH monthly_metrics AS (
    SELECT widget_id,
           COUNT(*) AS total_launches,
           COUNT(DISTINCT user_id) AS unique_views
    FROM widget_launches
    WHERE timestamp >= NOW() - INTERVAL '30 days'
    GROUP BY widget_id
)
INSERT INTO widget_metrics (widget_id, timeframe_type, timeframe_start, total_launches, unique_launches)
SELECT widget_id, 
       'monthly', 
       NOW() - INTERVAL '30 days', 
       total_launches, 
       unique_views
FROM monthly_metrics;

WITH yearly_metrics AS (
    SELECT widget_id,
           COUNT(*) AS total_launches,
           COUNT(DISTINCT user_id) AS unique_views
    FROM widget_launches
    WHERE timestamp >= NOW() - INTERVAL '365 days'
    GROUP BY widget_id
)
INSERT INTO widget_metrics (widget_id, timeframe_type, timeframe_start, total_launches, unique_launches)
SELECT widget_id, 
       'yearly',
       NOW() - INTERVAL '365 days', 
       total_launches, 
       unique_views
FROM yearly_metrics;

WITH all_time_metrics AS (
    SELECT widget_id,
           COUNT(*) AS total_launches,
           COUNT(DISTINCT user_id) AS unique_views
    FROM widget_launches
    GROUP BY widget_id
)
INSERT INTO widget_metrics (widget_id, timeframe_type, timeframe_start, total_launches, unique_launches)
SELECT widget_id, 
       'all_time', 
       '1970-01-01'::timestamp,  -- or the earliest date you want to track fro 
       total_launches, 
       unique_views
FROM all_time_metrics;
