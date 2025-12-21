DROP TABLE IF EXISTS dwd_behavior_logs;

CREATE TABLE dwd_behavior_logs AS

SELECT DISTINCT

    user_id,
    video_id,
    action_type,
    duration,

    CAST(time_stamp AS TIMESTAMP) AS every_time,    
    ip

FROM ods_behavior_logs

WHERE user_id IS NOT NULL
    AND video_id IS NOT NULL
    AND duration > 0;