SELECT *
FROM daily_trip_summary
WHERE data_variant = $data_variant
ORDER BY trip_date;
