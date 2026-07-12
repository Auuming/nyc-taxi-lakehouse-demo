SELECT *
FROM zone_flow_monthly
WHERE data_variant = $data_variant
  AND month_basis = $month_basis
  AND ($year_month IS NULL OR year_month = $year_month)
  AND ($pickup_location_id IS NULL OR pickup_location_id = $pickup_location_id)
  AND ($dropoff_location_id IS NULL OR dropoff_location_id = $dropoff_location_id)
ORDER BY trip_count DESC
LIMIT $row_limit;
