SELECT *
FROM vendor_monthly
WHERE data_variant = $data_variant
  AND month_basis = $month_basis
  AND ($year_month IS NULL OR year_month = $year_month)
  AND ($vendor_id IS NULL OR vendor_id = $vendor_id)
ORDER BY year_month, vendor_id;
