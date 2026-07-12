SELECT *
FROM monthly_kpi
WHERE data_variant = $data_variant
  AND month_basis = $month_basis
ORDER BY year_month;
