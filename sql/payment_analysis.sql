SELECT *
FROM payment_monthly
WHERE data_variant = $data_variant
  AND month_basis = $month_basis
  AND ($year_month IS NULL OR year_month = $year_month)
  AND ($payment_type IS NULL OR payment_type = $payment_type)
ORDER BY year_month, payment_type;
