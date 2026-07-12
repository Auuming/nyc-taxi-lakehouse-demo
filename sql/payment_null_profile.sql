SELECT *
FROM payment_null_profile
WHERE data_variant = $data_variant
  AND ($payment_type IS NULL OR payment_type = $payment_type)
ORDER BY payment_type, field_name;
