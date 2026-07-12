SELECT *
FROM data_quality_summary
WHERE ($month_basis IS NULL OR month_basis = $month_basis)
  AND ($metric_category IS NULL OR metric_category = $metric_category)
  AND ($source_month IS NULL OR source_month = $source_month)
ORDER BY month_basis, source_month NULLS FIRST, metric_category, metric_name;
