# Streamlit Dashboard

The dashboard supports two independent dimensions:

```text
data_variant:
- all
- exclude_zero_duration
- exclude_zero_distance
- exclude_both

month_basis:
- pickup_month
- source_month
```

Run:

```bash
pip install -r requirements-dashboard.txt
python pipelines/build_gold.py
streamlit run dashboard/app.py
```
