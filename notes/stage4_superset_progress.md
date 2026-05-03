# Stage IV — delivery checklist (team14)

Course reference: [BS/MS Stage IV - Presentation](https://firas-jolha.github.io/bigdata/html/common/BS_MS%20Stage%20IV%20-%20Presentation.html)

## Superset dashboard

- **URL:** `http://hadoop-03.uni.innopolis.ru:8808/superset/dashboard/218/`
- **Title:** `Flight Delay & Cancellation Analytics (Team 14)`
- **Layout:** Data (Postgres) → EDA (Hive `q1`–`q5`) → ML (Hive `ml_*`): metrics, hyperparameter grid, feature catalog, three prediction samples, Markdown/headers/dividers.
- **Legacy slug (if any):** `.../dashboard/p/M3rnyjyd7vd/` — prefer **218** in the report.

## Course checklist ↔ artifacts

| Checklist item | Artifact |
|----------------|-----------|
| Dashboard + project title | Superset dashboard title + headers |
| Organized layout | Rows/columns/tabs as built |
| **Postgres** charts in data description | `public.flights` sample (+ optional counts/types charts or SQL appendix in report) |
| Row counts, column types, samples, cleaning notes | Covered by charts and/or Markdown + report text |
| Stage II charts in insights | Five Hive `q*_results` charts |
| External Hive tables for Stage III | `sql/stage4_ml_dashboard.hql` + `bash scripts/stage4.sh` → `ml_*` in `team14_projectdb` |
| Hive charts: features, hyperparams, predictions, evaluation, comparison | Tables/charts on `ml_feature_catalog`, `ml_hyperparam_grid`, `ml_pred_*`, `ml_eval_metrics` |
| Publish dashboard | Published URL above |
| Automate (except Superset UI) | `scripts/stage4.sh`, `sql/data/*.csv`, `sql/stage4_ml_dashboard.hql` |
| Run `stage4.sh` | Tested on cluster; log `output/stage4_hive.txt` |
| **pylint** | `scripts/stage4_validate.py` (local file checks + optional `bash -n`); full `pylint scripts` as in `main.sh` on grader host |
| Report summary | LaTeX: `report/stage4_body_en.tex` (\input into Overleaf master) |

## Grading rubric (15 pts) — self-map

| Points | Criterion | Dashboard coverage |
|--------|-----------|-------------------|
| 1 | Data characteristics | Postgres sample + text; strengthen with counts/types if needed |
| 6 | Insights + description | Five EDA charts + Markdown takeaways |
| 4 | Model performance | `ml_eval_metrics` comparison table |
| 2 | Prediction results | Three `ml_pred_*` sample tables |
| 2 | Visual quality | Section headers, dividers, grid alignment |

## Repository / `main.sh` (assessment)

**`main.sh` must not be edited for grading** (see repo README). The **echo** line for Stage 4 matches the **template on `main`**: *“Streamlit”* — even though `scripts/stage4.sh` implements **Hive/Superset prep**, not Streamlit. **Do not change `main.sh` content** when merging to `main` for submission.

## Automation commands (cluster)

```bash
cd ~/project/big_data_project
bash scripts/stage4.sh
```

## Local smoke (any machine with bash optional)

```bash
python scripts/stage4_validate.py
python -m unittest tests.test_stage4_assets -v
```
