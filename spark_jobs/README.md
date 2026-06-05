# Spark Jobs - Lab 4

Ce dossier contient les points d'entree Spark executables en ligne de commande pour le Lab 4.

Le pipeline principal du rendu est orchestre par Airflow dans `dags/team_adrien.py`. Le code Spark de production se trouve dans `include/team_adrien_spark.py` et il est appele par la task Airflow `build_gold_kpis`.

## Role dans l'architecture

```text
Bronze CSV
  data/incoming/transactions_<ds>.csv
        |
        v
Silver Parquet
  data/raw/dt=<ds>/transactions.parquet
        |
        v
Gold KPIs PySpark
  data/curated/dt=<ds>/kpis_by_category_country.parquet
        |
        v
Serve JSON
  data/reports/dashboard_<ds>.json
```

## Fichiers importants

| Fichier | Role |
|---------|------|
| `../include/team_adrien_spark.py` | Module PySpark principal du projet. |
| `daily_kpis.py` | Wrapper CLI optionnel pour lancer un job Spark avec `spark-submit`. |
| `README.md` | Documentation rapide pour la demo Spark. |

## Preconditions

Depuis `lab4_student/`, generer d'abord les donnees d'entree :

```bash
python scripts/vendor_drop.py --seed-pack --volume small
python scripts/vendor_drop.py --reference
```

Puis demarrer l'environnement :

```bash
docker compose up -d
```

## Lancement recommande avec Airflow

Le chemin attendu pour le TP est le DAG Airflow :

```bash
docker compose exec airflow-scheduler airflow dags unpause team_adrien
docker compose exec airflow-scheduler airflow dags trigger team_adrien -e 2026-06-01T00:00:00+00:00
```

Airflow execute ensuite :

1. attente du CSV vendor ;
2. ingestion DuckDB vers Silver ;
3. validation qualite ;
4. transformations PySpark ;
5. publication du dashboard JSON.

## Lancement SparkSubmit optionnel

Pour une demo Track X, entrer dans un container Airflow :

```bash
docker compose exec airflow-worker bash
```

Puis lancer :

```bash
spark-submit /opt/airflow/spark_jobs/daily_kpis.py --date 2026-06-01
```

Note : dans le kit starter, `daily_kpis.py` pointe vers le smoke test Spark fourni. Le rendu principal utilise `include/team_adrien_spark.py`, qui peut aussi etre lance en CLI si besoin :

```bash
python /opt/airflow/include/team_adrien_spark.py --date 2026-06-01 --with-reference
```

## Sorties attendues

Apres une execution reussie pour `2026-06-01` :

```text
data/raw/dt=2026-06-01/transactions.parquet
data/curated/dt=2026-06-01/kpis_by_category_country.parquet
data/reports/dashboard_2026-06-01.json
```

Le JSON contient notamment :

- `logical_date`
- `status`
- `spark_version`
- `silver_valid_rows`
- `total_revenue_eur`
- `top_category_country`
- `revenue_by_category`

## Idempotence

Le pipeline est relancable plusieurs fois sur le meme `ds` :

- Silver est reecrit par `ingest_day`.
- Gold est reecrit par Spark avec `mode("overwrite")`.
- Le rapport JSON est remplace par ecriture temporaire puis `replace`.

Il n'y a donc pas d'accumulation de doublons lors d'un re-run.

## Demo d'echec visible dans Airflow

Generer un fichier corrompu :

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

Puis declencher le DAG :

```bash
docker compose exec airflow-scheduler airflow dags trigger team_adrien -e 2026-06-03T00:00:00+00:00
```

La task `validate_silver_quality` doit echouer, car la somme des montants vaut `0.0`. Dans l'UI Airflow, la task apparait en rouge et les logs indiquent une erreur de validation.
