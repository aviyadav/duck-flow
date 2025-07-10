import os
from ingestion.bigquery import get_bigquery_client, get_bigquery_result, build_pypi_query

def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "root/.config/gcloud/demo-pypi.json"
    df= get_bigquery_result(build_pypi_query(), get_bigquery_client("demo-pypi"))
    print(df.head())


if __name__ == '__main__':
    main()