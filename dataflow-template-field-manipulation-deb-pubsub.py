import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from google.cloud import bigquery, pubsub_v1
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

PROJECT_ID = "bionic-axon-448509-r2"
SUBSCRIPTION_ID = "debezium-postgres.public.employees-sub"
BQ_DATASET = "vasant_test"
BQ_TABLE = "employees_test"
TEMP_LOCATION = "gs://bucket-vas/dataflow-templates/temp"
STAGING_LOCATION = "gs://bucket-vas/dataflow-templates/temp/staging"
TEMPLATE_LOCATION = "gs://bucket-vas/dataflow-templates/psql-to-bigquery-df-template"
REGION = "asia-south1"
WORKER_DISK_SIZE_GB = 30

KEYS_TO_EXTRACT = [
    "employee_id", "first_name", "last_name", "position",
    "department", "salary", "hire_date", "city"
]

class ProcessPubSubMessage(beam.DoFn):
    def __init__(self, project_id, dataset, table, keys_to_extract):
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.keys_to_extract = keys_to_extract
        self.bq_client = None
        self.schema = None
        self.thread_pool = None
        self.publisher = None

    def setup(self):

        from google.cloud import bigquery, pubsub_v1
        from concurrent.futures import ThreadPoolExecutor

        self.bq_client = bigquery.Client()
        self.schema = self._fetch_bq_schema()
        self.thread_pool = ThreadPoolExecutor(max_workers=2)
        self.publisher = pubsub_v1.PublisherClient()

    def teardown(self):
        if self.thread_pool:
            self.thread_pool.shutdown(wait=True)

    def _fetch_bq_schema(self):
        table_id = f"{self.project_id}.{self.dataset}.{self.table}"
        table = self.bq_client.get_table(table_id)
        return {field.name: field.field_type for field in table.schema}

    def _extract_key(self, key, message_dict):

        import json
        import logging

        try:
            after_data = message_dict.get("payload", {}).get("after", {})
            return key, after_data.get(key, None)
        except Exception as e:
            logging.error(f"Error extracting key {key}: {e}")
            return key, None

    def _parse_message(self, data):

        import json
        import logging

        try:
            logging.info(f"Raw message: {data}")
            message_dict = json.loads(data)
            after_data = message_dict.get("payload", {}).get("after")

            if not after_data:
                logging.warning("Skipping message with no 'after' data.")
                return None

            logging.info(f"Parsed JSON: {after_data}")
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON. Message: {data}")
            return None

        future_results = {key: self.thread_pool.submit(self._extract_key, key, message_dict) for key in self.keys_to_extract}
        result = {key: future.result()[1] for key, future in future_results.items()}

        logging.info(f"Extracted data: {result}")
        return result

    def _publish_to_failed_topic(self, original_message):

        import json
        import logging

        FAILED_TOPIC = "projects/bionic-axon-448509-r2/topics/debezium-postgres.public.failed-employees"
        
        try:
            self.publisher.publish(FAILED_TOPIC, original_message.encode("utf-8"))
            logging.info(f"Published failed message to {FAILED_TOPIC}")
        except Exception as e:
            logging.error(f"Failed to publish to Pub/Sub: {e}")

    def _insert_into_bigquery(self, parsed_data, original_message):

        import json
        import logging

        table_id = f"{self.project_id}.{self.dataset}.{self.table}"
        
        data_to_insert = {
            "employee_id": int(parsed_data.get("employee_id", 0)) if parsed_data.get("employee_id") else None,
            "first_name": str(parsed_data.get("first_name")) if parsed_data.get("first_name") else None,
            "last_name": str(parsed_data.get("last_name")) if parsed_data.get("last_name") else None,
            "position": str(parsed_data.get("position")) if parsed_data.get("position") else None,
            "department": str(parsed_data.get("department")) if parsed_data.get("department") else None,
            "salary": float(parsed_data.get("salary")) if parsed_data.get("salary") else None,
            "hire_date": datetime.utcfromtimestamp(int(parsed_data.get("hire_date", 0)) * 86400).date().isoformat()
                         if parsed_data.get("hire_date") else None,
            "city": str(parsed_data.get("city")) if parsed_data.get("city") else None
        }
        
        if not any(data_to_insert.values()):
            logging.warning("No valid data to insert into BigQuery.")
            self._publish_to_failed_topic(original_message)
            return

        try:
            errors = self.bq_client.insert_rows_json(table_id, [data_to_insert])
            if errors:
                logging.error(f"Failed to insert into BigQuery: {errors}, Data: {data_to_insert}")
                self._publish_to_failed_topic(original_message)
            else:
                logging.info(f"Successfully inserted row into {table_id}")
        except Exception as e:
            logging.error(f"Error inserting into BigQuery: {e}")
            self._publish_to_failed_topic(original_message)

    def process(self, element):
        parsed_data = self._parse_message(element)
        if parsed_data:
            self._insert_into_bigquery(parsed_data, element)
        else:
            logging.warning("Invalid message received, skipping insertion.")

def run_pipeline():

    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

    logging.basicConfig(level=logging.INFO)
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "ReadMessages" >> beam.io.ReadFromPubSub(subscription=subscription_path)
            | "DecodeMessages" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "ProcessMessages" >> beam.ParDo(ProcessPubSubMessage(
                project_id=PROJECT_ID,
                dataset=BQ_DATASET,
                table=BQ_TABLE,
                keys_to_extract=KEYS_TO_EXTRACT
            ))
        )

if __name__ == "__main__":
    run_pipeline()
