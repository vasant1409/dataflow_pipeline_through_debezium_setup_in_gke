This pipeline consists of the debezium server set up in Google Kubernetes Engine and more google services, 
First the postgres database was installed in VM server then change events was sent to pubsub subscription through debezium.
Then using dataflow custom template the source data is streamed into bigquery with manipulated fields.

