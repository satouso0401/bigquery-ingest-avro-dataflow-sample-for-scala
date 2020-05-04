bigquery-ingest-avro-dataflow-sample-for-scala
====

## What is this?

Streaming Avro records into BigQuery using Dataflow with Scala.
This sample code was rewrite in Scala from the official sample code written in Java.
See also the related official article below.

https://github.com/GoogleCloudPlatform/bigquery-ingest-avro-dataflow-sample
https://cloud.google.com/solutions/streaming-avro-records-into-bigquery-using-dataflow

## Usage

Environment setting
```
export GOOGLE_APPLICATION_CREDENTIALS=[CREDENTIAL_FILE_PATH]
vi env.sh
. ./env.sh
```

Creating resources
```
gcloud pubsub topics create $MY_TOPIC
gsutil mb -l $REGION -c regional gs://$MY_BUCKET
bq --location=$BQ_REGION mk --dataset $GOOGLE_CLOUD_PROJECT:$BQ_DATASET
```

Generate Avro classes
```
cd beam-avro
sbt compile
```

Start Dataflow
```
sbt "run
 --project=$GOOGLE_CLOUD_PROJECT
 --runner=DataflowRunner
 --stagingLocation=gs://$MY_BUCKET/stage/
 --inputPath=projects/$GOOGLE_CLOUD_PROJECT/topics/$MY_TOPIC
 --workerMachineType=n1-standard-1
 --maxNumWorkers=$NUM_WORKERS
 --region=$REGION
 --dataset=$BQ_DATASET
 --bqTable=$BQ_TABLE
 --outputPath=$AVRO_OUT
 --format=AVRO"
```

Send data
```
cd generator
python3 -m venv env
. ./env/bin/activate
pip install -r requirements.txt
python3 gen.py -p $GOOGLE_CLOUD_PROJECT -t $MY_TOPIC -n 100 -f avro
```

