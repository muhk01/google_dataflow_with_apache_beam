# google_dataflow_with_apache_beam
Exercise for Apache Beam pipeline and executor in Google Dataflow

# Submit python program to Dataflow Job
by using this command specify google storage file and project
```
python bank_defaulter_dataflow.py --input1 gs://<bucket>/<cards.txt> --input2 gs://<bucket>/loan.txt --output gs://<bucket>/output --runner DataflowRunner --project project_name --temp_location gs://<bucket>/tmp --region us-central1
```
