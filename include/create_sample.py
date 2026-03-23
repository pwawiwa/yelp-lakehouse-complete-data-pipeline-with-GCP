import os
from google.cloud import storage

def create_sample():
    client = storage.Client('yelp-490821')
    bucket = client.bucket('yelp-490821-bronze')
    
    print("Reading 1000 lines from business json...")
    lines = []
    with open('/usr/local/airflow/include/data/yelp_academic_dataset_business.json', 'r') as f:
        for _ in range(1000):
            try:
                lines.append(next(f))
            except StopIteration:
                break
    
    print("Uploading to GCS...")
    blob = bucket.blob('yelp/raw/entity=sample_business/sample.json')
    blob.upload_from_string(''.join(lines))
    print("✅ Uploaded 1000 lines to gs://yelp-490821-bronze/yelp/raw/entity=sample_business/sample.json")

if __name__ == '__main__':
    create_sample()
