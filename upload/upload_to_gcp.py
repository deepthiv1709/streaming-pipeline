import os
from google.cloud import storage
from config.settings import GCP_BUCKET, GCP_OUTPUT_PATH, OUTPUT_DIR

def upload_parquet_to_gcp():
    """Upload all local Parquet files to GCP Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(GCP_BUCKET)

    uploaded = 0
    for root, _, files in os.walk(OUTPUT_DIR):
        for file in files:
            if file.endswith(".parquet"):
                local_path  = os.path.join(root, file)
                remote_path = os.path.join(
                    GCP_OUTPUT_PATH,
                    os.path.relpath(local_path, OUTPUT_DIR)
                )
                blob = bucket.blob(remote_path)
                blob.upload_from_filename(local_path)
                print(f"Uploaded: {local_path} → gs://{GCP_BUCKET}/{remote_path}")
                uploaded += 1

    print(f"\nDone. {uploaded} files uploaded to GCP.")

if __name__ == "__main__":
    upload_parquet_to_gcp()