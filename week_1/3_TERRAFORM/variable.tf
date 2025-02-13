variable "credentials" {
    description = "My Credentials"
    default = "./keys/my_cred.json"
}

variable "project_id" {
    description = "Project ID"
    default = "zoomcamp-data-engineer-450"
}

variable "bq_dataset_name" {
    description = "My BigQuery Dataset Name"
    default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "My Storange Bucket Name"
    default = "zoomcamp-data-engineer-450-terra-bucket"
}

variable "location" {
    description = "location"
    default = "US"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}