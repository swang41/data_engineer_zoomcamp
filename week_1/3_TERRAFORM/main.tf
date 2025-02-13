terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = "zoomcamp-data-engineer-450"
  region  = "us-central1"
}

resource "google_storage_bucket" "demo-bucket" {
  name          = "zoomcamp-data-engineer-450-terra-bucket"
  location      = "US"
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }
}