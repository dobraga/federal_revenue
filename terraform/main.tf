terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  project = local.envs["PROJECT_ID"]
  region  = "us-central1"
  zone    = "us-central1-c"
}
