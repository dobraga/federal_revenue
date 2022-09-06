resource "google_storage_bucket" "bucket" {
  name          = local.envs["BUCKET_NAME"]
}