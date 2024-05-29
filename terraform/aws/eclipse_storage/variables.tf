variable "bucket_name" {
  type = string
  description = "Bucket name to store ingested and processed data"
}

variable "eclipse_storage_directories" {

  type = list(string)
  description = "Define the list of eclipse storage directories"
}

