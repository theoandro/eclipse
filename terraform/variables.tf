variable "bucket_name" {
    type = string
    description = "Bucket name"
    default = "eclipse_storage"
}

variable "eclipse_storage_directories" {

    type = list(string)
    description = "Define the list of eclipse storage directories"
}