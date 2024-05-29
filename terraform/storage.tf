module "eclipse_storage" {
  source = "./aws/eclipse_storage"
  bucket_name = var.bucket_name
  eclipse_storage_directories = var.eclipse_storage_directories
}