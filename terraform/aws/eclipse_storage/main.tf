resource "aws_s3_bucket" "eclipse_storage" {
  bucket = var.bucket_name
}

resource "aws_s3_object" "data_quality_folder" {

  for_each = toset(var.eclipse_storage_directories)

  bucket = aws_s3_bucket.eclipse_storage.id
  key = "${each.value}/"
  content_type = "application/x-directory"
}