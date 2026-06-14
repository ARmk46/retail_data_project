terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# 1. Define the Cloud Region
provider "aws" {
  region = "us-east-1"
}

# 2. Create the Bronze Data Lake Storage
resource "aws_s3_bucket" "bronze_lake" {
  bucket        = "retail-bronze-lake-mohitk-2026" # CHANGE THIS TO A UNIQUE NAME
  force_destroy = true # Allows you to easily delete the bucket later for cleanup
}

# 3. Create the Strict IAM Policy (Principle of Least Privilege)
resource "aws_iam_policy" "bronze_access" {
  name        = "KafkaConnectBronzeAccess"
  description = "Allows Kafka to write only to the Bronze S3 bucket"
  policy      = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Effect   = "Allow"
        Resource = aws_s3_bucket.bronze_lake.arn
      },
      {
        Action   = [
          "s3:PutObject", 
          "s3:GetObject", 
          "s3:DeleteObject",
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts"
        ]
        Effect   = "Allow"
        Resource = "${aws_s3_bucket.bronze_lake.arn}/*"
      },
    ]
  })
}

# 4. Create the Kafka Service Account Bot
resource "aws_iam_user" "kafka_bot" {
  name = "kafka-connect-s3-bot"
}

# Attach the strict policy to the Bot
resource "aws_iam_user_policy_attachment" "bot_attach" {
  user       = aws_iam_user.kafka_bot.name
  policy_arn = aws_iam_policy.bronze_access.arn
}

# 5. GENERATE THE KEYS
resource "aws_iam_access_key" "kafka_keys" {
  user = aws_iam_user.kafka_bot.name
}

# 6. Output the credentials to your terminal
output "KAFKA_AWS_ACCESS_KEY_ID" {
  value = aws_iam_access_key.kafka_keys.id
}
output "KAFKA_AWS_SECRET_ACCESS_KEY" {
  value     = aws_iam_access_key.kafka_keys.secret
  sensitive = true 
}