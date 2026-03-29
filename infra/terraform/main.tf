terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.5.0"
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Bronze Bucket ──────────────────────────────────────
resource "aws_s3_bucket" "bronze" {
  bucket = "${var.project_name}-bronze-${var.aws_account_id}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "bronze"
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "bronze-lifecycle"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    expiration {
      days = 90
    }
  }
}

# ── IAM User for Pipeline ─────────────────────────────────
resource "aws_iam_user" "pipeline_user" {
  name = "${var.project_name}-pipeline-user"

  tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
  }
}

resource "aws_iam_access_key" "pipeline_user" {
  user = aws_iam_user.pipeline_user.name
}

resource "aws_iam_policy" "pipeline_policy" {
  name        = "${var.project_name}-pipeline-policy"
  description = "Allows pipeline user to read/write S3 bronze bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject",
        ]
        Resource = "${aws_s3_bucket.bronze.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.bronze.arn
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "pipeline" {
  user       = aws_iam_user.pipeline_user.name
  policy_arn = aws_iam_policy.pipeline_policy.arn
}

# ── Outputs ───────────────────────────────────────────────
output "s3_bucket_name" {
  description = "Name of the Bronze S3 bucket"
  value       = aws_s3_bucket.bronze.bucket
  sensitive   = true
}

output "iam_access_key_id" {
  description = "Access key ID for pipeline IAM user"
  value       = aws_iam_access_key.pipeline_user.id
  sensitive   = true
}

output "iam_secret_access_key" {
  description = "Secret access key for pipeline IAM user"
  value       = aws_iam_access_key.pipeline_user.secret
  sensitive   = true
}
# Override outputs to handle sensitive variable propagation
