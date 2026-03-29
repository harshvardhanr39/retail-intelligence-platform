variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Project name used in all resource names"
  type        = string
  default     = "retail-intelligence"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "production"
}

variable "aws_account_id" {
  description = "AWS Account ID for unique bucket naming"
  type        = string
  sensitive   = true
}
