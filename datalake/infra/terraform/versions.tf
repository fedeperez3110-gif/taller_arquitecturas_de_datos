terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    http = {
      source  = "hashicorp/http"
      version = "~> 3.4"
    }
  }

  # Estado local (terraform.tfstate), ignorado por git.
  # Suficiente para un proyecto individual; en equipo se usaría un backend S3.
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project   = var.project
      ManagedBy = "terraform"
      Course    = "EIA-Arquitecturas-de-Datos"
    }
  }
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
