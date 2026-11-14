terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  # Estado local (terraform.tfstate), ignorado por git. Contiene las contraseñas generadas:
  # no se comparte. En equipo se usaría un backend S3 cifrado.
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
