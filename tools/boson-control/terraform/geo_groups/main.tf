terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
      configuration_aliases = [
        aws.ap-east-1, aws.us-west-1, aws.eu-central-1, aws.sa-east-1, aws.af-south-1
      ]
    }
  }
}

variable "instance_type" {
  type    = string
  default = "c5a.xlarge"
}

variable "instance_count" {
  type    = number
  default = 1
}

variable "instance_state" {
  type    = string
  default = "running"
}

module "ap-east-1" {
  source = "../group"
  providers = {
    aws = aws.ap-east-1
  }
  instance_type  = var.instance_type
  instance_count = var.instance_count
  instance_state = var.instance_state
}

module "us-west-1" {
  source = "../group"
  providers = {
    aws = aws.us-west-1
  }
  instance_type  = var.instance_type
  instance_count = var.instance_count
  instance_state = var.instance_state
}

module "eu-central-1" {
  source = "../group"
  providers = {
    aws = aws.eu-central-1
  }
  instance_type  = var.instance_type
  instance_count = var.instance_count
  instance_state = var.instance_state
}

module "sa-east-1" {
  source = "../group"
  providers = {
    aws = aws.sa-east-1
  }
  instance_type  = var.instance_type
  instance_count = var.instance_count
  instance_state = var.instance_state
}

module "af-south-1" {
  source = "../group"
  providers = {
    aws = aws.af-south-1
  }
  instance_type  = var.instance_type
  instance_count = var.instance_count
  instance_state = var.instance_state
}

output "instances" {
  value = concat(
    module.ap-east-1.instances,
    module.us-west-1.instances,
    module.eu-central-1.instances,
    module.sa-east-1.instances,
    module.af-south-1.instances,
  )
}
