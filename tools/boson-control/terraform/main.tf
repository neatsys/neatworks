terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  alias  = "ap-east-1"
  region = "ap-east-1"
}

provider "aws" {
  alias  = "ap-southeast-1"
  region = "ap-southeast-1"
}

provider "aws" {
  alias  = "us-west-1"
  region = "us-west-1"
}

provider "aws" {
  alias  = "eu-central-1"
  region = "eu-central-1"
}

provider "aws" {
  alias  = "sa-east-1"
  region = "sa-east-1"
}

provider "aws" {
  alias  = "af-south-1"
  region = "af-south-1"
}

variable "state" {
  type    = string
  default = "running"
}

variable "mode" {
  type = string
  validation {
    condition     = contains(["mutex", "cops", "microbench"], var.mode)
    error_message = "Unexpected mode."
  }
}

module "microbench" {
  source = "./group"
  count  = 1 # has been using to build nitro enclaves image so always enable
  providers = {
    aws = aws.ap-southeast-1
  }
  instance_state = var.state
  instance_type  = "c5a.2xlarge"
}

module "microbench_quorum" {
  source = "./group"
  count  = var.mode == "microbench" ? 1 : 0
  providers = {
    aws = aws.ap-southeast-1
  }
  instance_state = var.state
  instance_count = 10
}

module "mutex" {
  source = "./geo_groups"
  count  = var.mode == "mutex" ? 1 : 0
  providers = {
    aws.ap-east-1    = aws.ap-east-1
    aws.us-west-1    = aws.us-west-1
    aws.eu-central-1 = aws.eu-central-1
    aws.sa-east-1    = aws.sa-east-1
    aws.af-south-1   = aws.af-south-1
  }
  instance_state = var.state
  instance_type  = "c5a.2xlarge"
  instance_count = 20
}

module "cops" {
  source = "./geo_groups"
  count  = var.mode == "cops" ? 1 : 0
  providers = {
    aws.ap-east-1    = aws.ap-east-1
    aws.us-west-1    = aws.us-west-1
    aws.eu-central-1 = aws.eu-central-1
    aws.sa-east-1    = aws.sa-east-1
    aws.af-south-1   = aws.af-south-1
  }
  instance_state = var.state
  instance_type  = "c5a.8xlarge"
}

module "cops_client" {
  source = "./geo_groups"
  count  = var.mode == "cops" ? 1 : 0
  providers = {
    aws.ap-east-1    = aws.ap-east-1
    aws.us-west-1    = aws.us-west-1
    aws.eu-central-1 = aws.eu-central-1
    aws.sa-east-1    = aws.sa-east-1
    aws.af-south-1   = aws.af-south-1
  }
  instance_state = var.state
  instance_type  = "c5a.2xlarge"
}

module "quorum" {
  source = "./geo_groups"
  # count  = contains(["cops", "mutex"], var.mode) ? 1 : 0
  count = 0
  providers = {
    aws.ap-east-1    = aws.ap-east-1
    aws.us-west-1    = aws.us-west-1
    aws.eu-central-1 = aws.eu-central-1
    aws.sa-east-1    = aws.sa-east-1
    aws.af-south-1   = aws.af-south-1
  }
  instance_state = var.state
  instance_type  = "c5a.8xlarge"
  instance_count = 2
}

output "microbench_instances" {
  value = flatten(module.microbench[*].instances)
}

output "microbench_quorum_instances" {
  value = flatten(module.microbench_quorum[*].instances)
}

output "mutex_instances" {
  value = flatten(module.mutex[*].instances)
}

output "cops_instances" {
  value = flatten(module.cops[*].instances)
}

output "cops_client_instances" {
  value = flatten(module.cops_client[*].instances)
}

output "quorum_instances" {
  value = flatten(module.quorum[*].instances)
}
