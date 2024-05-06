terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  alias  = "control"
  region = "ap-south-1"
}

provider "aws" {
  alias  = "ap"
  region = "ap-east-1"
}

provider "aws" {
  alias  = "us"
  region = "us-west-1"
}

provider "aws" {
  alias  = "eu"
  region = "eu-central-1"
}

provider "aws" {
  alias  = "sa"
  region = "sa-east-1"
}

provider "aws" {
  alias  = "af"
  region = "af-south-1"
}

variable "state" {
  type    = string
  default = "running"
}

variable "mode" {
  type    = string
  default = "microbench"
  validation {
    condition     = contains(["mutex", "cops", "microbench"], var.mode)
    error_message = "Unexpected mode."
  }
}

data "aws_ami" "al2023" {
  provider = aws.control

  most_recent = true
  filter {
    name   = "name"
    values = ["al2023-ami-2023.4.20240416.0-kernel-6.1-x86_64"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
  owners = ["137112412989", "910595266909", "210953353124"] # amazon
}

module "microbench_network" {
  source = "./group_network"
  providers = {
    aws = aws.control
  }
}

module "microbench" {
  source = "./group"
  providers = {
    aws = aws.control
  }

  network = module.microbench_network
  state   = var.state
  type    = var.mode == "microbench" ? "c5a.16xlarge" : "c5a.xlarge"
  ami     = data.aws_ami.al2023.id
}

module "microbench_quorum" {
  source = "./group"
  count  = var.mode == "microbench" ? 1 : 0
  providers = {
    aws = aws.control
  }

  network = module.microbench_network
  ami     = data.aws_ami.al2023.id
  state   = var.state
  n       = 10
}

module "ap" {
  source = "./region"
  providers = {
    aws = aws.ap
  }

  mode  = var.mode
  state = var.state
}

module "us" {
  source = "./region"
  providers = {
    aws = aws.us
  }

  mode  = var.mode
  state = var.state
}

module "eu" {
  source = "./region"
  providers = {
    aws = aws.eu
  }

  mode  = var.mode
  state = var.state
}

module "sa" {
  source = "./region"
  providers = {
    aws = aws.sa
  }

  mode  = var.mode
  state = var.state
}

module "af" {
  source = "./region"
  providers = {
    aws = aws.af
  }

  mode  = var.mode
  state = var.state
}

output "instances" {
  value = {
    microbench        = module.microbench.instances
    microbench_quorum = flatten(module.microbench_quorum[*].instances)
    regions = {
      ap = module.ap.instances
      us = module.us.instances
      eu = module.eu.instances
      sa = module.sa.instances
      af = module.af.instances
    }
  }
}
