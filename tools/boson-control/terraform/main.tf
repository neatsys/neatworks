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

variable "throughput" {
  type    = bool
  default = false
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
  type    = var.throughput ? "c5a.16xlarge" : "t3.micro"
  n       = var.throughput ? 2 : 10
}

module "ap" {
  source = "./region"
  count = var.mode == "microbench" ? 0 : 1
  providers = {
    aws = aws.ap
  }

  mode  = var.mode
  state = var.state
}

module "us" {
  source = "./region"
  count = var.mode == "microbench" ? 0 : 1
  providers = {
    aws = aws.us
  }

  mode  = var.mode
  state = var.state
}

module "eu" {
  source = "./region"
  count = var.mode == "microbench" ? 0 : 1
  providers = {
    aws = aws.eu
  }

  mode  = var.mode
  state = var.state
}

module "sa" {
  source = "./region"
  count = var.mode == "microbench" ? 0 : 1
  providers = {
    aws = aws.sa
  }

  mode  = var.mode
  state = var.state
}

module "af" {
  source = "./region"
  count = var.mode == "microbench" ? 0 : 1
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
    regions = var.mode == "microbench" ? {} : {
      ap = module.ap[0].instances
      us = module.us[0].instances
      eu = module.eu[0].instances
      sa = module.sa[0].instances
      af = module.af[0].instances
    }
  }
}
