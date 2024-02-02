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
    condition     = var.mode == "one" || var.mode == "five"
    error_message = "Mode must be either 'one' or 'five'."
  }
}

variable "instance-count" {
  type    = number
  default = 3
}

module "group-1" {
  source = "./group"
  providers = {
    aws = aws.ap-southeast-1
  }

  instance-state = var.state
  instance-count = var.mode == "one" ? var.instance-count : 0
}

module "group-5-1" {
  source = "./group"
  providers = {
    aws = aws.ap-east-1
  }

  instance-count = var.mode == "five" ? var.instance-count : 0
}


module "group-5-2" {
  source = "./group"
  providers = {
    aws = aws.us-west-1
  }

  instance-count = var.mode == "five" ? var.instance-count : 0
}

module "group-5-3" {
  source = "./group"
  providers = {
    aws = aws.eu-central-1
  }

  instance-count = var.mode == "five" ? var.instance-count : 0
}

module "group-5-4" {
  source = "./group"
  providers = {
    aws = aws.sa-east-1
  }

  instance-count = var.mode == "five" ? var.instance-count : 0
}

module "group-5-5" {
  source = "./group"
  providers = {
    aws = aws.af-south-1
  }

  instance-count = var.mode == "five" ? var.instance-count : 0
}

output "instances" {
  value = concat(
    module.group-1.instances,
    module.group-5-1.instances,
    module.group-5-2.instances,
    module.group-5-3.instances,
    module.group-5-4.instances,
    module.group-5-5.instances,
  )
}
