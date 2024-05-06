terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "state" {
  type = string
}

variable "mode" {
  type = string
  validation {
    condition     = contains(["mutex", "cops"], var.mode)
    error_message = "Unexpected mode."
  }
}

data "aws_ami" "al2023" {
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

module "network" {
  source = "../group_network"
}

module "mutex" {
  source = "../group"
  count  = var.mode == "mutex" ? 1 : 0

  network = module.network
  state   = var.state
  type    = "c5a.2xlarge"
  ami     = data.aws_ami.al2023.id
  n       = 20
}

module "cops" {
  source = "../group"
  count  = var.mode == "cops" ? 1 : 0

  network = module.network
  state   = var.state
  type    = "c5a.8xlarge"
  ami     = data.aws_ami.al2023.id
}

module "cops_client" {
  source = "../group"
  count  = var.mode == "cops" ? 1 : 0

  network = module.network
  state   = var.state
  type    = "c5a.2xlarge"
  ami     = data.aws_ami.al2023.id
}

module "quorum" {
  source = "../group"

  network = module.network
  state   = var.state
  type    = var.mode == "mutex" ? "c5a.8xlarge" : "c5a.2xlarge"
  ami     = data.aws_ami.al2023.id
  n       = 2
}

output "instances" {
  value = {
    mutex       = flatten(module.mutex[*].instances)
    cops        = flatten(module.cops[*].instances)
    cops_client = flatten(module.cops_client[*].instances)
    quorum      = module.quorum.instances
  }
}
