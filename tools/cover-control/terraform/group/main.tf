terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

variable "n" {
  type    = number
  default = 1
}

variable "type" {
  type    = string
  default = "t3.micro"
}

variable "state" {
  type = string
  validation {
    condition     = contains(["running", "stopped"], var.state)
    error_message = "state must be either running or stopped"
  }
}

variable "ami" {
  type    = string
  default = null
}

variable "key_name" {
  type    = string
  default = "Ephemeral"
}

variable "network" {
  type = object({
    subnet_id              = string,
    vpc_security_group_ids = list(string),
  })
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["099720109477"] # Canonical
}

locals {
  ami = coalesce(var.ami, data.aws_ami.ubuntu.id)
}

resource "aws_instance" "main" {
  count = var.n

  ami                    = local.ami
  instance_type          = var.type
  subnet_id              = var.network.subnet_id
  vpc_security_group_ids = var.network.vpc_security_group_ids
  key_name               = var.key_name

  enclave_options {
    enabled = true
  }
}

resource "aws_ec2_instance_state" "_1" {
  count = var.n

  instance_id = aws_instance.main[count.index].id
  state       = var.state
}

output "instances" {
  value = resource.aws_instance.main[*]
}
