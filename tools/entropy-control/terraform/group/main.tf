terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "aws_vpc" "entropy" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "entropy" {
  vpc_id                  = resource.aws_vpc.entropy.id
  cidr_block              = "10.0.0.0/16"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "entropy" {
  vpc_id = resource.aws_vpc.entropy.id
}

resource "aws_route_table" "entropy" {
  vpc_id = resource.aws_vpc.entropy.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = resource.aws_internet_gateway.entropy.id
  }
}

resource "aws_route_table_association" "_1" {
  route_table_id = resource.aws_route_table.entropy.id
  subnet_id      = resource.aws_subnet.entropy.id
}

resource "aws_security_group" "entropy" {
  vpc_id = resource.aws_vpc.entropy.id

  ingress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

data "aws_ami" "debian" {
  most_recent = true

  filter {
    name   = "name"
    values = ["debian-12-amd64-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  owners = ["136693071363"]
}

variable "instance_type" {
  type    = string
  default = "m5.4xlarge"
}

variable "instance-count" {
  type    = number
  default = 3
}

resource "aws_instance" "entropy" {
  count = var.instance-count

  ami                    = data.aws_ami.debian.id
  instance_type          = var.instance_type
  subnet_id              = resource.aws_subnet.entropy.id
  vpc_security_group_ids = [resource.aws_security_group.entropy.id]
  key_name               = "Ephemeral"

  root_block_device {
    volume_size = 20
  }
}

variable "instance-state" {
  type    = string
  default = "running"
}

resource "aws_ec2_instance_state" "entropy" {
  count = var.instance-count

  instance_id = aws_instance.entropy[count.index].id
  state       = var.instance-state
}

output "instances" {
  value = resource.aws_instance.entropy[*]
}
