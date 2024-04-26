terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
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

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "main" {
  vpc_id                  = resource.aws_vpc.main.id
  cidr_block              = "10.0.0.0/16"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "main" {
  vpc_id = resource.aws_vpc.main.id
}

resource "aws_route_table" "main" {
  vpc_id = resource.aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = resource.aws_internet_gateway.main.id
  }
}

resource "aws_route_table_association" "_1" {
  route_table_id = resource.aws_route_table.main.id
  subnet_id      = resource.aws_subnet.main.id
}

resource "aws_security_group" "main" {
  vpc_id = resource.aws_vpc.main.id

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

  owners = ["137112412989"] # amazon
}

resource "aws_instance" "main" {
  count = var.instance_count

  # ami                    = data.aws_ami.ubuntu.id
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = resource.aws_subnet.main.id
  vpc_security_group_ids = [resource.aws_security_group.main.id]
  key_name               = "Ephemeral"

  # root_block_device {
  #   volume_size = 20
  # }
}

resource "aws_ec2_instance_state" "_1" {
  count = var.instance_count

  instance_id = aws_instance.main[count.index].id
  state       = var.instance_state
}

output "instances" {
  value = resource.aws_instance.main[*]
}
