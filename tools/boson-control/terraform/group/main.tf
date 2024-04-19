terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

resource "aws_vpc" "boson" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "boson" {
  vpc_id                  = resource.aws_vpc.boson.id
  cidr_block              = "10.0.0.0/16"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "boson" {
  vpc_id = resource.aws_vpc.boson.id
}

resource "aws_route_table" "boson" {
  vpc_id = resource.aws_vpc.boson.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = resource.aws_internet_gateway.boson.id
  }
}

resource "aws_route_table_association" "_1" {
  route_table_id = resource.aws_route_table.boson.id
  subnet_id      = resource.aws_subnet.boson.id
}

resource "aws_security_group" "boson" {
  vpc_id = resource.aws_vpc.boson.id

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

variable "instance-type" {
  type    = string
  default = "c5a.xlarge"
}

variable "instance-count" {
  type    = number
  default = 1
}

resource "aws_instance" "boson" {
  count = var.instance-count

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = var.instance-type
  subnet_id              = resource.aws_subnet.boson.id
  vpc_security_group_ids = [resource.aws_security_group.boson.id]
  key_name               = "Ephemeral"

  # root_block_device {
  #   volume_size = 20
  # }
}

variable "instance-state" {
  type    = string
  default = "running"
}

resource "aws_ec2_instance_state" "boson" {
  count = var.instance-count

  instance_id = aws_instance.boson[count.index].id
  state       = var.instance-state
}

output "instances" {
  value = resource.aws_instance.boson[*]
}
