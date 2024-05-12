terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
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

output "subnet_id" {
  value = resource.aws_subnet.main.id
}

output "vpc_security_group_ids" {
  value = [resource.aws_security_group.main.id]
}
