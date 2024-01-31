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

module "group-1" {
  source = "./group"
  providers = {
    aws = aws.ap-southeast-1
  }

  instance_state = var.state
  #   instance_count = 20
}

# module "group-2" {
#   source = "./group"
#   providers = {
#     aws = aws.us-west-1
#   }

#   instance_count = 20
# }

# module "group-3" {
#   source = "./group"
#   providers = {
#     aws = aws.eu-central-1
#   }

#   instance_count = 20
# }

# module "group-4" {
#   source = "./group"
#   providers = {
#     aws = aws.sa-east-1
#   }

#   instance_count = 20
# }

# module "group-5" {
#   source = "./group"
#   providers = {
#     aws = aws.af-south-1
#   }

#   instance_count = 20
# }

# output "hosts" {
#   value = concat(
#     module.group-1.instances,
#     #     module.group-2.instances,
#     #     module.group-3.instances,
#     #     module.group-4.instances,
#     #     module.group-5.instances,
#   )
# }
