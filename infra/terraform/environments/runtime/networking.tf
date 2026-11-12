resource "aws_vpc" "runtime" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "blockchain-runtime-vpc"
  }
}

resource "aws_internet_gateway" "runtime" {
  vpc_id = aws_vpc.runtime.id

  tags = {
    Name = "blockchain-runtime-igw"
  }
}

resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.runtime.id
  cidr_block              = var.public_subnet_cidr
  availability_zone       = var.availability_zone
  map_public_ip_on_launch = true

  tags = {
    Name = "blockchain-runtime-public"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.runtime.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.runtime.id
  }

  tags = {
    Name = "blockchain-runtime-public"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "runtime" {
  name        = "blockchain-runtime"
  description = "No ingress; restricted egress for the EC2 container runtime"
  vpc_id      = aws_vpc.runtime.id

  tags = {
    Name = "blockchain-runtime"
  }
}

resource "aws_vpc_security_group_egress_rule" "https" {
  security_group_id = aws_security_group.runtime.id
  description       = "TLS for blockchain RPC, Snowflake, AWS APIs, ECR, and package retrieval"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "dns_udp" {
  security_group_id = aws_security_group.runtime.id
  description       = "DNS through the VPC resolver"
  ip_protocol       = "udp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = var.vpc_cidr
}

resource "aws_vpc_security_group_egress_rule" "dns_tcp" {
  security_group_id = aws_security_group.runtime.id
  description       = "DNS fallback through the VPC resolver"
  ip_protocol       = "tcp"
  from_port         = 53
  to_port           = 53
  cidr_ipv4         = var.vpc_cidr
}

resource "aws_vpc_security_group_egress_rule" "time_sync" {
  security_group_id = aws_security_group.runtime.id
  description       = "Amazon Time Sync Service"
  ip_protocol       = "udp"
  from_port         = 123
  to_port           = 123
  cidr_ipv4         = "169.254.169.123/32"
}
