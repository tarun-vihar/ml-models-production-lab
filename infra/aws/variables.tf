variable "aws_region" {
  description = "AWS region containing the VPC and all services."
  type        = string
}

variable "name" {
  description = "Short project prefix used in AWS resource names."
  type        = string
  default     = "fraud-lab"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,19}$", var.name))
    error_message = "name must be 3-20 lowercase letters, numbers, or hyphens and start with a letter."
  }
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "vpc_id" {
  description = "Existing VPC ID when create_network is false."
  type        = string
  default     = ""
}

variable "private_subnet_ids" {
  description = "At least two private subnet IDs in distinct AZs when create_network is false."
  type        = list(string)
  default     = []
}

variable "create_network" {
  description = "Create a new isolated VPC and private subnets instead of using an existing VPC."
  type        = bool
  default     = true
}

variable "vpc_cidr" {
  description = "CIDR block for the lab VPC when create_network is true."
  type        = string
  default     = "10.40.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDRs in separate AZs when create_network is true."
  type        = list(string)
  default     = ["10.40.0.0/20", "10.40.16.0/20"]
  validation {
    condition     = length(var.private_subnet_cidrs) >= 2
    error_message = "Provide at least two private subnet CIDRs in distinct AZs."
  }
}

variable "postgres_instance_class" {
  type        = string
  description = "RDS PostgreSQL instance class. Review regional availability and cost before apply."
  default     = "db.t4g.micro"
}

variable "postgres_multi_az" {
  type        = bool
  description = "Enable RDS Multi-AZ for higher availability (higher cost)."
  default     = false
}

variable "redis_node_type" {
  type        = string
  description = "ElastiCache Redis node type. Review regional availability and cost before apply."
  default     = "cache.t4g.micro"
}

variable "redis_num_cache_clusters" {
  type        = number
  description = "Use 1 for a low-cost lab or 2+ for automatic failover."
  default     = 1
  validation {
    condition     = var.redis_num_cache_clusters >= 1
    error_message = "redis_num_cache_clusters must be at least 1."
  }
}

variable "neptune_min_capacity" {
  type        = number
  description = "Minimum Neptune Serverless capacity in NCUs."
  default     = 1.0
}

variable "neptune_max_capacity" {
  type        = number
  description = "Maximum Neptune Serverless capacity in NCUs."
  default     = 2.0
}

variable "deletion_protection" {
  type        = bool
  description = "Protect RDS and Neptune from accidental deletion."
  default     = false
}

variable "postgres_skip_final_snapshot" {
  description = "Set false to require a final snapshot on RDS deletion."
  type        = bool
  default     = true
}

variable "postgres_final_snapshot_identifier" {
  description = "Required when postgres_skip_final_snapshot is false."
  type        = string
  default     = null
}

variable "neptune_skip_final_snapshot" {
  description = "Set false to require a final snapshot on Neptune deletion."
  type        = bool
  default     = true
}

variable "neptune_final_snapshot_identifier" {
  description = "Required when neptune_skip_final_snapshot is false."
  type        = string
  default     = null
}
