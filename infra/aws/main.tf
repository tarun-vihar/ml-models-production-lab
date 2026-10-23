locals {
  network_vpc_id = var.create_network ? aws_vpc.lab[0].id : var.vpc_id
  network_subnet_ids = var.create_network ? aws_subnet.private[*].id : var.private_subnet_ids
  kafka_topics = [
    "raw-transactions",
    "enriched-transactions",
    "dead-letter-transactions",
    "scored-transactions",
    "scoring-dead-letter-transactions",
    "graph-scored-transactions",
    "graph-dead-letter-transactions",
  ]
  kafka_groups = ["fraud-ingestion-v1", "fraud-scoring-v1", "fraud-graph-v1"]
  cluster_uuid = element(split("/", aws_msk_serverless_cluster.events.arn), 2)
  msk_topic_arns = { for topic in local.kafka_topics : topic => "arn:${data.aws_partition.current.partition}:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:topic/${var.name}-msk/${local.cluster_uuid}/${topic}" }
  msk_group_arns = { for group in local.kafka_groups : group => "arn:${data.aws_partition.current.partition}:kafka:${var.aws_region}:${data.aws_caller_identity.current.account_id}:group/${var.name}-msk/${local.cluster_uuid}/${group}" }
}

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_vpc" "lab" {
  count                = var.create_network ? 1 : 0
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "${var.name}-vpc" }
  lifecycle {
    precondition {
      condition     = length(data.aws_availability_zones.available.names) >= length(var.private_subnet_cidrs)
      error_message = "The selected region must have at least as many available AZs as private subnet CIDRs."
    }
  }
}

resource "aws_subnet" "private" {
  count                   = var.create_network ? length(var.private_subnet_cidrs) : 0
  vpc_id                  = aws_vpc.lab[0].id
  cidr_block              = var.private_subnet_cidrs[count.index]
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = false
  tags = {
    Name = "${var.name}-private-${count.index + 1}"
    Tier = "private"
  }
}

resource "aws_route_table" "private" {
  count  = var.create_network ? 1 : 0
  vpc_id = aws_vpc.lab[0].id
  tags   = { Name = "${var.name}-private" }
}

resource "aws_route_table_association" "private" {
  count          = var.create_network ? length(var.private_subnet_cidrs) : 0
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private[0].id
}

resource "aws_security_group" "application" {
  name        = "${var.name}-application"
  description = "Attach to application tasks that connect to the fraud data services."
  vpc_id      = local.network_vpc_id

  egress {
    description = "Allow application clients to reach dependencies and AWS APIs."
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  lifecycle {
    precondition {
      condition     = var.create_network || (length(var.vpc_id) > 0 && length(var.private_subnet_ids) >= 2)
      error_message = "Set create_network=true or provide an existing VPC ID and at least two private subnet IDs."
    }
  }
}

resource "aws_security_group" "msk" {
  name        = "${var.name}-msk"
  description = "Private IAM-authenticated Kafka access from application tasks."
  vpc_id      = local.network_vpc_id
  ingress {
    description     = "MSK IAM TLS listener from application tasks"
    from_port       = 9098
    to_port         = 9098
    protocol        = "tcp"
    security_groups = [aws_security_group.application.id]
  }
}

resource "aws_security_group" "postgres" {
  name        = "${var.name}-postgres"
  description = "Private PostgreSQL access from application tasks."
  vpc_id      = local.network_vpc_id
  ingress {
    description     = "PostgreSQL from application tasks"
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [aws_security_group.application.id]
  }
}

resource "aws_security_group" "redis" {
  name        = "${var.name}-redis"
  description = "Private TLS Redis access from application tasks."
  vpc_id      = local.network_vpc_id
  ingress {
    description     = "Redis TLS from application tasks"
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [aws_security_group.application.id]
  }
}

resource "aws_security_group" "neptune" {
  name        = "${var.name}-neptune"
  description = "Private Neptune access from application tasks."
  vpc_id      = local.network_vpc_id
  ingress {
    description     = "Neptune HTTPS from application tasks"
    from_port       = 8182
    to_port         = 8182
    protocol        = "tcp"
    security_groups = [aws_security_group.application.id]
  }
}

resource "aws_msk_serverless_cluster" "events" {
  cluster_name = "${var.name}-msk"
  client_authentication {
    sasl {
      iam { enabled = true }
    }
  }
  vpc_config {
    subnet_ids         = local.network_subnet_ids
    security_group_ids = [aws_security_group.msk.id]
  }
}

resource "aws_db_subnet_group" "postgres" {
  name       = "${var.name}-postgres"
  subnet_ids = local.network_subnet_ids
}

resource "aws_db_parameter_group" "postgres" {
  name   = "${var.name}-postgres17"
  family = "postgres17"
  parameter {
    name         = "rds.force_ssl"
    value        = "1"
    apply_method = "pending-reboot"
  }
}

resource "aws_db_instance" "postgres" {
  identifier                  = "${var.name}-postgres"
  engine                      = "postgres"
  engine_version              = "17"
  instance_class              = var.postgres_instance_class
  db_name                     = "fraud"
  username                    = "fraudadmin"
  manage_master_user_password = true
  port                        = 5432
  allocated_storage           = 20
  max_allocated_storage       = 100
  storage_type                = "gp3"
  storage_encrypted           = true
  db_subnet_group_name        = aws_db_subnet_group.postgres.name
  vpc_security_group_ids      = [aws_security_group.postgres.id]
  parameter_group_name        = aws_db_parameter_group.postgres.name
  publicly_accessible         = false
  multi_az                    = var.postgres_multi_az
  backup_retention_period     = 7
  auto_minor_version_upgrade  = true
  deletion_protection         = var.deletion_protection
  skip_final_snapshot         = var.postgres_skip_final_snapshot
  final_snapshot_identifier   = var.postgres_skip_final_snapshot ? null : var.postgres_final_snapshot_identifier
  copy_tags_to_snapshot       = true
  lifecycle {
    precondition {
      condition     = var.postgres_skip_final_snapshot || (var.postgres_final_snapshot_identifier != null && length(var.postgres_final_snapshot_identifier) > 0)
      error_message = "Set postgres_final_snapshot_identifier when postgres_skip_final_snapshot is false."
    }
  }
}

resource "random_password" "redis_auth" {
  length  = 40
  special = false
}

resource "aws_secretsmanager_secret" "redis_auth" {
  name                    = "${var.name}/redis/auth"
  description             = "ElastiCache AUTH token for ${var.name}; retrieve through the Layer 1 task role."
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "redis_auth" {
  secret_id     = aws_secretsmanager_secret.redis_auth.id
  secret_string = jsonencode({ username = "default", password = random_password.redis_auth.result })
}

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${var.name}-redis"
  subnet_ids = local.network_subnet_ids
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id       = "${var.name}-redis"
  description                = "Account transaction aggregates for ${var.name}"
  engine                     = "redis"
  engine_version             = "7.1"
  node_type                  = var.redis_node_type
  parameter_group_name       = "default.redis7"
  port                       = 6379
  num_cache_clusters         = var.redis_num_cache_clusters
  automatic_failover_enabled = var.redis_num_cache_clusters > 1
  multi_az_enabled           = var.redis_num_cache_clusters > 1
  subnet_group_name          = aws_elasticache_subnet_group.redis.name
  security_group_ids         = [aws_security_group.redis.id]
  transit_encryption_enabled = true
  at_rest_encryption_enabled = true
  auth_token                 = random_password.redis_auth.result
  auth_token_update_strategy = "SET"
  apply_immediately          = false
}

resource "aws_neptune_subnet_group" "graph" {
  name       = "${var.name}-neptune"
  subnet_ids = local.network_subnet_ids
}

resource "aws_neptune_cluster" "graph" {
  cluster_identifier                   = "${var.name}-neptune"
  engine                               = "neptune"
  neptune_subnet_group_name             = aws_neptune_subnet_group.graph.name
  vpc_security_group_ids                = [aws_security_group.neptune.id]
  iam_database_authentication_enabled   = true
  storage_encrypted                    = true
  backup_retention_period              = 7
  deletion_protection                  = var.deletion_protection
  skip_final_snapshot                  = var.neptune_skip_final_snapshot
  final_snapshot_identifier           = var.neptune_skip_final_snapshot ? null : var.neptune_final_snapshot_identifier
  serverless_v2_scaling_configuration {
    min_capacity = var.neptune_min_capacity
    max_capacity = var.neptune_max_capacity
  }
  lifecycle {
    precondition {
      condition     = var.neptune_skip_final_snapshot || (var.neptune_final_snapshot_identifier != null && length(var.neptune_final_snapshot_identifier) > 0)
      error_message = "Set neptune_final_snapshot_identifier when neptune_skip_final_snapshot is false."
    }
    precondition {
      condition     = var.neptune_min_capacity >= 1 && var.neptune_max_capacity >= var.neptune_min_capacity && var.neptune_max_capacity <= 128
      error_message = "Neptune Serverless capacity must be at least 1 NCU and max must be >= min and <= 128."
    }
  }
}

resource "aws_neptune_cluster_instance" "graph" {
  identifier         = "${var.name}-neptune-1"
  cluster_identifier = aws_neptune_cluster.graph.id
  instance_class     = "db.serverless"
  neptune_subnet_group_name = aws_neptune_subnet_group.graph.name
  apply_immediately  = false
}

data "aws_iam_policy_document" "ecs_tasks_trust" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "layer1_msk" {
  statement {
    sid       = "ConnectToCluster"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:WriteDataIdempotently"]
    resources = [aws_msk_serverless_cluster.events.arn]
  }
  statement {
    sid       = "CreateAndDescribeTopics"
    actions   = ["kafka-cluster:CreateTopic", "kafka-cluster:DescribeTopic"]
    resources = [for topic in ["raw-transactions", "enriched-transactions", "dead-letter-transactions"] : local.msk_topic_arns[topic]]
  }
  statement {
    sid       = "ConsumeRawTransactions"
    actions   = ["kafka-cluster:ReadData"]
    resources = [local.msk_topic_arns["raw-transactions"]]
  }
  statement {
    sid       = "PublishEnrichedAndDeadLetters"
    actions   = ["kafka-cluster:WriteData"]
    resources = [local.msk_topic_arns["enriched-transactions"], local.msk_topic_arns["dead-letter-transactions"]]
  }
  statement {
    sid       = "UseIngestionConsumerGroup"
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = [local.msk_group_arns["fraud-ingestion-v1"]]
  }
}

data "aws_iam_policy_document" "layer2_msk" {
  statement {
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:WriteDataIdempotently"]
    resources = [aws_msk_serverless_cluster.events.arn]
  }
  statement {
    actions   = ["kafka-cluster:CreateTopic", "kafka-cluster:DescribeTopic"]
    resources = [for topic in ["scored-transactions", "scoring-dead-letter-transactions"] : local.msk_topic_arns[topic]]
  }
  statement {
    actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"]
    resources = [local.msk_topic_arns["enriched-transactions"]]
  }
  statement {
    actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:WriteData"]
    resources = [local.msk_topic_arns["scored-transactions"], local.msk_topic_arns["scoring-dead-letter-transactions"]]
  }
  statement {
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = [local.msk_group_arns["fraud-scoring-v1"]]
  }
}

data "aws_iam_policy_document" "layer3_msk" {
  statement {
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "kafka-cluster:WriteDataIdempotently"]
    resources = [aws_msk_serverless_cluster.events.arn]
  }
  statement {
    actions   = ["kafka-cluster:CreateTopic", "kafka-cluster:DescribeTopic"]
    resources = [for topic in ["graph-scored-transactions", "graph-dead-letter-transactions"] : local.msk_topic_arns[topic]]
  }
  statement {
    actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"]
    resources = [local.msk_topic_arns["scored-transactions"]]
  }
  statement {
    actions   = ["kafka-cluster:DescribeTopic", "kafka-cluster:WriteData"]
    resources = [local.msk_topic_arns["graph-scored-transactions"], local.msk_topic_arns["graph-dead-letter-transactions"]]
  }
  statement {
    actions   = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]
    resources = [local.msk_group_arns["fraud-graph-v1"]]
  }
}

resource "aws_iam_role" "layer1" {
  name               = "${var.name}-layer1-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_trust.json
}
resource "aws_iam_role" "layer2" {
  name               = "${var.name}-layer2-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_trust.json
}
resource "aws_iam_role" "layer3" {
  name               = "${var.name}-layer3-task"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_trust.json
}
resource "aws_iam_role" "execution" {
  name               = "${var.name}-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_tasks_trust.json
}
resource "aws_iam_role_policy_attachment" "execution_baseline" {
  role       = aws_iam_role.execution.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role_policy" "layer1_msk" {
  name   = "msk-access"
  role   = aws_iam_role.layer1.id
  policy = data.aws_iam_policy_document.layer1_msk.json
}
resource "aws_iam_role_policy" "layer2_msk" {
  name   = "msk-access"
  role   = aws_iam_role.layer2.id
  policy = data.aws_iam_policy_document.layer2_msk.json
}
resource "aws_iam_role_policy" "layer3_msk" {
  name   = "msk-access"
  role   = aws_iam_role.layer3.id
  policy = data.aws_iam_policy_document.layer3_msk.json
}

data "aws_iam_policy_document" "layer1_secrets" {
  statement {
    actions   = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
    resources = [aws_db_instance.postgres.master_user_secret[0].secret_arn, aws_secretsmanager_secret.redis_auth.arn]
  }
}
resource "aws_iam_role_policy" "execution_secrets" {
  name   = "layer1-database-and-cache-secrets"
  role   = aws_iam_role.execution.id
  policy = data.aws_iam_policy_document.layer1_secrets.json
}

data "aws_iam_policy_document" "layer3_neptune" {
  statement {
    actions = [
      "neptune-db:ReadDataViaQuery", "neptune-db:WriteDataViaQuery", "neptune-db:DeleteDataViaQuery"
    ]
    resources = ["arn:${data.aws_partition.current.partition}:neptune-db:${var.aws_region}:${data.aws_caller_identity.current.account_id}:${aws_neptune_cluster.graph.cluster_resource_id}/*"]
    condition {
      test     = "StringEquals"
      variable = "neptune-db:QueryLanguage"
      values   = ["OpenCypher"]
    }
  }
}
resource "aws_iam_role_policy" "layer3_neptune" {
  name   = "neptune-opencypher-query"
  role   = aws_iam_role.layer3.id
  policy = data.aws_iam_policy_document.layer3_neptune.json
}
