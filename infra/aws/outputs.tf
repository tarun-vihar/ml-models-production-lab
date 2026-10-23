output "msk_iam_bootstrap_brokers" {
  description = "Use as KAFKA_BOOTSTRAP_SERVERS and SPRING_KAFKA_BOOTSTRAP_SERVERS."
  value       = aws_msk_serverless_cluster.events.bootstrap_brokers_sasl_iam
}

output "postgres_host" {
  value = aws_db_instance.postgres.address
}
output "postgres_jdbc_url" {
  value = "jdbc:postgresql://${aws_db_instance.postgres.address}:5432/fraud?sslmode=verify-full"
}
output "postgres_master_secret_arn" {
  description = "RDS-managed master credentials. Prefer creating a restricted application DB user before production."
  value       = aws_db_instance.postgres.master_user_secret[0].secret_arn
}

output "redis_host" {
  value = aws_elasticache_replication_group.redis.primary_endpoint_address
}
output "redis_port" {
  value = aws_elasticache_replication_group.redis.port
}
output "redis_auth_secret_arn" {
  description = "Secret JSON has username and password keys; inject these as ECS secrets."
  value       = aws_secretsmanager_secret.redis_auth.arn
}

output "neptune_endpoint" {
  value = "https://${aws_neptune_cluster.graph.endpoint}:8182"
}
output "neptune_cluster_resource_id" {
  value = aws_neptune_cluster.graph.cluster_resource_id
}

output "application_security_group_id" {
  description = "Attach this SG to ECS service tasks that connect to these data services."
  value       = aws_security_group.application.id
}
output "vpc_id" { value = local.network_vpc_id }
output "private_subnet_ids" { value = local.network_subnet_ids }
output "layer1_task_role_arn" { value = aws_iam_role.layer1.arn }
output "layer2_task_role_arn" { value = aws_iam_role.layer2.arn }
output "layer3_task_role_arn" { value = aws_iam_role.layer3.arn }
output "task_execution_role_arn" {
  description = "Attach to ECS task definitions; retrieves Layer 1 secrets and supports image pulls/log delivery."
  value       = aws_iam_role.execution.arn
}
output "aws_region" { value = var.aws_region }
