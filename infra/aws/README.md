# AWS data services for the fraud lab

Terraform provisions an isolated VPC with private subnets, private Amazon MSK Serverless with IAM auth, RDS for PostgreSQL, TLS and AUTH-protected ElastiCache for Redis OSS, and IAM-authenticated Neptune Serverless with openCypher access. It also creates service security groups, separate ECS task roles, and Secrets Manager references for database/cache credentials. It does not create an application load balancer or ECS cluster/services.

## Network prerequisites

By default, the template creates a new VPC and two private subnets in distinct Availability Zones, with no internet gateway or NAT gateway. That keeps the databases and broker private and avoids NAT gateway charges. You can instead set `create_network=false` and supply `vpc_id` and `private_subnet_ids` for an existing VPC. Adjust the default `10.40.0.0/16` CIDR if it conflicts with networks you may connect later.

The services have no public endpoints. A developer's AWS profile grants identity credentials; it does not create a network route from a laptop into the VPC. To connect from your laptop, you need VPN/Direct Connect or a controlled in-VPC access host. ECS tasks added later will need network access to ECR, CloudWatch Logs, and Secrets Manager, via NAT or appropriate VPC endpoints. Do not open data service security groups to `0.0.0.0/0`.

The Terraform creates resources that incur AWS charges. It does not run `apply`; review the cost and plan first. Defaults are for a low-traffic learning environment, not a highly available production deployment. MSK Serverless and Neptune Serverless still incur usage charges; RDS and Redis have provisioned instance costs.

## Initialize and review

Install Terraform 1.10+ and AWS CLI. Configure a profile and region, then log in if the profile uses IAM Identity Center:

```sh
aws sso login --profile your-profile
aws sts get-caller-identity --profile your-profile
```

Create or select a private S3 state bucket with versioning and encryption before using this template. In the AWS console, create a general-purpose bucket with Block Public Access enabled, versioning enabled, and default encryption enabled. State contains sensitive provider values, including the Redis AUTH token, and must be tightly access controlled. Never commit state, `.terraform/`, or populated `terraform.tfvars`.

```sh
cd infra/aws
cp terraform.tfvars.example terraform.tfvars
# Edit the region, project name, and CIDRs if needed. The default creates a private VPC.
AWS_PROFILE=your-profile terraform init \
  -backend-config="bucket=<terraform-state-bucket>" \
  -backend-config="key=fraud-lab/dev/terraform.tfstate" \
  -backend-config="region=<state-bucket-region>" \
  -backend-config="encrypt=true" \
  -backend-config="use_lockfile=true"
AWS_PROFILE=your-profile terraform plan -out=fraud-lab.plan
```

Inspect the plan and its estimated cost. Only after review, an operator can create the resources with `terraform apply fraud-lab.plan`. This repository task has not applied any AWS changes. Remove the resources when finished with a reviewed `terraform destroy`; deletion protection must be disabled and RDS/Neptune final snapshots considered first.

## Connect the applications

Read outputs after apply and set task configuration from them. Do not commit output containing secrets (the outputs contain endpoints and secret ARNs, not password values):

```sh
terraform output
```

Layer 1 task configuration:

```text
SPRING_PROFILES_ACTIVE=aws
SPRING_KAFKA_BOOTSTRAP_SERVERS=<msk_iam_bootstrap_brokers>
SPRING_DATASOURCE_URL=<postgres_jdbc_url>
SPRING_DATASOURCE_USERNAME=fraudadmin
SPRING_DATASOURCE_PASSWORD=<inject RDS master secret JSON key password from Secrets Manager>
SPRING_DATA_REDIS_HOST=<redis_host>
SPRING_DATA_REDIS_PORT=<redis_port>
SPRING_DATA_REDIS_USERNAME=default
SPRING_DATA_REDIS_PASSWORD=<inject Redis secret JSON key password from Secrets Manager>
FRAUD_TOPICS_REPLICATION_FACTOR=3
```

The Redis secret JSON includes `username=default` and the generated password; ECS injects the credential values using the task execution role. For production, create a restricted PostgreSQL application role and secret instead of connecting as the RDS master user.

RDS rotates its managed master secret independently. ECS injects secret values when a task starts, so redeploy Layer 1 after secret rotation. Redis AUTH rotation is manual in this template; update the Redis replication group and Secrets Manager secret together, then roll the Layer 1 tasks.

Layer 2 task configuration:

```text
KAFKA_BOOTSTRAP_SERVERS=<msk_iam_bootstrap_brokers>
MSK_AUTH_MODE=iam
AWS_REGION=<aws_region>
```

Layer 3 task configuration:

```text
KAFKA_BOOTSTRAP_SERVERS=<msk_iam_bootstrap_brokers>
MSK_AUTH_MODE=iam
AWS_REGION=<aws_region>
GRAPH_MODE=aws
NEPTUNE_ENDPOINT=<neptune_endpoint>
NEPTUNE_AUTH_MODE=iam
```

Attach the matching `layer1_task_role_arn`, `layer2_task_role_arn`, or `layer3_task_role_arn` as each ECS task role. Attach `task_execution_role_arn` as the ECS task execution role; it can pull images, send logs, and retrieve the Layer 1 database/cache secrets that ECS injects into the task. Set `SPRING_DATA_REDIS_USERNAME=default` for the current Java Redis client. For production, create a restricted PostgreSQL application role and secret instead of connecting as the RDS master user.

For local host runs, `AWS_PROFILE` supplies AWS credentials for MSK IAM and Neptune SigV4, but private network reachability is still required. Do not put AWS access keys in `.env` or container images.

## AWS service choices and limits

- MSK is Serverless and IAM-only. The task policies cover this app's known topic and consumer-group names; topic creation by Spring uses replication factor 3. MSK Serverless itself applies AWS-managed Kafka capacity and topic constraints.
- RDS PostgreSQL uses private networking, encryption, SSL enforcement, automated backups, and an AWS-managed master secret. The low-cost default is single-AZ.
- ElastiCache uses a single Redis OSS 7 node by default, TLS in transit, encryption at rest, and a generated AUTH token stored in Secrets Manager. Set `redis_num_cache_clusters=2` or more to turn on failover; the Java service's current config expects password AUTH, not ElastiCache IAM tokens.
- Neptune uses Serverless capacity with IAM database auth enabled. The Layer 3 task role is scoped to the Neptune data resource and openCypher query language.
- Task roles are for ECS. A local Docker container does not automatically inherit the VS Code AWS profile; the profile must be made available to the container securely, and private services still require a private route.

This stack has not been applied or validated against a live account. AWS service availability, quotas, subnet AZ placement, DB instance class support, and pricing vary by region/account; review the plan and AWS console before applying.
