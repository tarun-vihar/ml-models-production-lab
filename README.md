# ML Models Production Lab

Parent repository for production focused machine learning projects. Each project is kept as an independent application module, with its own source, dependencies, and run documentation.

## Projects

| Module | Purpose | Documentation |
| --- | --- | --- |
| `fraud-detection/` | Spring Boot service that ingests transaction events, enriches them, applies rules, and publishes results through Kafka | [Fraud Detection README](fraud-detection/README.md) |
| `fraud-scoring/` | Python service that scores enriched events and publishes model results to Kafka | [Fraud Scoring README](fraud-scoring/README.md) |

## Build the lab

The root Maven project is an aggregator. It builds all modules:

```sh
mvn package
```

Build or test only fraud detection (from the repository root):

```sh
mvn -pl fraud-detection -am package -DskipTests
mvn -pl fraud-detection -am test
```

`-am` also builds required modules. The artifact keeps its application name, `fraud-ingestion`.

## Run the fraud detection application

Docker Compose stays at the repository root and starts the app with Kafka, Redis, and PostgreSQL:

```sh
docker compose up --build -d
docker compose ps
```

Check readiness at <http://localhost:8080/actuator/health/readiness>. Follow logs with `docker compose logs -f app`. Stop the stack with `docker compose down`.

For manual event submission, database checks, IDE debugging notes, architecture, and current implementation limits, see the [fraud detection guide](fraud-detection/README.md).

The fraud scoring service consumes `enriched-transactions` and publishes to `scored-transactions`. Local Compose uses an explicitly labeled development-only scoring baseline until a versioned trained model and its feature manifest are provided. See the [Fraud Scoring guide](fraud-scoring/README.md) for contracts and verification commands.

## Adding another project

Create a new project directory with its own `pom.xml`, then add its directory name under `<modules>` in the root `pom.xml`. Keep project-specific source, Dockerfile, and documentation inside that module. Add or extend root-level Compose services only when the projects need shared local infrastructure.
