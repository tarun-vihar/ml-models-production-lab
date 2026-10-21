FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace
COPY pom.xml ./pom.xml
COPY fraud-detection/pom.xml ./fraud-detection/pom.xml
COPY fraud-detection/src ./fraud-detection/src
RUN mvn -B -q -f fraud-detection/pom.xml package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /workspace/fraud-detection/target/fraud-ingestion-0.1.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
