FROM openjdk:17-jdk-slim-buster
WORKDIR /app

COPY target/kafka-dlq-0.0.1-SNAPSHOT.jar build/app.jar
COPY src/main/resources/client-certs /build/client-certs

WORKDIR /app/build

ENTRYPOINT java -jar app.jar