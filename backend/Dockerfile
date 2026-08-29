FROM gradle:8.14-jdk21 AS build

WORKDIR /workspace
COPY . .
RUN gradle bootJar --no-daemon

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /workspace/build/libs/*.jar app.jar
EXPOSE 18090
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
