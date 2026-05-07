FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN apk add --no-cache maven && mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /src/target/plain-java-*.jar /app/app.jar
# Override CMD in docker-compose for producer vs consumer.
CMD ["java", "-cp", "/app/app.jar", "com.example.kafka.SimpleProducer"]
