FROM eclipse-temurin:17-jdk-alpine AS build
WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN apk add --no-cache maven && mvn -q -B package -DskipTests

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /src/target/spring-kafka-*.jar /app/app.jar
EXPOSE 8080
ENV JAVA_OPTS=""
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
