FROM ballerina/ballerina:2201.13.0 AS build
WORKDIR /app
COPY . .
RUN bal build

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/bin/delivery_service.jar ./delivery_service.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "delivery_service.jar"]