FROM ballerina/ballerina:2201.13.5 AS build
WORKDIR /home/ballerina/app
COPY --chown=ballerina:ballerina . .
RUN bal build

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /home/ballerina/app/target/bin/restaurant_service.jar .
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "restaurant_service.jar"]
