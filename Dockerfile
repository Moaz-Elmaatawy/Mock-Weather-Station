FROM maven:3.6.0-jdk-11-slim

WORKDIR /Weather-Station

COPY src/ /Weather-Station/src/
COPY pom.xml .

RUN mvn clean package

ENTRYPOINT [ "mvn", "exec:java","-Dexec.mainClass=org.example.WeatherStation"]

#docker run --network container:kafka0 -it mock-station6