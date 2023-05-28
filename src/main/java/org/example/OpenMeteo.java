package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import org.example.WeatherStationMock.WeatherData;
import org.example.WeatherStationMock.WeatherStatusMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class OpenMeteo {
    private final String open_meteo_URL = "curl https://api.open-meteo.com/v1/forecast?latitude=31.2026646&longitude=29.9189541&current_weather=true&hourly=temperature_2m,relativehumidity_2m,windspeed_10m";
    private static final String TOPIC = "weather_data";
    private static final Long STATION_ID = (long) 0;

    public ArrayList<String> getWeather() throws IOException, ParseException {

        Process process = Runtime.getRuntime().exec(open_meteo_URL);

        InputStream inputStream = process.getInputStream();
        String response = new BufferedReader(new InputStreamReader(inputStream))
                .lines()
                .collect(Collectors.joining("\n"));
        
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode weatherDataResopnse = objectMapper.readTree(response);

        Iterator<JsonNode> time = weatherDataResopnse.get("hourly").get("time").elements();
        Iterator<JsonNode> temp = weatherDataResopnse.get("hourly").get("temperature_2m").elements();
        Iterator<JsonNode> humidity = weatherDataResopnse.get("hourly").get("relativehumidity_2m").elements();
        Iterator<JsonNode> wind = weatherDataResopnse.get("hourly").get("windspeed_10m").elements();
        int messageCounter = 0;

        ArrayList<String> messages = new ArrayList<>();
        Gson gson =  new Gson();
        
        while(time.hasNext()){
            WeatherData weatherData = new WeatherData(humidity.next().asInt(), temp.next().asInt(), wind.next().asInt());

            LocalDateTime dateTime = LocalDateTime.parse(time.next().asText());
            long timestamp = dateTime.toEpochSecond(ZoneOffset.UTC);

            WeatherStatusMessage statusMessage = new WeatherStatusMessage(STATION_ID, ++messageCounter, "meteo-api", timestamp, weatherData); 
            messages.add(gson.toJson(statusMessage));
        }   
        
        return messages;
    }

    public static void main(String[] args) throws IOException, ParseException {
        // Retrieve host and port from environment variables
        String kafkaHost = System.getenv("KAFKA_HOST");
        String kafkaPort = System.getenv("KAFKA_PORT");
        String hostname = System.getenv("HOSTNAME");

        System.out.println("KAFKA_HOST:" + kafkaHost + "\nKAFKA_PORT:" +kafkaPort + "\nHOSTNAME:" + hostname);
        
        if (kafkaHost == null) {
            System.err.println("KAFKA_HOST environment variable not set");
            System.exit(1);
        }

        if (kafkaPort == null) {
            System.err.println("KAFKA_PORT environment variable not set");
            System.exit(1);
        }

        if (hostname == null) {
            System.err.println("HOSTNAME environment variable not set");
            System.exit(1);
        }

        String BOOTSTRAP_SERVERS =  kafkaHost+':'+kafkaPort;
        // Set up the producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a new Kafka producer
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

        OpenMeteo openMeteo = new OpenMeteo();
        ArrayList <String> messages = openMeteo.getWeather();

        for(String message : messages) {
            // Create a producer record with the message and send it to the Kafka topic
            ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC,STATION_ID,message);
            producer.send(record);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(message);
            
        }
    }
}
