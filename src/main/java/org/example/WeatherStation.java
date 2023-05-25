package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WeatherStation implements Runnable{
    private static final String TOPIC = "weather_data";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private final int stationID;

    public WeatherStation(int stationID) {
        this.stationID = stationID;
    }

    public static void main(String[] args) {
        // Set up the producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a new Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create weather station mock

        WeatherStationMock station = args.length>0?new WeatherStationMock(Integer.parseInt(args[0])):new WeatherStationMock(1);
        

        while (true) {
            // Create a weather status message
            String message = station.generateStatusMessage();
            
            // drop message
            if(message == null)
                continue;

            // Create a producer record with the message and send it to the Kafka topic
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
            producer.send(record);

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            System.out.println(message);
            
        }

        // Close the producer
        //producer.close();
        
    }
    @Override
    public void run() {
        main(new String[]{String.valueOf(this.stationID)});
    }
}
