package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WeatherStation implements Runnable{
    private static final String TOPIC = "weather_data";
    private static long stationID;

    public WeatherStation(long stationID) {
        WeatherStation.stationID = stationID;
    }

    public static void main(String[] args) {
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

        //create weather station mock
        Random rand = new Random(hostname.hashCode());
        stationID = Math.abs(rand.nextLong());
        System.out.println("===="+ stationID);
        WeatherStationMock station = new WeatherStationMock(args.length >0 ?Long.parseLong(args[0]):1);
        

        while (true) {
            // Create a weather status message
            String message = station.generateStatusMessage();
            
            // drop message
            if(message == null)
                continue;

            // Create a producer record with the message and send it to the Kafka topic
            ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC,stationID,message);
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
        main(new String[]{String.valueOf(WeatherStation.stationID)});
    }
}
