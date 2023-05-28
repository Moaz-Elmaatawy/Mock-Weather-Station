package org.example;
import com.google.gson.Gson;
import java.util.Random;

public class WeatherStationMock {
    private final long stationId;
    private long messageCounter;
    private final Random random;
    private final Gson gson;
    
    public WeatherStationMock(long stationId) {
        this.stationId = stationId;
        this.messageCounter = 0;
        this.random = new Random();
        this.gson = new Gson();
    }
    
    public String generateStatusMessage() {
        // Increment message counter
        messageCounter++;
        
        // Generate weather data
        int humidity = random.nextInt(101);
        int temperature = random.nextInt(100) + 1; // Temperature is from 1 to 100 degrees Fahrenheit
        int windSpeed = random.nextInt(61); // Wind speed is from 0 to 60 km/h
        
        // Generate battery status
        String batteryStatus = "medium";
        int batteryStatusRoll = random.nextInt(10) + 1;
        if (batteryStatusRoll <= 3) {
            batteryStatus = "low";
        } else if (batteryStatusRoll >= 8) {
            batteryStatus = "high";
        }
        
        // Check if message should be dropped
        if (random.nextInt(10) == 0) { // 10% chance of dropping message
            return null;
        }
        
        // Construct status message
        long timestamp = System.currentTimeMillis();
        WeatherData weatherData = new WeatherData(humidity, temperature, windSpeed);
        WeatherStatusMessage statusMessage = new WeatherStatusMessage(stationId, messageCounter, batteryStatus, timestamp, weatherData);
        return gson.toJson(statusMessage);
    }
    
    public static class WeatherStatusMessage {
        private final long station_id;
        private final long s_no;
        private final String battery_status;
        private final long status_timestamp;
        private final WeatherData weather;
        
        public WeatherStatusMessage(long station_id, long s_no, String battery_status, long status_timestamp, WeatherData weather) {
            this.station_id = station_id;
            this.s_no = s_no;
            this.battery_status = battery_status;
            this.status_timestamp = status_timestamp;
            this.weather = weather;
        }
    }
    
    public static class WeatherData {
        private final int humidity;
        private final int temperature;
        private final int wind_speed;
        
        public WeatherData(int humidity, int temperature, int wind_speed) {
            this.humidity = humidity;
            this.temperature = temperature;
            this.wind_speed = wind_speed;
        }
    }
}
