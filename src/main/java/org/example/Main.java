package org.example;

public class Main {
    public static void main(String[] args){
        for(int i=1;i<=10;i++){
            WeatherStation weatherStation=new WeatherStation(i);
            Thread thread=new Thread(weatherStation);
            thread.start();
        }
    }
}