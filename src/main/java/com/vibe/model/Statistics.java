package com.vibe.model;

/**
 * Created by santiagoarce on 9/17/18.
 */
public class Statistics {
    Flights flights;
    String delays;
    String minutesDelayed;

    public Statistics(Flights flights, String delays, String minutesDelayed) {
        this.flights = flights;
        this.delays = delays;
        this.minutesDelayed = minutesDelayed;
    }
}
