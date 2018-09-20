package com.javacodegeeks.examples.wordcount.model;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

/**
 * Created by santiagoarce on 9/17/18.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AirportObject {
    Airport airport;
    Statistics statistics;
    Time time;
    Carrier carrier;

    public AirportObject(Airport airport, Statistics statistics, Time time, Carrier carrier) {
        this.airport = airport;
        this.statistics = statistics;
        this.time = time;
        this.carrier = carrier;
    }

    public AirportObject() {
    }
}
