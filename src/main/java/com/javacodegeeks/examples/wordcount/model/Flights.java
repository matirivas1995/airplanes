package com.javacodegeeks.examples.wordcount.model;

public class Flights {
    Integer cancelled;
    Integer ontime;
    Integer total;
    Integer delayed;
    Integer diverted;

    public Flights(Integer cancelled, Integer ontime, Integer total, Integer delayed, Integer diverted) {
        this.cancelled = cancelled;
        this.ontime = ontime;
        this.total = total;
        this.delayed = delayed;
        this.diverted = diverted;
    }
}
