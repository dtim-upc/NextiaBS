package edu.upc.essi.dtim.nextiabs2.vocabulary;


public enum Formats {


    CSV("CSV"),
    JSON("JSON");

    private String element;

    Formats(String element) {
        this.element = element;
    }

    public String val() {
        return element;
    }



}