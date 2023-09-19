package edu.upc.essi.dtim.nextiabs.vocabulary;


public enum Formats {


    CSV("CSV"),
    JSON("JSON"),

    SQL("SQL");

    private final String element;

    Formats(String element) {
        this.element = element;
    }

    public String val() {
        return element;
    }



}
