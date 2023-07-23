package edu.upc.essi.dtim.nextiabs2.vocabulary;

public enum Namespaces {

    G("http://www.essi.upc.edu/DTIM/"),
    NextiaDI("http://www.essi.upc.edu/DTIM/NextiaDI/");


    private final String element;

    Namespaces(String element) {
        this.element = element;
    }

    public String getURI() {
        return element;
    }

}
