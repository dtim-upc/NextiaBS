package edu.upc.essi.dtim.nextiabs2.temp;

//import edu.upc.essi.dtim.Graph.URI;

public class RDF {
    public static final String uri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    /** returns the URI for this schema
     @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static String resource(String local)
    { return new String( uri + local ); }

    public static final String type = Init.type();

    public static final String Property = Init.Property();


    public static class Init {
        public static String type() {return resource("Type");}
        public static String Property() {return resource ("Property");}
    }

}