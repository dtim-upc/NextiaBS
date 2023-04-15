package edu.upc.essi.dtim.nextiabs.metamodels;

import org.apache.jena.graph.* ;
import org.apache.jena.rdf.model.* ;

public class DataFrame_MM {
    /**
     * The namespace of the vocabulary as a string
     */
    public static final String uri="https://www.essi.upc.edu/dtim/dataframe-metamodel#";

    protected static final Resource resource( String local )
    { return ResourceFactory.createResource( uri + local ); }

    protected static final Property property( String local )
    { return ResourceFactory.createProperty( uri, local ); }

    public static final Resource DataSource = Init.DataSource();
    public static final Resource DataFrame = Init.DataFrame();
    public static final Resource Data = Init.Data();
    public static final Resource DataType = Init.DataType();
    public static final Resource Array = Init.Array();
    public static final Resource Primitive = Init.Primitive();
    public static final Resource String = Init.String();
    public static final Resource Number = Init.Number();


    public static final Property hasData     = Init.hasData();
    public static final Property hasDataType     = Init.hasDataType();




    public static class Init {
        public static Resource DataSource() { return resource("DataSource"); }
        public static Resource DataFrame() { return resource("DataFrame"); } //or object
        public static Resource Data() { return resource("Data"); }
        public static Resource DataType() { return resource("DataType"); }
        public static Resource Array() { return resource("Array"); }
        public static Resource Primitive() { return resource("Primitive"); }
        //NOT object <-- Strings, Arrays and Objects. DataFrames are also Python::Objects
        //int64, float64, datetime64, bool
        public static Resource String() { return resource("String"); }
        public static Resource Number() { return resource("Number"); }

        public static Property hasData() { return property( "hasData"); }

        public static Property hasDataType() { return property( "hasDataType"); }
    }

    /**
     returns the URI for this schema
     @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }
}
