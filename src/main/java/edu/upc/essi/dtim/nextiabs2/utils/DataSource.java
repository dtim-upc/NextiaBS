package edu.upc.essi.dtim.nextiabs2.utils;

//import edu.upc.essi.dtim.nextiabs2.metamodels.DataFrame_MM;
//import edu.upc.essi.dtim.nextiabs2.metamodels.JSON_MM;
import edu.upc.essi.dtim.NextiaCore.graph.*;

import edu.upc.essi.dtim.nextiabs2.vocabulary.DataSourceVocabulary;
import lombok.Getter;
import lombok.Setter;


import edu.upc.essi.dtim.NextiaCore.vocabulary.*;

//import edu.upc.essi.dtim.nextiabs2.vocabulary.RDF;
//import edu.upc.essi.dtim.nextiabs2.vocabulary.RDFS;
//import edu.upc.essi.dtim.nextiabs2.vocabulary.XSD;

//import org.apache.jena.vocabulary.RDF;
//import org.apache.jena.vocabulary.RDFS;
//import org.apache.jena.vocabulary.XSD;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@Getter
@Setter
public abstract class DataSource implements IDataSource {
    //used for the RDFS graph
    public Graph G_target;
    public String wrapper;

    public String id;
    public String name;
    public String description;
//    dataSourceName
    public Map<String, String> prefixes;

    public DataSource(){
//        G_target = new LocalGraph(new URI(""),new HashSet<Triple>());
        G_target = CoreGraphFactory.createGraphInstance("normal");
        id = "";
        prefixes = new HashMap<>();
//        setPrefixes();
    }

    public String createIRI(String name){
        if(id.equals("")){
            return DataSourceVocabulary.Schema.getURI() + name;
        }
        return DataSourceVocabulary.Schema.getURI() + id+"/"+ name;
    }

//    public void setPrefixes(){
//
//        if(id.equals(""))
//            prefixes.put("nextiaSchema", DataSourceVocabulary.Schema.getURI());
//        else
//            prefixes.put("nextiaSchema", DataSourceVocabulary.Schema.getURI()+id+"/");
//        prefixes.put("nextiaDataSource", DataSourceVocabulary.DataSource.getURI() +"/");
//        prefixes.put("rdf", RDF.getUri());
//        prefixes.put("rdfs", RDFS.getURI());
//        prefixes.put("xsd", XSD.getURI());
//        prefixes.put("json", JSON_MM.getURI());
//        prefixes.put("dataFrame", DataFrame_MM.getURI());
//    }
}
