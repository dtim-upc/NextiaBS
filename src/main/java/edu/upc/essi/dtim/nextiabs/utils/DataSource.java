package edu.upc.essi.dtim.nextiabs.utils;

import edu.upc.essi.dtim.nextiabs.metamodels.JSON_MM;
import edu.upc.essi.dtim.nextiabs.vocabulary.DataSourceVocabulary;
import lombok.Getter;
import lombok.Setter;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public abstract class DataSource implements IDataSource {
    //used for the RDFS graph
    public Graph G_target;
    public String wrapper;

    public String id;
    public String name;
//    dataSourceName
    public Map<String, String> prefixes;

    public DataSource(){
        G_target = new Graph();
        id = "";
        prefixes = new HashMap<>();
        setPrefixes();
    }

    public String createIRI(String name){
        if(id.equals("")){
            return DataSourceVocabulary.Schema.val() + name;
        }
        return DataSourceVocabulary.Schema.val() + id+"/"+ name;
    }

    public void setPrefixes(){

        if(id.equals(""))
            prefixes.put("nextiaSchema", DataSourceVocabulary.Schema.val());
        else
            prefixes.put("nextiaSchema", DataSourceVocabulary.Schema.val()+id+"/");
        prefixes.put("nextiaDataSource", DataSourceVocabulary.DataSource.val() +"/");
        prefixes.put("rdf", RDF.getURI());
        prefixes.put("rdfs", RDFS.getURI());
        prefixes.put("xsd", XSD.getURI());
        prefixes.put("json", JSON_MM.getURI());
    }
}
