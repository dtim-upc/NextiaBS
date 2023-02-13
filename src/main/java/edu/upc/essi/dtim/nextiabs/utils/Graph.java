package edu.upc.essi.dtim.nextiabs.utils;

import lombok.Getter;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdf.model.impl.PropertyImpl;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.update.UpdateAction;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class Graph {

    private Model model;

    public Graph(){
//        org.apache.jena.query.ARQ.init();
        JenaSystem.init();
        model = ModelFactory.createDefaultModel();
    }

    public void setModel(Model model){
        this.model.add(model);
    }

    public void add(String subject, String predicate, String object) {
        Resource r = model.createResource(subject);
        r.addProperty(model.createProperty(predicate), model.createResource(object));
    }

    public void addLiteral(String subject, String predicate, String literal) {
        Resource r = model.createResource(subject);
        r.addProperty(model.createProperty(predicate), literal);
    }

    public void addLiteral(String subject, Property predicate, String literal) {
        Resource r = model.createResource(subject);
        r.addProperty(predicate, literal);
    }
    public void addLiteral(String subject, Property predicate, Literal literal) {
        Resource r = model.createResource(subject);
        r.addProperty(predicate, literal);
    }

    public void add(String subject, Property predicate, Resource object) {
        Resource r = model.createResource(subject);
        r.addProperty(predicate, object);
    }

    public void add(String subject, Property predicate, String object) {
        Resource r = model.createResource(subject);
        r.addProperty(predicate, model.createResource(object));
    }

//    public void add(String subject, String predicate, String literal,) {
//        Resource r = model.createResource(subject);
//
//        r.addProperty(model.createProperty(predicate), literal);
//        System.out.println("hola");
//    }

    public void deleteResource(String uri) {
        deleteSubject(uri);
        deleteObject(uri);
    }

    public void deleteSubject(String uri) {
        Resource r = model.createResource(uri);
        model.removeAll(r, null, null);
    }

    public void deleteObject(String uri) {
        Resource r = model.createResource(uri);
        model.removeAll(null, null, r);
    }



    public void delete(String subject, String predicate, String object){
        model.removeAll(new ResourceImpl(subject), new PropertyImpl(predicate), new ResourceImpl(object));
    }



    /**
     * Delete triple with oldIri and insert new triple with newIri in jena graph
     * @param oldIRI actual iri that appears in the triples.
     * @param newIRI new iri that is going to replace the actual iri.
     */
    public void updateResourceNodeIRI(String oldIRI, String newIRI){
        // Look and update triples where oldIRI is object.
        runAnUpdateQuery("DELETE {?s ?p <"+oldIRI+">} " +
                "INSERT {?s ?p <"+newIRI+">} WHERE {  ?s ?p <"+oldIRI+"> }");
        // Look and update triples where oldIRI is subject.
        runAnUpdateQuery("DELETE {<"+oldIRI+"> ?p ?o} " +
                "INSERT {<"+newIRI+"> ?p ?o} WHERE {  <"+oldIRI+"> ?p ?o }");
    }


    public void updateProperty(String oldIRI, String newIRI){
        // Look and update triples where oldIRI is object.
        runAnUpdateQuery("DELETE {?s ?p <"+oldIRI+">} " +
                "INSERT {?s ?p <"+newIRI+">} WHERE {  ?s ?p <"+oldIRI+"> }");
        // Look and update triples where oldIRI is subject.
        runAnUpdateQuery("DELETE {<"+oldIRI+"> ?p ?o} " +
                "INSERT {<"+newIRI+"> ?p ?o} WHERE {  <"+oldIRI+"> ?p ?o }");

//        runAnUpdateQuery("DELETE {<"+oldIRI+"> <"+RDF.type.getURI()+"> ?type} " +
//                "INSERT { <"+newIRI+"> <"+RDF.type.getURI()+"> ?type } WHERE {  <"+oldIRI+"> <"+RDF.type.getURI()+"> ?type }");
    }

    public  void runAnUpdateQuery(String sparqlQuery) {

        try {
                UpdateAction.parseExecute(sparqlQuery, model);
        } catch (Exception e) {
                e.printStackTrace();
        }
    }

    public ResultSet runAQuery(String query) {

        try (QueryExecution qExec = QueryExecutionFactory.create(QueryFactory.create(query), model)) {
            ResultSetRewindable results = ResultSetFactory.copyResults(qExec.execSelect());
            qExec.close();
            return results;
        } catch (Exception e) {
//            System.out.println("error runAqEURY");
            e.printStackTrace();
        }
        return null;
    }

    public boolean contains(String subject, String predicate, String object){

        return model.contains(model.createResource(subject), model.createProperty(predicate), model.createResource(object)  );
    }






    public String getRange(String subject) {

        String query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
                "SELECT ?range WHERE { <"+subject+"> rdfs:range ?range.}";
        List<String> result =  getVar(query, "range");
        if(result.isEmpty()){
            return null;
        } else {
            return result.get(0);
        }

    }


    public String getResources(String property) {

        String range = getRange(property);

        String query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>" +
                "SELECT ?resource WHERE { {?class rdf:type rdfs:Class} .}";
        List<String> result =  getVar(query, "resource");
        if(result.isEmpty()){
            return range;
        } else {
            return result.get(0);
        }

    }



    public String getDomain(String subject) {

        String query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " +
                "SELECT ?domain WHERE { <"+subject+"> rdfs:domain ?domain.}";
        List<String> result = getVar(query, "domain");
        if(result.isEmpty()){

            return null;
        } else {
            return result.get(0);
        }
    }

    private List<String> getVar(String query, String varname) {

        ResultSet results = runAQuery(query);
        List<String> rows = new ArrayList<>();
        while(results.hasNext()) {
            QuerySolution solution = results.nextSolution();
            rows.add(solution.getResource(varname).getURI());
        }
        if(rows.isEmpty()){
//            System.out.println("error, no "+varname+" definition....");
        }
        return rows;
    }

    public void connect(String subject, String predicate, String object) {
        Resource r = model.createResource(subject);
        Resource r2 = model.createResource(object);
        r.addProperty(model.createProperty(predicate), r2);
    }

    public void loadModel(String path){
//        model = RDFDataMgr.loadModel(path);
        model.read(path, "TURTLE");
    }


    public void write(String file, Lang lang) {
        try {
            RDFDataMgr.write(new FileOutputStream(file), model, Lang.TURTLE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(String file, String lang) {
        try {
            RDFDataMgr.write(new FileOutputStream(file), model, Lang.TURTLE);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void write(OutputStream writer, String lang) {
            RDFDataMgr.write(writer, model, Lang.TURTLE);

    }

    public void setPrefixes(Map<String, String> prefixes){
        this.model = model.setNsPrefixes(prefixes);
    }



}
