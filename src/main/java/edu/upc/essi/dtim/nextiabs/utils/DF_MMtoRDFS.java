package edu.upc.essi.dtim.nextiabs.utils;

import edu.upc.essi.dtim.nextiabs.metamodels.DataFrame_MM;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

public class DF_MMtoRDFS {

    public Graph productionRulesDataframe_to_RDFS(Graph G_source){
        Graph G_target = new Graph();

        // Rule 1. Instances of J:Object(dataframe) are translated to instances of rdfs:Class .
        G_source.runAQuery("SELECT ?df ?label WHERE { ?df <"+RDF.type+"> <"+ DataFrame_MM.DataFrame+">. ?df <"+RDFS.label+"> ?label }").forEachRemaining(res -> {
            G_target.add(res.getResource("df").getURI(),RDF.type,RDFS.Class);
            G_target.addLiteral(res.getResource("df").getURI(),RDFS.label, res.getLiteral("label") );
            System.out.println("#1 - "+res.getResource("df").getURI()+", "+RDF.type+", "+RDFS.Class);
        });

        // Rule 2. Instances of DF:data (columnes o keys) are translated to instances of rdf:Property .
        G_source.runAQuery("SELECT ?df ?d ?label WHERE { ?df <"+DataFrame_MM.hasData+"> ?d. ?d <"+RDFS.label+"> ?label   }").forEachRemaining(res -> {
            G_target.add(res.getResource("d").getURI(),RDF.type,RDF.Property); System.out.println("#3 - "+res.getResource("d").getURI()+", "+RDF.type+", "+RDF.Property);
            G_target.addLiteral(res.getResource("d").getURI(),RDFS.label, res.getLiteral("label") );
            G_target.add(res.getResource("d").getURI(),RDFS.domain,res.getResource("df").getURI()); System.out.println("#3 - "+res.getResource("d").getURI()+", "+RDFS.domain+", "+res.getResource("df").getURI());
        });

        // Rule 3. Array keys (from json) are also ContainerMembershipProperty
        G_source.runAQuery("SELECT ?df ?d WHERE { ?df <"+DataFrame_MM.hasData+"> ?d . ?d <"+DataFrame_MM.hasDataType+"> ?a . ?a <"+RDF.type+"> <"+DataFrame_MM.Array+"> }").forEachRemaining(res -> {
            System.out.println("---------------------------------------\n"+res+"\n----------------------------");
            G_target.add(res.getResource("d").getURI(),RDF.type,RDFS.ContainerMembershipProperty);
        });

        //Rule 4. Range of primitives.
        G_source.runAQuery("SELECT ?d ?dt WHERE { ?d <"+DataFrame_MM.hasDataType+">+ <"+DataFrame_MM.String+"> . ?d <"+RDF.type+"> <"+DataFrame_MM.Data+"> }").forEachRemaining(res -> {
            G_target.add(res.getResource("d").getURI(),RDFS.range, XSD.xstring); System.out.println("#4 - "+res.getResource("d").getURI()+", "+RDFS.range+", "+XSD.xstring);
        });
        G_source.runAQuery("SELECT ?d ?dt WHERE { ?d <"+DataFrame_MM.hasDataType+">+ <"+DataFrame_MM.Number+"> . ?d <"+RDF.type+"> <"+DataFrame_MM.Data+"> }").forEachRemaining(res -> {
            G_target.add(res.getResource("d").getURI(),RDFS.range,XSD.xint); System.out.println("#4 - "+res.getResource("d").getURI()+", "+RDFS.range+", "+XSD.xint);
        });

        //Rule 5. Range of dataframes (i.e. json:objects).
        //        G_source.runAQuery("SELECT ?k ?v WHERE { ?k <"+JSON_MM.hasValue+">+ ?v . ?k <"+RDF.type+"> <"+JSON_MM.Key+"> . ?v <"+RDF.type+"> <"+JSON_MM.Object+"> }").forEachRemaining(res -> {
        G_source.runAQuery("SELECT ?d ?dt WHERE { ?d <"+DataFrame_MM.hasDataType+">+ ?dt . ?d <"+RDF.type+"> <"+DataFrame_MM.Data+"> . ?dt <"+RDF.type+"> <"+DataFrame_MM.DataFrame+"> }").forEachRemaining(res -> {
            G_target.add(res.getResource("d").getURI(),RDFS.range,res.getResource("dt")); System.out.println("#5 - "+res.getResource("d").getURI()+", "+RDFS.range+", "+res.getResource("dt"));
        });

        return G_target;
    }
}
