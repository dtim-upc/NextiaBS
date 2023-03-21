package edu.upc.essi.dtim.nextiabs;

import com.github.andrewoma.dexx.collection.Pair;
import edu.upc.essi.dtim.nextiabs.utils.DataSource;
import edu.upc.essi.dtim.nextiabs.utils.Graph;
import edu.upc.essi.dtim.nextiabs.vocabulary.DataSourceVocabulary;
//import edu.upc.essi.dtim.nextiabs.vocabulary.Formats;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
//import org.slf4j.impl.StaticLoggerBinder;
import java.io.IOException;
//import java.io.WriteAbortedException;
//import java.sql.*;
import java.util.*;

/**
 * Generates an RDFS-compliant representation of a postgresSQL database
 * @author juane
 */
public class SQLBootstrap extends DataSource implements IBootstrap<Graph> {

    private IDatabaseSystem Database;
    private HashMap<String, SQLMetamodelTable> Metamodel;
    private String hostname, username, password;

    public SQLBootstrap(String id, String name, IDatabaseSystem DBType, String hostname, String username, String password) {
        super();
        this.id = id;
        this.name = name;
        this.Database = DBType;
        this.hostname = hostname;
        this.username = username;
        this.password = password;
        this.Metamodel = new HashMap<String, SQLMetamodelTable>();

    }

    @Override
    public Graph bootstrapSchema() throws IOException {
        return bootstrapSchema(false);
    }

    @Override
    public Graph bootstrapSchema(Boolean generateMetadata) throws IOException {

        //* Graph, llista de tables, llista de totes les columnes (per si hi ha repetits), llista de refs. entre taules
        G_target = new Graph();
        Database.connect(hostname, username, password);
        Metamodel = Database.getMetamodel();
        String tableName = "tabla1";

        setPrefixes();

        //? productionRules_SQL_to_RDFS(); //este sería para convertir todo el metamodelo, incluidas relaciones

        //este sería para una sola tabla a partir de un conjunto de tablas que estan en formato SQLMetamodelTable
        productionRules_SQL_to_RDFS(tableName); //setea G_target;

        //? generateWrapper(); //el wrapper sería un "SELECT * FROM TABLE"
        wrapper = "SELECT * from "+tableName;
        // System.out.println(wrapper);

        if(generateMetadata) {
            generateMetadata();
        }

        G_target.setPrefixes(prefixes);
        return G_target;
    }

    private void productionRules_SQL_to_RDFS(String tableName) {
        //Rule 1 Instances of sql:Table are translated to instances of rdfs:Class.
        // ∀t(〈t, rdf:type, sql:Table〉(G))=⇒∃c(〈c, rdf:type, rdfs:Class〉(G′)∧c=t)
        G_target.add(createIRI(tableName), RDF.type, RDFS.Class);
        G_target.addLiteral(createIRI(tableName), RDFS.label, name);

        //Rule 2 Instances of sql:Column are translated to instances of rdf:Property. Also, it's required to define the rdfs:domain of said rdf:Property.
        //∀t, a (〈t, sql:hasColumn, a〉(G))=⇒∃c,p(〈p, rdf:type, rdf:Property〉(G′)∧〈p, rdfs:domain, c〉(G′)∧p=a∧c=t)
        SQLMetamodelTable tableOrigin = Metamodel.get(tableName);
        for(Pair<String, String> col: tableOrigin.getColumns()) {
            G_target.add(createIRI(tableName+"."+col.component1()), RDF.type, RDF.Property);
            G_target.add(createIRI(tableName+"."+col.component1()), RDFS.domain, createIRI(tableName));
            G_target.add(createIRI(tableName+"."+col.component1()), RDFS.range, DBTypeToRDFSType(col.component2()));
            G_target.addLiteral(createIRI(tableName+"."+col.component1()), RDFS.label,col.component1());
        }

        //! l'he fet dalt
        //Rule 3.
        //The rdfs:range of an instance of sql:DataType. Case for instances of sql:Varchar which we translate to xsd:string.
        // The procedure  should be similar for instances of sql:Integer, sql:Date... using their pertaining type.
        //	∀a,d(〈a, sql:hasDataType, d〉(G)∧〈a, rdf:type, sql:Column〉(G)∧〈v, rdf:type, sql:Varchar〉(G))
        //  ⇒∃p(〈p, rdf:type, rdf:Property〉(G′)∧〈p, rdfs:range, xsd:string〉(G′)∧p=a

    }



    private org.apache.jena.rdf.model.Resource DBTypeToRDFSType(String type) {
        switch (type.toUpperCase()) {
            case "CHAR":
            case "VARCHAR":
            case "LONGVARCHAR":
            case "NCHAR":
            case "NVARCHAR":
            case "LONGNVARCHAR":
            case "INTERVAL DAY-TIME":
            case "INTERVAL YEAR-MONTH":
                return XSD.xstring;
            case "BINARY":
            case "VARBINARY":
            case "LONGVARBINARY":
                return XSD.base64Binary;
            case "BOOLEAN":
                return XSD.xboolean;
            case "SMALLINT":
                return XSD.xshort;
            case "INTEGER":
                return XSD.xint;
            case "BIGINT":
                return XSD.xlong;
            case "DECIMAL":
            case "NUMERIC":
                return XSD.decimal;
            case "FLOAT":
            case "REAL":
                return XSD.xfloat;
            case "DOUBLE PRECISION":
                return XSD.xdouble;
            case "DATE":
                return XSD.date;
            case "TIME":
                return XSD.time;
            case "TIMESTAMP":
                return XSD.dateTimeStamp;
            default:
                return XSD.xstring;
        }
    }

    @Override
    public void generateMetadata(){
        String ds = DataSourceVocabulary.DataSource.getURI() +"/" + name;
        if (!id.equals("")){
            ds = DataSourceVocabulary.DataSource.getURI() +"/" + id;
            G_target.addLiteral( ds , DataSourceVocabulary.HAS_ID.getURI(), id);
        }
//
//        G_target.add( ds , RDF.type.getURI(),  DataSourceVocabulary.DataSource.getURI() );
////        G_target.addLiteral( ds , DataSourceVocabulary.HAS_PATH.getURI(), path);
//        G_target.addLiteral( ds , RDFS.label.getURI(),  name );
//
//        G_target.addLiteral( ds , DataSourceVocabulary.HAS_FORMAT.getURI(), Formats.JSON.val());

        G_target.addLiteral( ds , DataSourceVocabulary.HAS_WRAPPER.getURI(), wrapper);
    }

    public void write(String file, String lang){
        G_target.write(file,lang);
    }



    public static void main(String[] args) throws IOException {


        SQLBootstrap sql = new SQLBootstrap("18","SQLInterface1",new PostgresSQLImpl(),"localhost...", "user", "psswrd");
        Graph m = sql.bootstrapSchema(true);
//      m.write(System.out, "turtle");
        m.write("C:\\Users\\juane\\Documents\\NEXTIA\\src\\main\\resources\\out\\SQLInterface1.ttl", "Turlte");

    }



}

