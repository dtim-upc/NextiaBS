package edu.upc.essi.dtim.nextiabs;

import com.github.andrewoma.dexx.collection.Pair;
import edu.upc.essi.dtim.nextiabs.metamodels.DataFrame_MM;
import edu.upc.essi.dtim.nextiabs.utils.*;
import edu.upc.essi.dtim.nextiabs.vocabulary.DataSourceVocabulary;
//import edu.upc.essi.dtim.nextiabs.vocabulary.Formats;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
//import org.slf4j.impl.StaticLoggerBinder;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
//import java.io.WriteAbortedException;
//import java.sql.*;


/**
 * Generates an instance of a DataFrame_Metamodel representation of a postgresSQL database
 * @author juane
 */
public class SQLBootstrap extends DataSource implements IBootstrap<Graph> {

    private final IDatabaseSystem Database;
    //private HashMap<String, SQLMetamodelTable> Metamodel;
    private SQLTableData tableData;
    private final String hostname;
    private final String username;
    private final String password;
    private String tableName;

    public SQLBootstrap(String id, String name, IDatabaseSystem DBType, String hostname, String username, String password) {
        super();
        this.id = id;
        this.name = name;
        G_target = new Graph();
        this.Database = DBType;
        this.hostname = hostname;
        this.username = username;
        this.password = password;
        this.tableName = "empleats";
        this.tableData = new SQLTableData(tableName);
        //this.Metamodel = new HashMap<String, SQLMetamodelTable>();

    }

    @Override
    public Graph bootstrapSchema() throws IOException {
        return bootstrapSchema(false);
    }

    @Override
    public Graph bootstrapSchema(Boolean generateMetadata) throws IOException {

        //* Graph, llista de tables, llista de totes les columnes (per si hi ha repetits), llista de refs. entre taules

        Database.connect(hostname, username, password);

        //Metamodel = Database.getMetamodel();
        tableData = Database.getMetamodelSingleTable(tableName);

        setPrefixes();

        //? productionRules_SQL_to_RDFS(); //este sería para convertir todo el metamodelo, incluidas relaciones

        //este sería para una sola tabla a partir de un conjunto de tablas que estan en formato SQLMetamodelTable
        productionRules_SQL_to_DF_MM(tableName); //setea G_target;

        //el wrapper sería un "SELECT * FROM TABLE"
        wrapper = generateWrapper();
        // System.out.println(wrapper);

        if(generateMetadata) {
            generateMetadata();
        }

        G_target.setPrefixes(prefixes);
        return G_target;
    }

    private String generateWrapper() {
        String wrapper = "SELECT ";
        List<String> columns = new LinkedList<>();
        for(Pair<String, String> col: tableData.getColumns())
            columns.add(col.component1());
        String columnNames = String.join(", ", columns);

        wrapper += columnNames;
        wrapper += " FROM ";
        wrapper += tableName;
        System.out.println("Generated wrapper: "+wrapper);
        return wrapper;
    }

    private void productionRules_SQL_to_DF_MM(String tableName) {
//      Rule 1 Instances of sql:Table are translated to instances of dfMM:DataFrame.
//      ∀t(〈t, rdf:type, sql:Table〉(G))=⇒∃c(〈c, rdf:type, dfMM:DataFrame〉(G′)∧c=t)
        G_target.add(createIRI(tableName), RDF.type, DataFrame_MM.DataFrame);
        G_target.addLiteral(createIRI(tableName), RDFS.label, tableName);

        //Rule 1 Instances of sql:Table are translated to instances of rdfs:Class.
        // ∀t(〈t, rdf:type, sql:Table〉(G))=⇒∃c(〈c, rdf:type, rdfs:Class〉(G′)∧c=t)
//        G_target.add(createIRI(tableName), RDF.type, RDFS.Class);
//        G_target.addLiteral(createIRI(tableName), RDFS.label, name);

        //Rule 2 Instances of sql:Column are translated to instances of rdf:Property. Also, it's required to define the rdfs:domain of said rdf:Property.
        //∀t, a (〈t, sql:hasColumn, a〉(G))=⇒∃c,p(〈p, rdf:type, rdf:Property〉(G′)∧〈p, rdfs:domain, c〉(G′)∧p=a∧c=t)
        //? ---------------- no, no?? -------------
//        for(Pair<String, String> col: tableData.getColumns()) {
//            G_target.add(createIRI(tableName+"."+col.component1()), RDF.type, DataFrame_MM.Data);
//            G_target.add(createIRI(tableName+"."+col.component1()), RDFS.domain, createIRI(tableName));
//            G_target.add(createIRI(tableName+"."+col.component1()), RDFS.range, DBTypeToRDFSType(col.component2()));
//            G_target.addLiteral(createIRI(tableName+"."+col.component1()), RDFS.label,col.component1());
//        }

        for(Pair<String, String> col: tableData.getColumns()){
            G_target.add(createIRI(tableName+"."+col.component1()), RDF.type, DataFrame_MM.Data);
            G_target.addLiteral(createIRI(tableName+"."+col.component1()), RDFS.label,col.component1());
            G_target.add(createIRI(tableName), DataFrame_MM.hasData,createIRI(tableName+"."+col.component1()));
            System.out.println(col.component2());
            if(col.component2().equals("integer"))
                G_target.add(createIRI(tableName+"."+col.component1()),DataFrame_MM.hasDataType, DataFrame_MM.Number );
            else
                G_target.add(createIRI(tableName+"."+col.component1()),DataFrame_MM.hasDataType, DataFrame_MM.String );
        }

        //Rule 2 Instances of sql:Column are translated to instances of rdf:Property. Also, it's required to define the rdfs:domain of said rdf:Property.
        //∀t, a (〈t, sql:hasColumn, a〉(G))=⇒∃c,p(〈p, rdf:type, rdf:Property〉(G′)∧〈p, rdfs:domain, c〉(G′)∧p=a∧c=t)
//        SQLTableData tableOrigin = tableData;
//        for(Pair<String, String> col: tableOrigin.getColumns()) {
//            G_target.add(createIRI(tableName+"."+col.component1()), RDF.type, RDF.Property);
//            G_target.add(createIRI(tableName+"."+col.component1()), RDFS.domain, createIRI(tableName));
//            G_target.add(createIRI(tableName+"."+col.component1()), RDFS.range, DBTypeToRDFSType(col.component2()));
//            G_target.addLiteral(createIRI(tableName+"."+col.component1()), RDFS.label,col.component1());
//        }

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

    private org.apache.jena.rdf.model.Resource DBTypeToDF_MMType(String type) {
        return DataFrame_MM.Primitive; //
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
        m.write("src/main/resources/out/withDataFrameSOURCE.ttl", "Turlte");
        DF_MMtoRDFS translate = new DF_MMtoRDFS();
        Graph x = translate.productionRulesDataframe_to_RDFS(m);
        x.setPrefixes(m.getModel().getNsPrefixMap());
        x.write("C:\\Users\\juane\\Documents\\NEXTIA\\src\\main\\resources\\out\\withDataFrameTARGET.ttl", "Turlte");
    }



}

