package edu.upc.essi.dtim.nextiabs;

import com.github.andrewoma.dexx.collection.Pair;
import edu.upc.essi.dtim.nextiabs.utils.DataSource;
import edu.upc.essi.dtim.nextiabs.utils.Graph;
import edu.upc.essi.dtim.nextiabs.vocabulary.DataSourceVocabulary;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
//import org.slf4j.impl.StaticLoggerBinder;
import java.io.IOException;
import java.io.WriteAbortedException;
import java.sql.*;
import java.util.*;

/**
 * Generates an RDFS-compliant representation of a postgresSQL database
 * @author juane
 */
public class SQLBootstrap extends DataSource implements IBootstrap<Graph> {

    //! public Class.forName("com.mysql.jdbc.Driver");
    public Connection conn = null;

//!    private String sDriver = "com.mysql.jdbc.Driver";
//!    private String sURL = "jdbc:mysql://localhost:5432";

    public SQLBootstrap(String id, String name) {
        super();
        this.id = id;
        this.name = name;

        String connectionUrl = "jdbc:postgresql://localhost:5432/postgres";//?username=postgres&password=1234";
//?    jdbc:mysql://[host][,failoverhost...]
//?    [:port]/[database]
//?    [?propertyName1][=propertyValue1]
//?    [&propertyName2][=propertyValue2]...

        try {
            conn = DriverManager.getConnection(connectionUrl, "postgres", "1234");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
//TODO:        finally {
//            try {
//                conn.close();
//            } catch (SQLException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    @Override
    public Graph bootstrapSchema() throws IOException {
        return bootstrapSchema(false);
    }

    @Override
    public Graph bootstrapSchema(Boolean generateMetadata) throws IOException {

        //* Graph, llista de tables, llista de totes les columnes (per si hi ha repetits), llista de refs. entre taules
        G_target = new Graph();
        this.id = id;
        List<String> tables = new ArrayList();
        Map<String, Integer> allColumns = new HashMap<String, Integer>();
        Map<Pair<String, String>, String> references = new HashMap<Pair<String, String>, String>();

        G_target.add(createIRI(name), RDF.type, RDFS.Class);
        G_target.addLiteral(createIRI(name), RDFS.label, name);

        try {
            //? llista de taules guardades a 'tables'
            Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = stmt.executeQuery("SELECT table_name\n" +
                    "FROM INFORMATION_SCHEMA.TABLES\n" +
                    "WHERE table_type = 'BASE TABLE'");
            while(rs.next()) {
                String tableNamei = rs.getString("table_name");
                //! treiem les del sistema (no es universal, solució temporal) // no pots tenir una taula que comenci per sql_ feta pel user
                if(!tableNamei.startsWith("pg_") && !tableNamei.startsWith("sql_")) tables.add(tableNamei);
            }

            //? llista de referencies entre taules guardades a 'referencies'
            rs = stmt.executeQuery("select u.table_name as TablaDesti, u.column_name as ColumnaDesti, r.table_name as TablaOrigen, r.column_name as ColumnaOrigen\n" +
                    "FROM information_schema.constraint_column_usage       u\n" +
                    "INNER JOIN information_schema.referential_constraints fk\n" +
                    "           ON u.constraint_catalog = fk.unique_constraint_catalog\n" +
                    "               AND u.constraint_schema = fk.unique_constraint_schema\n" +
                    "               AND u.constraint_name = fk.unique_constraint_name\n" +
                    "INNER JOIN information_schema.key_column_usage        r\n" +
                    "           ON r.constraint_catalog = fk.constraint_catalog\n" +
                    "               AND r.constraint_schema = fk.constraint_schema\n" +
                    "               AND r.constraint_name = fk.constraint_name;");


            while(rs.next()){
                Pair<String, String> tmp = new Pair<String, String>(rs.getString("TablaOrigen"),rs.getString("ColumnaOrigen"));
                references.put(tmp,rs.getString("TablaDesti"));
                System.out.println("REFERENCIA: \n"+tmp + "  " + rs.getString("TablaDesti"));
            }

            //! per cada taula
            int i = 0;
            for (String t: tables) {
                //* ja tenim el primer node del graph (SQLPruebaX) com a classe i amb label
                //* creem propietats (Taula1, Taula2...) per aquest primer node
                G_target.add(createIRI("T"+i), RDF.type, RDF.Property);
                G_target.add(createIRI("T"+i),RDFS.domain,createIRI(name));

                //* creem la propia taula com a classe amb etiqueta
                G_target.add(createIRI(t), RDF.type, RDFS.Class);
                G_target.addLiteral(createIRI(t), RDFS.label, t);

                //* la unim al graph principal amb la propietat (T1, T2...)
                G_target.add(createIRI("T"+i),RDFS.range,createIRI(t));

                //? les columnes de cada taula
                Map<String, String> columns = new HashMap<String, String>();

                //?llista de columnes de cada taula
                rs = stmt.executeQuery("SELECT *\n" +
                        "  FROM information_schema.columns\n" +
                        "   WHERE table_name   = '"+t+"';"
                );
                while(rs.next()) {
                    String ColumnNamei = rs.getString("column_name");
                    String DataTypei = rs.getString("data_type");
                    if(allColumns.containsKey(ColumnNamei)){
                        allColumns.put(ColumnNamei, allColumns.get(ColumnNamei)+1);
                        columns.put(ColumnNamei+"("+allColumns.get(ColumnNamei).toString()+")", DataTypei);
                    }
                    else{
                        allColumns.put(ColumnNamei, 0);
                        columns.put(ColumnNamei, DataTypei);
                    }
                }

                //! per cada columna
                for (Map.Entry<String, String> set : columns.entrySet()) {

                    //* creo la columna com a property
                    G_target.add(createIRI(set.getKey()), RDF.type, RDF.Property);

                    //* aqui volia que la IRI fos TAULA/Columna - per columnes repetides no tenir que posar un (1)
//NO SE PUEDE -->   G_target.add(createIRI(t+"/"+set.getKey()), RDF.type, RDF.Property);

                    //* aqui volia mirar si la tripleta ja existia, pero el contains no em deixa fer-lo servir
//NO ME DEJA -->         if(!G_target.contains(createIRI(set.getKey()),RDFS.domain,createIRI(t))) G_target.add(createIRI(set.getKey()),RDFS.domain,createIRI(t));
                    G_target.add(createIRI(set.getKey()),RDFS.domain,createIRI(t));

                    //* miro si la columna es referrencia de alguna altra taula
                    if(references.containsKey(new Pair<String, String>(t, columnOriginal(set.getKey())))){
                        System.out.println(" ==== Table: "+t+" y columna: "+set.getKey());
                        //* i la poso com a range de la property
                        G_target.add(createIRI(set.getKey()),RDFS.range, createIRI(references.get(new Pair<String, String>(t, columnOriginal(set.getKey())))));
                    }
                    else{
                        //* o li setejo el type a la property
                        G_target.add(createIRI(set.getKey()),RDFS.range,DBTypeToRDFSType(set.getValue()));
                    }

                    //* poso el Label
                    G_target.addLiteral(createIRI(set.getKey()), RDFS.label,set.getKey());

                }

                ++i;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

//        String select =  parser.getHeaderNames().stream().map(a ->{ return  a +" AS "+ a.replace(".","_"); }).collect(Collectors.joining(","));
//        wrapper = "SELECT " + select  + " FROM " + name;
//
        if(generateMetadata)
            generateMetadata();


        wrapper = "SELECT * from "+UnionAllTables(tables);
        //System.out.println(wrapper);
        G_target.setPrefixes(prefixes);
        return G_target;
    }

    private String UnionAllTables(List<String> tables) {
        String result = "";
        for(int i = 0; i < tables.size(); ++i){
         result += (tables.get(i));
         if(i < tables.size()-1) result += " UNION ";
        }
        return result;
    }

    private String columnOriginal(String key) {
        if(key.matches(".*\\(\\d+\\)$")){
            return key.replaceAll("\\(\\d+\\)$", "");
        }
        return key;
    }

    private org.apache.jena.rdf.model.Resource DBTypeToRDFSType(String type) {
        if(type.equals("integer")) return XSD.xint;
        return XSD.xstring;
    }

    @Override
    public void generateMetadata(){
        String ds = DataSourceVocabulary.DataSource.getURI() +"/" + name;
        if (!id.equals("")){
            ds = DataSourceVocabulary.DataSource.getURI() +"/" + id;
            G_target.addLiteral( ds , DataSourceVocabulary.HAS_ID.getURI(), id);
        }
        G_target.add( ds , RDF.type.getURI(),  DataSourceVocabulary.DataSource.getURI() );
        G_target.addLiteral( ds , RDFS.label.getURI(),  name );

       // G_target.addLiteral( ds , DataSourceVocabulary.HAS_WRAPPER.getURI(), wrapper);
    }

    public void write(String file, String lang){
        G_target.write(file,lang);
    }



    public static void main(String[] args) throws IOException {

        SQLBootstrap sql = new SQLBootstrap("18","SQLPrueba4");
        Graph m = sql.bootstrapSchema(true);
//      m.write(System.out, "turtle");
        m.write("C:\\Users\\juane\\Documents\\NEXTIA\\src\\main\\resources\\out\\SQLPrueba4.ttl", "Turlte");

    }



}

