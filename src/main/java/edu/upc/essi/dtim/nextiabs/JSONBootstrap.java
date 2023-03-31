package edu.upc.essi.dtim.nextiabs;

import edu.upc.essi.dtim.nextiabs.metamodels.JSON_MM;
import edu.upc.essi.dtim.nextiabs.utils.DataSource;
import edu.upc.essi.dtim.nextiabs.utils.Graph;
import edu.upc.essi.dtim.nextiabs.utils.JSON_Aux;
import edu.upc.essi.dtim.nextiabs.vocabulary.DataSourceVocabulary;
import edu.upc.essi.dtim.nextiabs.vocabulary.Formats;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
public class JSONBootstrap extends DataSource implements IBootstrap<Graph> {

    /**
     * used for the graph in the source metamodel
     */
    protected Graph G_source;

    public String path;

    /**
     * structure that contains all the primitives of a JSON. The key represents the label of each primitive,
     * whereas the values represent the complete information of each primitive (original key, label and path).
     */
    protected HashMap<String, JSON_Aux> JSONPrimitives;
    /**
     * structure that allows to transform arrays into rows when generating the wrapper
     */
    protected List<Pair<String,String>> lateralViews;
    /**
     * structure that contains all the keys that are wrong formatted
     */
    protected HashSet<String> keysWrongFormatted;

    private int objectCounter;
    private int arrayCounter;

    private boolean emptyJson;


    /**
     * creates an instance of the class JSONBootstrap
     * @param id identifier of the instance
     * @param name name of the file that contains the JSON to be bootstrapped
     * @param path path of the file that cointains the JSON to be bootstrapped
     */
    public JSONBootstrap(String id, String name, String path) {
        super();
        this.id = id;
        this.name = name;
        this.path = path;
        this.G_source = new Graph();
        objectCounter = 0;
        arrayCounter = 0;
        emptyJson = true;

        JSONPrimitives = new HashMap<>();
        lateralViews = Lists.newArrayList();
        keysWrongFormatted = new HashSet<>();
    }

    /**
     * generates the RDFS metamodel graph
     * @return generates the RDFS metamodel graph
     * @throws IOException if there is any error
     */
    @Override
    public Graph bootstrapSchema() throws IOException {
        return bootstrapSchema(false);
    }

    /**
     * generates the RDFS metamodel graph
     * @param generateMetadata boolean that indicates if the metadata has to be generated or not
     * @return the generated RDFS metamodel graph
     * @throws IOException if there is any error
     */
    @Override
    public Graph bootstrapSchema(Boolean generateMetadata) throws IOException {
        setPrefixes();
        Document(path, name);
        G_source.getModel().setNsPrefixes(prefixes);

        productionRules_JSON_to_RDFS();

        if (!emptyJson) {
            createWrapper();
        }

        generateMetadata();

        G_target.setPrefixes(prefixes);
        return G_target;
    }

    /**
     * generates the Metadata of the graph
     */
    @Override
    public void generateMetadata() {
        String ds = DataSourceVocabulary.DataSource.getURI() + "/" + name;

        if (!id.equals("")) {
            ds = DataSourceVocabulary.DataSource.getURI() + "/" + id;
            G_target.addLiteral(ds, DataSourceVocabulary.HAS_ID.getURI(), id);
        }

        G_source.add(ds, RDF.type.getURI(), DataSourceVocabulary.DataSource.getURI());
        G_source.addLiteral(ds, DataSourceVocabulary.HAS_PATH.getURI(), path);
        G_source.addLiteral(ds, RDFS.label.getURI(), name);

        G_target.add(ds, RDF.type.getURI(), DataSourceVocabulary.DataSource.getURI());
        G_target.addLiteral(ds, DataSourceVocabulary.HAS_PATH.getURI(), path);
        G_target.addLiteral(ds, RDFS.label.getURI(), name);
        G_target.addLiteral(ds, DataSourceVocabulary.HAS_FORMAT.getURI(), Formats.JSON.val());
        if (!emptyJson) G_target.addLiteral(ds, DataSourceVocabulary.HAS_WRAPPER.getURI(), wrapper);
    }

    /**
     * Creates the wrapper of the JSON that is being bootstrapped
     */
    private void createWrapper() {
        String SELECT = JSONPrimitives.entrySet().stream().map(p -> {
            if (p.getKey().equals(p.getValue().getKey())) return p.getValue().getPath();
            return p.getValue().getPath() + " AS " + p.getValue().getLabel();
        }).collect(Collectors.joining(","));

        String FROM = name;

        String LATERAL = lateralViews.stream()
                .map(p -> "LATERAL VIEW explode(" + p.getLeft() + ") AS " + p.getRight()).collect(Collectors.joining("\n"));
        wrapper = "SELECT " + SELECT + " FROM " + FROM + " " + LATERAL;
    }

    /**
     * Prints through terminal the wrapper generated using spark
     * @param j instance of the JSON that is being bootstrapped
     * @param path path of the file that contains the json to be bootstrapped
     * @param nameDataset name of the dataset
     */
    private static void printWrapper(JSONBootstrap j, String path, String nameDataset) {
        SparkConf conf = new SparkConf()
                .setAppName("Java Spark SQL basic example")
                .setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .sparkContext(new JavaSparkContext(conf).sc())
                .appName("spark")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> dataset = spark.read().option("multiline", true).json(path);
        dataset.createOrReplaceTempView(nameDataset);

        System.out.println("Wrapper is : " + j.getWrapper());
        Dataset<Row> namesDF = spark.sql(j.getWrapper());
        namesDF.show();
    }

    /**
     * instantiates the J:Document in the source graph using the document's root
     * @param documentPath path of the file that contains the JSON to be bootstrapped
     * @param documentName name of the document
     */
    private void Document(String documentPath, String documentName) {
        InputStream fis = null;
        try {
            fis = new FileInputStream(documentPath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        G_source.add(createIRI(documentName), RDF.type, JSON_MM.Document);

        Object(Json.createReader(fis).readValue().asJsonObject(), new JSON_Aux(documentName,"",""));
    }

    /**
     * checks which type of value is the key, and calls the respective functions depending on the type
     * @param value value of a pair key:value
     * @param keyInfo complete information of the key (original key name, label and path)
     */
    private void DataType(JsonValue value, JSON_Aux keyInfo) {
        if (value.getValueType() == JsonValue.ValueType.OBJECT) Object((JsonObject) value, keyInfo);
        else if (value.getValueType() == JsonValue.ValueType.ARRAY) Array((JsonArray) value, keyInfo);
        else Primitive(value, keyInfo);
    }

    /**
     * instantiates the source JSON graph with the content of the jsonObject
     * @param jsonObject a JSON object that contains a set of pairs key:value
     * @param parentInfo the complete information (original key name, label and path) of the parent object of the jsonObject (it can be empty)
     */
    private void Object (JsonObject jsonObject, JSON_Aux parentInfo) {
        String u_prime = freshObject();
        String iri_u_prime = createIRI(u_prime);

        G_source.add(iri_u_prime, RDF.type, JSON_MM.Object);
        G_source.addLiteral(iri_u_prime, RDFS.label, parentInfo.getKey());

        jsonObject.forEach((key,value)-> {
            emptyJson = false;
            String k_prime;
            if (!parentInfo.getPath().equals("")) {
                k_prime = freshAttribute(key, parentInfo.getPath());
            } else k_prime = freshAttribute(key, "");
            String iri_k = createIRI(k_prime);
            G_source.add(iri_k, RDF.type, JSON_MM.Key);

            if( value.getValueType() == JsonValue.ValueType.OBJECT || value.getValueType() == JsonValue.ValueType.ARRAY)
                G_source.addLiteral(iri_k, RDFS.label,"has " + k_prime);
            else {
                G_source.addLiteral(iri_k, RDFS.label, k_prime);
            }
            G_source.add(iri_u_prime, JSON_MM.hasKey, iri_k);

            String keyPath;
            if (keysWrongFormatted.contains(key) && !parentInfo.getPath().equals("")) {
                keyPath = parentInfo.getPath() + "." + "`" + key + "`";
            } else if (keysWrongFormatted.contains(key) && parentInfo.getPath().equals("")) {
                keyPath = "`" + key + "`";
            } else {
                keyPath = parentInfo.getPath() + "." + key;
                if (parentInfo.getPath().equals(""))
                    keyPath = key;
            }
            DataType(value, new JSON_Aux(key, k_prime, keyPath));
        });

        G_source.add(createIRI(parentInfo.getLabel()), JSON_MM.hasValue, iri_u_prime);
    }

    private String replaceLast(String string, String toReplace, String replacement) {
        int pos = string.lastIndexOf(toReplace);
        if (pos > -1) {
            return string.substring(0, pos)
                    + replacement
                    + string.substring(pos + toReplace.length());
        } else {
            return string;
        }
    }

    /**
     * instantiates the J:Array with a fresh iri and checks the type of its elements
     * @param jsonArray an homogeneous array that is either empty or its elements are of any of the types of JSON's ValueType
     * @param keyInfo complete information of the key (original key name, label and path)
     */
    private void Array (JsonArray jsonArray, JSON_Aux keyInfo) {
        String u_prime = freshArray();
        String iri_u_prime = createIRI(u_prime);

        G_source.add(iri_u_prime, RDF.type, JSON_MM.Array);
        G_source.addLiteral(iri_u_prime, RDFS.label, keyInfo.getKey());

        if (jsonArray.size() > 0) {
            DataType(jsonArray.get(0), new JSON_Aux(u_prime, keyInfo.getLabel(), replaceLast(keyInfo.getPath(),keyInfo.getKey(),keyInfo.getKey()+"_view")));
        } else {
            // TODO: some ds have empty array, check below example images array
            G_source.add(createIRI(keyInfo.getKey()), JSON_MM.hasValue, JSON_MM.String);
        }

        lateralViews.add(Pair.of(keyInfo.getKey(),keyInfo.getKey()+"_view"));
        G_source.add(createIRI(keyInfo.getKey()), JSON_MM.hasValue, iri_u_prime);
    }

    /**
     *checks whether the value is of type number, string or boolean
     * @param value value of a pair key:value
     * @param keyInfo complete information of the key (original key name, label and path)
     */
    private void Primitive (JsonValue value, JSON_Aux keyInfo) {
        if (value.getValueType() == JsonValue.ValueType.NUMBER) {
            G_source.add(createIRI(keyInfo.getLabel()), JSON_MM.hasValue, JSON_MM.Number);
            JSONPrimitives.put(keyInfo.getLabel(), keyInfo);
        }
        else if ((value.getValueType() == JsonValue.ValueType.TRUE) || (value.getValueType() == JsonValue.ValueType.FALSE)) {
            G_source.add(createIRI(keyInfo.getLabel()), JSON_MM.hasValue, JSON_MM.Boolean);
            JSONPrimitives.put(keyInfo.getLabel(), keyInfo);
        }
        else {
            G_source.add(createIRI(keyInfo.getLabel()), JSON_MM.hasValue, JSON_MM.String);
            JSONPrimitives.put(keyInfo.getLabel(), keyInfo);
        }
    }

    /**
     * generates a new name for an object
     * @return the name generated of the object
     */
    private String freshObject() {
        setObjectCounter(getObjectCounter()+1);
        return "Object_" + getObjectCounter();
    }

    /**
     * generates a new name for the key of an attribute
     * @param attribute the attribute for which to generate the name
     * @return the new name generated for the attribute
     */
    private String freshAttribute(String attribute, String parentPath) {
        if (attribute.contains(" ") || attribute.contains(".")) {
            keysWrongFormatted.add(attribute);
            attribute = attribute.replace(' ', '_');
            attribute = attribute.replace('.', '_');
        }

        if (!parentPath.equals("")) {
            if (parentPath.contains(" ") || parentPath.contains("`") || parentPath.contains(".")) {
                parentPath = parentPath.replace(' ', '_');
                parentPath = parentPath.replace('.', '_');
                parentPath = parentPath.replace("`", "");
            }
            attribute = parentPath + "_" + attribute;
        }
        return attribute;
    }

    /**
     * generates a new name for an array
     * @return the name generated of the array
     */
    private String freshArray() {
        setArrayCounter(getArrayCounter()+1);
        return "Array_" + getArrayCounter();
    }

    /**
     * applies a set of production rules to the source JSON graph in order to generate the target RDFS graph
     */
    private void productionRules_JSON_to_RDFS() {
        // Rule 1. Instances of J:Object are translated to instances of rdfs:Class .
        G_source.runAQuery("SELECT ?o ?label WHERE { ?o <"+RDF.type+"> <"+JSON_MM.Object+">. ?o <"+RDFS.label+"> ?label }").forEachRemaining(res -> {
            G_target.add(res.getResource("o").getURI(),RDF.type,RDFS.Class);
            G_target.addLiteral(res.getResource("o").getURI(),RDFS.label, res.getLiteral("label") );
            System.out.println("#1 - "+res.getResource("o").getURI()+", "+RDF.type+", "+RDFS.Class);
        });

        // Rule 2. Instances of J:Key are translated to instances of rdf:Property . Additionally, this requires defining the rdfs:domain
        //of such newly defined instance of rdf:Property .
        G_source.runAQuery("SELECT ?o ?k ?label WHERE { ?o <"+JSON_MM.hasKey+"> ?k. ?k <"+RDFS.label+"> ?label   }").forEachRemaining(res -> {
            G_target.add(res.getResource("k").getURI(),RDF.type,RDF.Property); System.out.println("#3 - "+res.getResource("k").getURI()+", "+RDF.type+", "+RDF.Property);
            G_target.addLiteral(res.getResource("k").getURI(),RDFS.label, res.getLiteral("label") );
            G_target.add(res.getResource("k").getURI(),RDFS.domain,res.getResource("o").getURI()); System.out.println("#3 - "+res.getResource("k").getURI()+", "+RDFS.domain+", "+res.getResource("o").getURI());
        });

        // Rule 3. Array keys are also ContainerMembershipProperty
        G_source.runAQuery("SELECT ?o ?k WHERE { ?o <"+JSON_MM.hasKey+"> ?k . ?k <"+JSON_MM.hasValue+"> ?a . ?a <"+RDF.type+"> <"+JSON_MM.Array+"> }").forEachRemaining(res -> {
            G_target.add(res.getResource("k").getURI(),RDF.type,RDFS.ContainerMembershipProperty);
        });

        //Rule 4. Range of primitives.
        //String case
        G_source.runAQuery("SELECT ?k WHERE { ?k <"+JSON_MM.hasValue+">+ <"+JSON_MM.String+"> . ?k <"+RDF.type+"> <"+JSON_MM.Key+"> }").forEachRemaining(res -> {
            G_target.add(res.getResource("k").getURI(),RDFS.range, XSD.xstring);
            System.out.println("#4 - "+res.getResource("k").getURI()+", "+RDFS.range+", "+XSD.xstring);
        });
        //Number case
        G_source.runAQuery("SELECT ?k WHERE { ?k <"+JSON_MM.hasValue+">+ <"+JSON_MM.Number+"> . ?k <"+RDF.type+"> <"+JSON_MM.Key+"> }").forEachRemaining(res -> {
            G_target.add(res.getResource("k").getURI(),RDFS.range,XSD.xint);
            System.out.println("#4 - "+res.getResource("k").getURI()+", "+RDFS.range+", "+XSD.xint);
        });
        //Boolean case
        G_source.runAQuery("SELECT ?k WHERE { ?k <"+JSON_MM.hasValue+">+ <"+JSON_MM.Boolean+"> . ?k <"+RDF.type+"> <"+JSON_MM.Key+"> }").forEachRemaining(res -> {
            G_target.add(res.getResource("k").getURI(), RDFS.range,XSD.xboolean);
            System.out.println("#4 - "+res.getResource("k").getURI()+", "+RDFS.range+", "+XSD.xboolean);
        });

        //Rule 5. Range of objects.
        G_source.runAQuery("SELECT ?k ?v WHERE { ?k <"+JSON_MM.hasValue+">+ ?v . ?k <"+RDF.type+"> <"+JSON_MM.Key+"> . ?v <"+RDF.type+"> <"+JSON_MM.Object+"> }").forEachRemaining(res -> {
            G_target.add(res.getResource("k").getURI(),RDFS.range,res.getResource("v")); System.out.println("#5 - "+res.getResource("k").getURI()+", "+RDFS.range+", "+res.getResource("v"));
        });


/**
 // Array of primitives

 // Array of arrays

 G_source.runAQuery("SELECT ?k ?v WHERE { ?k <"+JSON_MM.hasValue+"> ?v . ?v <"+RDF.type+"> <"+JSON_MM.Array+"> . ?k <"+RDF.type+"> <"+JSON_MM.Key+"> }").forEachRemaining(res -> {
 //			G_target.add(res.getResource("k").getURI(),RDF.type,RDF.Property); System.out.println("#5 - "+res.getResource("k").getURI()+", "+RDF.type+", "+RDF.Property);
 //?			G_target.add(res.getResource("k").getURI(),RDFS.range,res.getResource("v")); System.out.println("#5 - "+res.getResource("k").getURI()+", "+RDFS.range+", "+res.getResource("v"));
 });

 //Arrays of objects
 **/
    }


    public static void main(String[] args) throws IOException {
        String path = "src/main/resources/systematic_testing/pruebaGrandeCuatro.json";
        String nameDataset = "dataset1";
        JSONBootstrap j = new JSONBootstrap("1234", nameDataset, path);

        Graph M = j.bootstrapSchema();

        //write the source JSON graph and the target RDFS graph in files

        j.getG_source().write("src/main/resources/out/MJSONGraph.ttl", Lang.TURTLE);
        j.getG_target().write("src/main/resources/out/MRDFSGraph.ttl", Lang.TURTLE);

        Graph G = new Graph();
        java.nio.file.Path temp = Files.createTempFile("bootstrap",".ttl");
        System.out.println("Graph written to " + temp);
        G.write(temp.toString(), Lang.TURTLE);

        //following prints are used for debugging

        System.out.println("Primitives of the JSON");
        System.out.println(j.getJSONPrimitives());

        System.out.println("Lateral views");
        System.out.println(j.getLateralViews());

        System.out.println("Keys with spaces");
        System.out.println(j.getKeysWrongFormatted());

        //print the wrapper if it can be printed

        if (!j.emptyJson) {
            printWrapper(j, path, nameDataset);
        }
    }
}
