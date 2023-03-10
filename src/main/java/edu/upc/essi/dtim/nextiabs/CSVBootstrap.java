package edu.upc.essi.dtim.nextiabs;

import edu.upc.essi.dtim.nextiabs.utils.DataSource;
import edu.upc.essi.dtim.nextiabs.utils.Graph;
import edu.upc.essi.dtim.nextiabs.vocabulary.DataSourceVocabulary;
import edu.upc.essi.dtim.nextiabs.vocabulary.Formats;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Generates an RDFS-compliant representation of a CSV file schema
 * @author snadal
 */
public class CSVBootstrap extends DataSource implements IBootstrap<Graph> {

	public String path;

	public CSVBootstrap(String id, String name, String path) {
		super();
		this.id = id;
		this.name = name;
		this.path = path;
	}

	@Override
	public Graph bootstrapSchema() throws IOException {
		return bootstrapSchema(false);
	}

	@Override
	public Graph bootstrapSchema(Boolean generateMetadata) throws IOException {
		G_target = new Graph();
		this.id = id;

		BufferedReader br = new BufferedReader(new FileReader(path));
		CSVParser parser = CSVParser.parse(br, CSVFormat.DEFAULT.withFirstRecordAsHeader());

		G_target.add(createIRI(name), RDF.type, RDFS.Class);
		G_target.addLiteral(createIRI(name), RDFS.label, name);
		parser.getHeaderNames().forEach(h -> {
			String h2 = h.replace("\"", "").trim();
//			System.out.println(h2);
			G_target.add(createIRI(h2),RDF.type,RDF.Property);
			G_target.add(createIRI(h2),RDFS.domain,createIRI(name));
			G_target.add(createIRI(h2),RDFS.range,XSD.xstring);
			G_target.addLiteral(createIRI(h2), RDFS.label,h2 );
		});

		String select =  parser.getHeaderNames().stream().map(a ->{ return  a +" AS "+ a.replace(".","_"); }).collect(Collectors.joining(","));
		wrapper = "SELECT " + select  + " FROM " + name;

		if(generateMetadata)
			generateMetadata();
		G_target.setPrefixes(prefixes);
		return G_target;
	}

	@Override
	public void generateMetadata(){
		String ds = DataSourceVocabulary.DataSource.getURI() +"/" + name;
		if (!id.equals("")){
			ds = DataSourceVocabulary.DataSource.getURI() +"/" + id;
			G_target.addLiteral( ds , DataSourceVocabulary.HAS_ID.getURI(), id);
		}
		G_target.add( ds , RDF.type.getURI(),  DataSourceVocabulary.DataSource.getURI() );
		G_target.addLiteral( ds , DataSourceVocabulary.HAS_PATH.getURI(), path);
		G_target.addLiteral( ds , RDFS.label.getURI(),  name );

		G_target.addLiteral( ds , DataSourceVocabulary.HAS_FORMAT.getURI(), Formats.CSV.val());
		G_target.addLiteral( ds , DataSourceVocabulary.HAS_WRAPPER.getURI(), wrapper);
	}

	public void write(String file, String lang){
		G_target.write(file,lang);
	}

	public static void main(String[] args) throws IOException {

		String pathcsv = "src/main/resources/artworks.csv";
		CSVBootstrap csv = new CSVBootstrap("12","artworks", pathcsv);
		Graph m =csv.bootstrapSchema(true);
		m.write(System.out, "Turtle");
	}


}

