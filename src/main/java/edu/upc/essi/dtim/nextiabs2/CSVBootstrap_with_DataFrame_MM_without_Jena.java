package edu.upc.essi.dtim.nextiabs2;

import edu.upc.essi.dtim.NextiaCore.graph.*;
/*import edu.upc.essi.dtim.Graph.LocalGraph;
import edu.upc.essi.dtim.Graph.Triple;
import edu.upc.essi.dtim.Graph.URI;
import edu.upc.essi.dtim.nextiaEngine.temp.*;*/
import edu.upc.essi.dtim.nextiabs2.IBootstrap;
import edu.upc.essi.dtim.nextiabs2.utils.DF_MMtoRDFS;
import edu.upc.essi.dtim.nextiabs2.utils.DataSource;
import edu.upc.essi.dtim.nextiabs2.vocabulary.DataSourceVocabulary;
import edu.upc.essi.dtim.nextiabs2.vocabulary.Formats;
import edu.upc.essi.dtim.nextiabs2.temp.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.jena.atlas.lib.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Generates an RDFS-compliant representation of a CSV file schema
 * @author snadal
 */
public class CSVBootstrap_with_DataFrame_MM_without_Jena extends DataSource implements IBootstrap<Graph> {

	public String path;

	public CSVBootstrap_with_DataFrame_MM_without_Jena(String id, String name, String path) {
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
		G_target = CoreGraphFactory.createGraphInstance("local");
		this.id = id;
//		setPrefixes();

		BufferedReader br = new BufferedReader(new FileReader(path));
		CSVParser parser = CSVParser.parse(br, CSVFormat.DEFAULT.withFirstRecordAsHeader());

		G_target.addTriple(createIRI(name), RDF.type, DataFrame_MM.DataFrame);
		G_target.addTripleLiteral(createIRI(name), RDFS.label, name);
		parser.getHeaderNames().forEach(h -> {
			String h2 = h.replace("\"", "").trim();
//			System.out.println(h2);
			G_target.addTriple(createIRI(h2),RDF.type,DataFrame_MM.Data);
			G_target.addTripleLiteral(createIRI(h2), RDFS.label,h2 );
			G_target.addTriple(createIRI(name),DataFrame_MM.hasData,createIRI(h2));
			G_target.addTriple(createIRI(h2),DataFrame_MM.hasDataType,DataFrame_MM.String);

		});

		String select =  parser.getHeaderNames().stream().map(a ->{ return  a +" AS "+ a.replace(".","_"); }).collect(Collectors.joining(","));
		wrapper = "SELECT " + select  + " FROM " + name;

//		if(generateMetadata)
//			generateMetadata();
//		G_target.setPrefixes(prefixes);

		DF_MMtoRDFS translate = new DF_MMtoRDFS();
		G_target = translate.productionRulesDataframe_to_RDFS(G_target);
		return G_target;
	}

	@Override
	public void generateMetadata(){
		String ds = DataSourceVocabulary.DataSource.getURI() +"/" + name;
		if (!id.equals("")){
			ds = DataSourceVocabulary.DataSource.getURI() +"/" + id;
			G_target.addTripleLiteral( ds , DataSourceVocabulary.HAS_ID.getURI(), id);
		}
		G_target.addTriple( ds , RDF.type,  DataSourceVocabulary.DataSource.getURI() );
		G_target.addTripleLiteral( ds , DataSourceVocabulary.HAS_PATH.getURI(), path);
		G_target.addTripleLiteral( ds , RDFS.label,  name );

		G_target.addTripleLiteral( ds , DataSourceVocabulary.HAS_FORMAT.getURI(), Formats.CSV.val());
		G_target.addTripleLiteral( ds , DataSourceVocabulary.HAS_WRAPPER.getURI(), wrapper);
	}

//	public void write(String file, String lang){
//		G_target.write(file,lang);
//	}

	public static void main(String[] args) throws IOException {

		String pathcsv = "src/main/resources/artworks.csv";
		CSVBootstrap_with_DataFrame_MM_without_Jena csv = new CSVBootstrap_with_DataFrame_MM_without_Jena("12","artworks", pathcsv);
		Graph m =csv.bootstrapSchema(true);

//		DF_MMtoRDFS translate = new DF_MMtoRDFS();
//		Graph x = translate.productionRulesDataframe_to_RDFS(m);

		PrintGraph.printGraph(m);

//		x.setPrefixes(m.getModel().getNsPrefixMap());
//		x.write("src/main/resources/out/artworkDATAFRAME.ttl", "Turtle");
	}
}

