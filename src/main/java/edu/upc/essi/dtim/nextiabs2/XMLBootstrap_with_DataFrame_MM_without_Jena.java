package edu.upc.essi.dtim.nextiabs2;

import edu.upc.essi.dtim.NextiaCore.graph.CoreGraphFactory;
import edu.upc.essi.dtim.NextiaCore.graph.Graph;
import edu.upc.essi.dtim.nextiabs2.temp.DataFrame_MM;
import edu.upc.essi.dtim.nextiabs2.temp.PrintGraph;
import edu.upc.essi.dtim.nextiabs2.temp.RDF;
import edu.upc.essi.dtim.nextiabs2.temp.RDFS;
import edu.upc.essi.dtim.nextiabs2.utils.DF_MMtoRDFS;
import edu.upc.essi.dtim.nextiabs2.utils.DataSource;
import edu.upc.essi.dtim.nextiabs2.vocabulary.DataSourceVocabulary;
import edu.upc.essi.dtim.nextiabs2.vocabulary.Formats;

import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

/**
 * Generates an RDFS-compliant representation of a CSV file schema
 * @author snadal
 */
public class XMLBootstrap_with_DataFrame_MM_without_Jena extends DataSource implements IBootstrap<Graph> {

	public String path;

	public XMLBootstrap_with_DataFrame_MM_without_Jena(String id, String name, String path) {
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
		try {

			//build the XML DOM
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			Document document = builder.parse(new File(path));
			document.getDocumentElement().normalize();

			// Get the root element
			Element root = document.getDocumentElement();
			String rootName = root.getNodeName();
			System.out.println("Root element: " + rootName);
			for (int n = 0; n < root.getChildNodes().getLength(); n++) System.out.println("  "+n+"-"+root.getChildNodes().item(n).getNodeName());

			//Add the triples to the graph
			G_target.addTriple(createIRI(rootName), RDF.type, DataFrame_MM.DataFrame);
			G_target.addTripleLiteral(createIRI(rootName), RDFS.label, rootName);

			// Extract attributes recursively
			extractSubElementsFromElement(root);
		} catch (Exception e) {
			e.printStackTrace();
		}

//		String select =  parser.getHeaderNames().stream().map(a ->{ return  a +" AS "+ a.replace(".","_"); }).collect(Collectors.joining(","));
//		wrapper = "SELECT " + select  + " FROM " + name;

//		if(generateMetadata)
//			generateMetadata();
//		G_target.setPrefixes(prefixes);

		DF_MMtoRDFS translate = new DF_MMtoRDFS();
		G_target = translate.productionRulesDataframe_to_RDFS(G_target);

		return G_target;
	}

	private void extractSubElementsFromElement(Element element) {
		NodeList childNodes = element.getChildNodes();
		String parentName = element.getNodeName();

		int numChildren = childNodes.getLength();
		for (int n = 0; n < numChildren; n++) {
			Node child = childNodes.item(n);
			String childName = child.getNodeName();

			//All nodes have at least a '#text' node as child
			//It's almost always empty,
			//  but sometimes it contains the text value of the node (treated below)
			if(!childName.equals("#text")) {
				G_target.addTriple(createIRI(childName),RDF.type,DataFrame_MM.Data); 	//TODAS elem--type-->DF.Data
				G_target.addTripleLiteral(createIRI(childName), RDFS.label,childName);	//TODAS -->label-->elem.name
				G_target.addTriple(createIRI(parentName),DataFrame_MM.hasData,createIRI(childName)); //TODAS parentElem--hasData--> elem
			}

			if(child.getNodeType() == Node.ELEMENT_NODE){
				G_target.addTriple(createIRI(childName),DataFrame_MM.hasDataType,DataFrame_MM.Data);
			}

			//when child node is '#text' and it's not empty, it' value isn't 'null' nor ""
			//it's value is "\n   "
			if(child.getNodeType() == Node.TEXT_NODE && !child.getNodeValue().replaceAll("\n","").isBlank()) {
				G_target.addTriple(createIRI(childName),DataFrame_MM.hasDataType,DataFrame_MM.String);
			}

			//recursive call if the child has sub-elements
			if(child.hasChildNodes())extractSubElementsFromElement((Element) child);

		}
	}

	@Override
	public void generateMetadata(){
//		String ds = DataSourceVocabulary.DataSource.getURI() +"/" + name;
//		if (!id.equals("")){
//			ds = DataSourceVocabulary.DataSource.getURI() +"/" + id;
//			G_target.addTripleLiteral( ds , DataSourceVocabulary.HAS_ID.getURI(), id);
//		}
//		G_target.addTriple( ds , RDF.type,  DataSourceVocabulary.DataSource.getURI() );
//		G_target.addTripleLiteral( ds , DataSourceVocabulary.HAS_PATH.getURI(), path);
//		G_target.addTripleLiteral( ds , RDFS.label,  name );
//
//		G_target.addTripleLiteral( ds , DataSourceVocabulary.HAS_FORMAT.getURI(), Formats.CSV.val());
//		G_target.addTripleLiteral( ds , DataSourceVocabulary.HAS_WRAPPER.getURI(), wrapper);
	}

//	public void write(String file, String lang){
//		G_target.write(file,lang);
//	}

	public static void main(String[] args) throws IOException {

		String pathcsv = "src/main/resources/museums-and-galleries-1.xml";
		XMLBootstrap_with_DataFrame_MM_without_Jena csv = new XMLBootstrap_with_DataFrame_MM_without_Jena("12","artworks", pathcsv);
		Graph m =csv.bootstrapSchema(true);

		PrintGraph.printGraph(m);

	}
}

