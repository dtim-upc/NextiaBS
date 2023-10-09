package edu.upc.essi.dtim.nextiabs;

import edu.upc.essi.dtim.NextiaCore.datasources.dataset.*;
import edu.upc.essi.dtim.NextiaCore.graph.Graph;
import edu.upc.essi.dtim.NextiaCore.graph.CoreGraphFactory;
import edu.upc.essi.dtim.nextiabs.utils.PostgresSQLImpl;

import java.io.IOException;

public class NextiaBootstrapImpl implements NextiaBootstrapInterface{

    @Override
    public Graph bootstrap(Dataset dataset) {
        Graph bootstrapG = CoreGraphFactory.createGraphInstance("normal");

        if (dataset.getClass().equals(CsvDataset.class)) {
            CSVBootstrap_with_DataFrame_MM_without_Jena csv = new CSVBootstrap_with_DataFrame_MM_without_Jena(dataset.getId(), dataset.getDatasetName(), ((CsvDataset) dataset).getPath());
            try {
                bootstrapG = csv.bootstrapSchema();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (dataset.getClass().equals(JsonDataset.class)) {
            JSONBootstrap_with_DataFrame_MM_without_Jena json = new JSONBootstrap_with_DataFrame_MM_without_Jena(dataset.getId(), dataset.getDatasetName(), ((JsonDataset) dataset).getPath());
            try {
                bootstrapG = json.bootstrapSchema();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (dataset.getClass().equals(SQLDataset.class)) {
            SQLBootstrap_with_DataFrame_MM_without_Jena sql =
                    new SQLBootstrap_with_DataFrame_MM_without_Jena(
                            dataset.getId(),
                            "odin_test",
                            ((SQLDataset) dataset).getTableName(),
                            new PostgresSQLImpl(),//Database type: postgres, mysql...
                            ((SQLDataset) dataset).getHostname(),
                            ((SQLDataset) dataset).getPort(),
                            ((SQLDataset) dataset).getUsername(),
                            ((SQLDataset) dataset).getPassword(),
                            "odin_test");
            bootstrapG = sql.bootstrapSchema();
        } else if (dataset.getClass().equals(XmlDataset.class)) {
            XMLBootstrap_with_DataFrame_MM_without_Jena xml = new XMLBootstrap_with_DataFrame_MM_without_Jena(dataset.getId(), dataset.getDatasetName(), ((XmlDataset) dataset).getPath());
            bootstrapG = xml.bootstrapSchema();
        }

        return bootstrapG;
    }
}
