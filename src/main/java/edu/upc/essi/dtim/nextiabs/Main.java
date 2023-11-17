package edu.upc.essi.dtim.nextiabs;

import edu.upc.essi.dtim.NextiaCore.datasources.dataset.CsvDataset;
import edu.upc.essi.dtim.NextiaCore.discovery.Alignment;
import edu.upc.essi.dtim.NextiaCore.discovery.Attribute;
import edu.upc.essi.dtim.NextiaCore.graph.Graph;
import edu.upc.essi.dtim.nextiabs.utils.BootstrapResult;

import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception {
        CSVBootstrap_with_DataFrame_MM_without_Jena bs = new CSVBootstrap_with_DataFrame_MM_without_Jena();
        CsvDataset d = new CsvDataset("id", "zillow", "description", "C:\\Work\\Files\\test_datasets\\zillow.csv");

        BootstrapResult bsr = bs.bootstrap(d);
        System.out.println(bsr.getWrapper());
    }
}
