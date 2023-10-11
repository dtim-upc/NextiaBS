package edu.upc.essi.dtim.nextiabs;

import edu.upc.essi.dtim.NextiaCore.datasources.dataset.Dataset;
import edu.upc.essi.dtim.NextiaCore.graph.Graph;

public interface NextiaBootstrapInterface {
    Graph bootstrap(Dataset dataset);

    String getWrapper(Dataset dataset);
}
