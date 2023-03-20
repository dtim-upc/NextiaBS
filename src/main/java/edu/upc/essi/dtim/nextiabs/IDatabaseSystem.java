package edu.upc.essi.dtim.nextiabs;

import java.util.HashMap;

public interface IDatabaseSystem {
    void connect(String hostname, String username, String password);

    HashMap<String, SQLMetamodelTable> getMetamodel();

    SQLMetamodelTable getMetamodelSingleTable(String tablename);
}
