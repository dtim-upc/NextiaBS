package edu.upc.essi.dtim.nextiabs.utils;

import java.util.HashMap;

public interface IDatabaseSystem {
    void connect(String hostname, String username, String password);

    HashMap<String, SQLTableData> getMetamodel();

    SQLTableData getMetamodelSingleTable(String tablename);
}
