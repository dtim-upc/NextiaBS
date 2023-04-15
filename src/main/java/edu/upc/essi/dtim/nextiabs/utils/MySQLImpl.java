package edu.upc.essi.dtim.nextiabs.utils;

import edu.upc.essi.dtim.nextiabs.SQLTableData;

import java.util.HashMap;

public class MySQLImpl implements IDatabaseSystem {
    @Override
    public void connect(String hostname, String username, String password) {
        //! public Class.forName("com.mysql.jdbc.Driver");
        //!    private String sDriver = "com.mysql.jdbc.Driver";
        //!    private String sURL = "jdbc:mysql://localhost:3306";
    }

    @Override
    public HashMap<String, SQLTableData> getMetamodel() {
        return null;
    }

    @Override
    public SQLTableData getMetamodelSingleTable(String tablename) {
        return null;
    }
}
