package edu.upc.essi.dtim.nextiabs;

import java.util.HashMap;

public class MySQLImpl implements IDatabaseSystem {
    @Override
    public void connect(String hostname, String username, String password) {
        //! public Class.forName("com.mysql.jdbc.Driver");
        //!    private String sDriver = "com.mysql.jdbc.Driver";
        //!    private String sURL = "jdbc:mysql://localhost:3306";
    }

    @Override
    public HashMap<String, SQLMetamodelTable> getMetamodel() {
        return null;
    }

    @Override
    public SQLMetamodelTable getMetamodelSingleTable(String tablename) {
        return null;
    }
}
