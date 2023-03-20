package edu.upc.essi.dtim.nextiabs;

import com.github.andrewoma.dexx.collection.Pair;

import java.util.ArrayList;
import java.util.List;

public class SQLMetamodelTable {
    private String name;

    //el datatype de les columnes hauria d'estar ja en RDFS? no tindria sentit, hauria d'estar en l'original
    // perque esta és la implementació, però despres s'haurà de consultar quina estrategia és suposo
    // per veure les conversións? Son diferents o son totes en SQL iguals?
    private List<Pair<String, String>> columns;

    private List<Pair<List<String>, String>> references;


    SQLMetamodelTable(String name){
        this.name = name;
        this.columns = new ArrayList<>();
    }

    List<Pair<String, String>> getColumns(){
        return columns;
    }
    void putColumns(List<Pair<String, String>> columns){
        this.columns = columns;
    }

    void addColumn(Pair<String, String> column){
        columns.add(column);
    }

    void addColumn(String name, String dataType) {
        columns.add(new Pair(name, dataType));
    }


    //setters i getters
    // Esta clase se setea al sacar el metamodelo de una base de datos
}
