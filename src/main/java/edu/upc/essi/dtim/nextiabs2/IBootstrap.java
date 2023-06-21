package edu.upc.essi.dtim.nextiabs2;

import edu.upc.essi.dtim.nextiabs2.utils.IDataSource;

import java.io.IOException;

public interface IBootstrap<T> extends IDataSource {

    //each class's constructor will take the appropriate arguments and then run bootstrapSchema
    T bootstrapSchema() throws IOException;
    T bootstrapSchema(Boolean generateMetadata) throws IOException;

    void generateMetadata();

}