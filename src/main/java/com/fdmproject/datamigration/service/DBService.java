package com.fdmproject.datamigration.service;

import com.fdmproject.datamigration.model.DBConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Connection;

@Service
public class DBService {

    private final DBConnection SourceDBConnection;
    private final DBConnection DestinationDBConnection;

    @Autowired
    public DBService(DBConnection SourceDBConnection, DBConnection DestinationDBConnection){
        this.SourceDBConnection = SourceDBConnection;
        this.DestinationDBConnection = DestinationDBConnection;
    }


    //public loginService()



    private createDestinationTables(){

    }

}
