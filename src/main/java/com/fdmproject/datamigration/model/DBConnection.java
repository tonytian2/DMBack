package com.fdmproject.datamigration.model;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBConnection {


    private Connection connection;
    public DBConnection(){

    }

    public void setConnection(String url, String username, String password) throws SQLException {
        this.connection = DriverManager.getConnection(url, username, password);
    }


    public Connection getConnection(){
        return this.connection;
    }
}
