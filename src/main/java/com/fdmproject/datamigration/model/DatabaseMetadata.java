package com.fdmproject.datamigration.model;


import jakarta.persistence.*;

import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "databaseMetadata")
public class DatabaseMetadata {

    public DatabaseMetadata(){

    }


    @Id
    private String tableName;

    @ElementCollection
    private List<String> columnNames = new ArrayList<>();
}
