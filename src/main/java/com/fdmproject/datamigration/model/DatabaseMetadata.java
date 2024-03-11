package com.fdmproject.datamigration.model;


import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "databaseMetadata")
public class DatabaseMetadata {

    @Id
    private String tableName;
    @Column(length = 1023)
    private String columnNames;
    @Column(length = 1023)
    private String columnTypes;
    private int rowCount;
    private String snapshotPath;

    public DatabaseMetadata() {

    }

    public DatabaseMetadata(String tableName, String columnNames, String columnTypes, int rowCount, String snapshotPath) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.rowCount = rowCount;
        this.snapshotPath = snapshotPath;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String columnNames) {
        this.columnNames = columnNames;
    }

    public String getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(String columnTypes) {
        this.columnTypes = columnTypes;
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public String getSnapshotPath() {
        return snapshotPath;
    }

    public void setSnapshotPath(String snapshotPath) {
        this.snapshotPath = snapshotPath;
    }

}
