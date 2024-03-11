package com.fdmproject.datamigration.model;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;

import java.io.Serializable;
import java.util.Objects;

@Embeddable
public class MigrationMetadataKey implements Serializable {
    @Column(name = "table_name", nullable = false)
    private String tableName;
    @Column(name = "attempt", nullable = false)
    private int attempt;

    public MigrationMetadataKey() {

    }

    public MigrationMetadataKey(String tableName, int attempt) {
        this.tableName = tableName;
        this.attempt = attempt;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    @Override
    public int hashCode() {
        return Objects.hash(attempt, tableName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MigrationMetadataKey other = (MigrationMetadataKey) obj;
        return attempt == other.attempt && Objects.equals(tableName, other.tableName);
    }
}
