package com.fdmproject.datamigration.model;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;

@Entity
public class MigrationMetadata {
    @EmbeddedId
    MigrationMetadataKey key;


    public MigrationMetadata() {
    }
    public MigrationMetadata(String tableName, int attempt) {
        this.key = new MigrationMetadataKey(tableName,attempt);
    }

    public MigrationMetadataKey getKey() {
        return key;
    }

    public void setKey(MigrationMetadataKey key) {
        this.key = key;
    }

    public int getNumRowsMigrated() {
        return numRowsMigrated;
    }

    public void setNumRowsMigrated(int numRowsMigrated) {
        this.numRowsMigrated = numRowsMigrated;
    }

    public State getAccuracy() {
        return accuracy;
    }

    public void setAccuracy(State accuracy) {
        this.accuracy = accuracy;
    }

    public State getCompleteness() {
        return completeness;
    }

    public void setCompleteness(State completeness) {
        this.completeness = completeness;
    }

    private int numRowsMigrated;
    private State accuracy;
    private State completeness;
}
