package com.fdmproject.datamigration.model;

import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;

@Entity
public class MigrationMetadata {
    @EmbeddedId
    MigrationMetadataKey key;

    private int numRowsMigrated;


    private State migrationState;
    private State accuracy;
    private State completeness;

    public MigrationMetadata() {
    }

    public MigrationMetadata(MigrationMetadataKey key, int numRowsMigrated) {
        this.key = key;
        this.numRowsMigrated = numRowsMigrated;
    }

    public MigrationMetadata(String tableName, int attempt) {

        this.key = new MigrationMetadataKey(tableName,attempt);
        this.numRowsMigrated = 0;
        migrationState = State.NOT_STARTED;
        accuracy = State.NOT_STARTED;
        completeness = State.NOT_STARTED;
        ;
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


    public State getMigrationState() {
        return migrationState;
    }

    public void setMigrationState(State migrationState) {
        this.migrationState = migrationState;
    }
}
