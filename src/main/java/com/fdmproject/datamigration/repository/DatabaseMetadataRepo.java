package com.fdmproject.datamigration.repository;

import com.fdmproject.datamigration.model.DatabaseMetadata;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;


public interface DatabaseMetadataRepo extends JpaRepository<DatabaseMetadata, String> {

}

