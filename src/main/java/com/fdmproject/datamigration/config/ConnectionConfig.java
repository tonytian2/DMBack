package com.fdmproject.datamigration.config;

import com.fdmproject.datamigration.model.DBConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConnectionConfig {

    @Bean("SourceDBConnection")
    public DBConnection SourceDBConnection(){

        return new DBConnection();
    }
      @Bean("DestinationDBConnection")
      public DBConnection DestinationDBConnection(){

          return new DBConnection();
      }


}

