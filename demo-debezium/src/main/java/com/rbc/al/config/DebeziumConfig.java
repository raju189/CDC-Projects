package com.rbc.al.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.IOException;

@Configuration
public class DebeziumConfig {

    @Value("${datasource.host:localhost}")
    private String customerDbHost;

    @Value("${datasource.port:1433}")
    private int customerDbPort;

    @Value("${datasource.username:sa}")
    private String customerDbUsername;

    @Value("${datasource.password:Password123}")
    private String customerDbPassword;

    @Value("${datasource.dbName:accounts}")
    private String customerDbName;

    @Value("${topic-prefix:accounts}")


    @Bean
    public io.debezium.config.Configuration customerConnector() throws IOException {
        var offsetStorageTempFile = File.createTempFile("offsets_", ".dat");

        return io.debezium.config.Configuration.create()
                .with("name", "customer-mysql-connector")
                .with("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", offsetStorageTempFile.getPath())
                .with("offset.flush.interval.ms", "60000")
                .with("database.hostname", customerDbHost)
                .with("database.port", customerDbPort)
                .with("database.user", customerDbUsername)
                .with("database.password", customerDbPassword)
                .with("database.names", customerDbName)
                .with("database.include.list", customerDbName)
                .with("include.schema.changes", "false")
                .with("database.server.id", "10181")
                .with("topic.prefix","test")
                .with("schema.history.internal.kafka.topic", "members")
                .with("schema.history.internal.kafka.bootstrap.servers", "localhost:29092")
                .with("database.server.name", "accounts-mssql-db-server")
                .with("database.encrypt", false)
                .with("publication.autocreate.mode", "all_tables")
                .with("database.history", "io.debezium.relational.history.MemoryDatabaseHistory")
                .with("table.whitelist","dbo.Members")
//                .with("database.history", "io.debezium.relational.history.FileDatabaseHistory")
//                .with("database.history.file.filename", "/tmp/dbhistory.dat")

                .with("debezium.source.schema.history.internal","io.debezium.storage.file.history.FileSchemaHistory")
                .with("debezium.source.schema.history.internal.file.filename","data/schema_history.dat")
                .build();
    }
}
