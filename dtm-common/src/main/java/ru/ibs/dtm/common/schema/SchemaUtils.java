/**
 * Copyright (C) Cloudera, Inc. 2018
 */
package ru.ibs.dtm.common.schema;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class SchemaUtils {


    public static final String SCHEMA_PROVIDER_FACTORY_CONFIG = "schemaProviderFactory";

    public static SchemaProvider getSchemaProvider(Map<String, ?> configs) {
        String schemaProviderFactoryClassName = (String) configs.get(SCHEMA_PROVIDER_FACTORY_CONFIG);
        try {
            return ((SchemaProviderFactory)Class.forName(schemaProviderFactoryClassName).newInstance()).getProvider(configs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, VersionedSchema> getVersionedSchemas(Map<String, ?> configs, SchemaProvider schemaProvider) {
        Map<String, VersionedSchema> schemas = new HashMap<>();
        Stream<String> schemaConfigs = configs.keySet().stream().filter(k -> k.startsWith("schemaversion."));
        schemaConfigs.forEach(k -> {
            String schemaName = k.substring("schemaversion.".length());
            Integer schemaVersion;
            if (configs.get(k) instanceof String) {
                schemaVersion = Integer.valueOf((String)configs.get(k));
            } else
            {
                schemaVersion = (Integer)configs.get(k);
            }
            VersionedSchema versionedSchema = schemaProvider.get(schemaName, schemaVersion);
            schemas.put(schemaName, versionedSchema);
        });
        return schemas;
    }

    public static int readSchemaId(InputStream stream ) throws IOException {
        try(DataInputStream is = new DataInputStream(stream)) {
            return is.readInt();
        }
    }


}
