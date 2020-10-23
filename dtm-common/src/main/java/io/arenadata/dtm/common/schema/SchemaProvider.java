package io.arenadata.dtm.common.schema;

import org.apache.avro.Schema;

public interface SchemaProvider extends AutoCloseable {
    public VersionedSchema get(int id);
    public VersionedSchema get(String schemaName, int schemaVersion);
    public VersionedSchema getMetadata(Schema schema);
}
