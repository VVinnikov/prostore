package io.arenadata.dtm.common.schema;

public interface SchemaStore extends SchemaProvider {
    public void add(VersionedSchema schema) throws Exception;
}
