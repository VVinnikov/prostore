package ru.ibs.dtm.common.schema;

import java.util.Map;

public interface SchemaProviderFactory {
    public SchemaProvider getProvider(Map<String, ?> config) throws Exception;
}
