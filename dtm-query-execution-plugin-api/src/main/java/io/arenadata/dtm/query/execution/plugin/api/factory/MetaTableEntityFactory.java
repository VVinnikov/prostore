package io.arenadata.dtm.query.execution.plugin.api.factory;

import io.vertx.core.Future;

import java.util.Optional;

public interface MetaTableEntityFactory<T> {
    String COLUMN_NAME = "column_name";
    String DATA_TYPE = "data_type";

    Future<Optional<T>> create(String envName, String schema, String table);
}
