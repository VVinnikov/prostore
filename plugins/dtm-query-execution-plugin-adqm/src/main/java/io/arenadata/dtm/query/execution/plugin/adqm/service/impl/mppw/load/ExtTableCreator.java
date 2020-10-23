package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load;

import lombok.NonNull;
import org.apache.avro.Schema;

public interface ExtTableCreator {
    String generate(@NonNull String topic,
                    @NonNull String table,
                    @NonNull Schema schema,
                    @NonNull String sortingKey);
}
