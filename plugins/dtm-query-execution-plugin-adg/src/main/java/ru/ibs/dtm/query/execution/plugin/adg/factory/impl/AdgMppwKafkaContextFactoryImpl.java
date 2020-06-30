package ru.ibs.dtm.query.execution.plugin.adg.factory.impl;

import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgMppwKafkaContextFactory;

@Component
@RequiredArgsConstructor
public class AdgMppwKafkaContextFactoryImpl implements AdgMppwKafkaContextFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    @Override
    public AdgMppwKafkaContext create(String datamartMnemonic,
                                      String topicName,
                                      String tableName,
                                      JsonObject schema,
                                      long hotDelta) {
        return new AdgMppwKafkaContext(
                topicName,
                hotDelta,
                tableName,
                helperTableNamesFactory.create(datamartMnemonic, tableName),
                schema
        );
    }
}
