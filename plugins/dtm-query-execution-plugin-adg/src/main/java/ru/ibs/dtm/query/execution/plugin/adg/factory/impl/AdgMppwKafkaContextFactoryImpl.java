package ru.ibs.dtm.query.execution.plugin.adg.factory.impl;

import io.vertx.core.json.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adg.dto.mppw.AdgMppwKafkaContext;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgHelperTableNamesFactory;
import ru.ibs.dtm.query.execution.plugin.adg.factory.AdgMppwKafkaContextFactory;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

@Component
@RequiredArgsConstructor
public class AdgMppwKafkaContextFactoryImpl implements AdgMppwKafkaContextFactory {
    private final AdgHelperTableNamesFactory helperTableNamesFactory;

    @Override
    public AdgMppwKafkaContext create(MppwRequest request) {
        val tableName = request.getKafkaParameter().getTargetTableName();
        val datamart = request.getKafkaParameter().getDatamart();
        val systemName = request.getQueryRequest().getEnvName();
        val helperTableNames = helperTableNamesFactory.create(systemName, datamart, tableName);
        return new AdgMppwKafkaContext(
                request.getKafkaParameter().getUploadMetadata().getTopic(),
                request.getKafkaParameter().getSysCn(),
                tableName,
                helperTableNames,
                JsonObject.mapFrom(request.getKafkaParameter().getUploadMetadata().getExternalTableSchema())//FIXME check this
        );
    }
}
