package ru.ibs.dtm.query.execution.plugin.adg.factory.impl;

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
        val tableName = request.getQueryLoadParam().getTableName();
        val datamart = request.getQueryLoadParam().getDatamart();
        val systemName = request.getQueryRequest().getEnvName();
        val helperTableNames = helperTableNamesFactory.create(systemName, datamart, tableName);
        return new AdgMppwKafkaContext(
                request.getTopic(),
                request.getQueryLoadParam().getDeltaHot(),
                tableName,
                helperTableNames,
                request.getSchema()
        );
    }
}
