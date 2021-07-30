package io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.SYS_FROM_ATTR;
import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.SYS_TO_ATTR;

@Component
public class MppwTransferRequestFactoryImpl implements MppwTransferRequestFactory {

    @Override
    public MppwTransferDataRequest create(MppwKafkaRequest request, List<String> keyColumns) {
        return MppwTransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .tableName(request.getDestinationTableName())
                .hotDelta(request.getSysCn())
                .columnList(getColumnList(request))
                .keyColumnList(keyColumns)
                .build();
    }

    private List<String> getColumnList(MppwRequest request) {
        final List<String> columns = new Schema.Parser().parse(request.getUploadMetadata().getExternalSchema())
                .getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        columns.add(SYS_FROM_ATTR);
        columns.add(SYS_TO_ATTR);
        return columns;
    }
}
