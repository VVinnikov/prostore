package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class MppwTransferRequestFactoryImpl implements MppwTransferRequestFactory {

    @Override
    public MppwTransferDataRequest create(MppwKafkaRequest request, List<Map<String, Object>> keyColumns) {
        return MppwTransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .tableName(request.getDestinationTableName())
                .hotDelta(request.getSysCn())
                .columnList(getColumnList(request))
                .keyColumnList(getKeyColumnList(keyColumns))
                .build();
    }

    private List<String> getKeyColumnList(List<Map<String, Object>> result) {
        return result.stream().map(o -> o.get("column_name").toString()).collect(Collectors.toList());
    }

    private List<String> getColumnList(MppwRequest request) {
        final List<String> columns = new Schema.Parser().parse(request.getUploadMetadata().getExternalSchema())
                .getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        columns.add(MetadataSqlFactoryImpl.SYS_FROM_ATTR);
        columns.add(MetadataSqlFactoryImpl.SYS_TO_ATTR);
        return columns;
    }
}
