package io.arenadata.dtm.query.execution.plugin.adb.factory.impl;

import io.arenadata.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class MppwTransferRequestFactoryImpl implements MppwTransferRequestFactory {

    @Override
    public MppwTransferDataRequest create(MppwRequestContext context, List<Map<String, Object>> keyColumns) {
        return MppwTransferDataRequest.builder()
                .datamart(context.getRequest().getKafkaParameter().getDatamart())
                .tableName(context.getRequest().getKafkaParameter().getDestinationTableName())
                .hotDelta(context.getRequest().getKafkaParameter().getSysCn())
                .columnList(getColumnList(context))
                .keyColumnList(getKeyColumnList(keyColumns))
                .build();
    }

    private List<String> getKeyColumnList(List<Map<String, Object>> result) {
        return result.stream().map(o -> o.get("column_name").toString()).collect(Collectors.toList());
    }

    private List<String> getColumnList(MppwRequestContext context) {
        final List<String> columns = new Schema.Parser().parse(context.getRequest()
                .getKafkaParameter().getUploadMetadata().getExternalSchema())
                .getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
        columns.add(MetadataSqlFactoryImpl.SYS_FROM_ATTR);
        columns.add(MetadataSqlFactoryImpl.SYS_TO_ATTR);
        return columns;
    }
}
