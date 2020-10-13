package ru.ibs.dtm.query.execution.plugin.adb.factory.impl;

import io.vertx.core.json.JsonObject;
import org.apache.avro.Schema;
import org.springframework.stereotype.Component;
import ru.ibs.dtm.query.execution.plugin.adb.factory.MppwTransferRequestFactory;
import ru.ibs.dtm.query.execution.plugin.adb.service.impl.mppw.dto.MppwTransferDataRequest;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class MppwTransferRequestFactoryImpl implements MppwTransferRequestFactory {

    @Override
    public MppwTransferDataRequest create(MppwRequestContext context, List<JsonObject> keyColumns) {
        return MppwTransferDataRequest.builder()
                .datamart(context.getRequest().getKafkaParameter().getDatamart())
                .tableName(context.getRequest().getKafkaParameter().getTargetTableName())
                .hotDelta(context.getRequest().getKafkaParameter().getSysCn())
                .columnList(getColumnList(context))
                .keyColumnList(getKeyColumnList(keyColumns))
                .build();
    }

    private List<String> getKeyColumnList(List<JsonObject> result) {
        return result.stream().map(o -> o.getString("column_name")).collect(Collectors.toList());
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
