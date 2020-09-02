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
        MppwTransferDataRequest mppwTransferDataRequest = new MppwTransferDataRequest();
        mppwTransferDataRequest.setDatamart(context.getRequest().getQueryLoadParam().getDatamart());
        mppwTransferDataRequest.setTableName(context.getRequest().getQueryLoadParam().getTableName());
        mppwTransferDataRequest.setHotDelta(context.getRequest().getQueryLoadParam().getDeltaHot());
        mppwTransferDataRequest.setColumnList(new Schema.Parser().parse(context.getRequest().getSchema().encode())
                .getFields().stream().map(Schema.Field::name).collect(Collectors.toList()));
        mppwTransferDataRequest.setKeyColumnList(getKeyColumnList(keyColumns));
        return mppwTransferDataRequest;
    }

    private List<String> getKeyColumnList(List<JsonObject> result) {
        return result.stream().map(o -> o.getString("column_name")).collect(Collectors.toList());
    }
}
