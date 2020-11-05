package io.arenadata.dtm.query.execution.core.service.ddl.impl;

import io.arenadata.dtm.common.eventbus.DataHeader;
import io.arenadata.dtm.common.eventbus.DataTopic;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.ddl.DatamartSchemaChangedEvent;
import io.arenadata.dtm.query.execution.plugin.api.ddl.PostSqlActionType;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlPostExecutor;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Service
public class PublishStatusDdlPostExecutor implements DdlPostExecutor {
    private final Vertx vertx;

    @Autowired
    public PublishStatusDdlPostExecutor(@Qualifier("coreVertx") Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public Future<Void> execute(DdlRequestContext context) {
        try {
            DatamartSchemaChangedEvent eventData = DatamartSchemaChangedEvent.builder()
                    .datamart(context.getDatamartName())
                    .changeDateTime(LocalDateTime.now(ZoneOffset.UTC))
                    .build();
            val message = DatabindCodec.mapper().writeValueAsString(eventData);
            val options = new DeliveryOptions();
            options.addHeader(DataHeader.DATAMART.getValue(), context.getDatamartName());
            options.addHeader(DataHeader.STATUS_EVENT_CODE.getValue(), StatusEventCode.DATAMART_SCHEMA_CHANGED.name());
            vertx.eventBus().send(DataTopic.STATUS_EVENT_PUBLISH.getValue(), message, options);
            return Future.succeededFuture();
        }
        catch (Exception e)
        {
            return Future.failedFuture(e);
        }
    }

    @Override
    public PostSqlActionType getPostActionType() {
        return PostSqlActionType.PUBLISH_STATUS;
    }
}
