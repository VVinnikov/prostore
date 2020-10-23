package io.arenadata.dtm.query.execution.core.service.delta;

import io.arenadata.dtm.common.eventbus.DataHeader;
import io.arenadata.dtm.common.eventbus.DataTopic;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.SneakyThrows;
import lombok.val;

public interface StatusEventPublisher {

    @SneakyThrows
    default void publishStatus(StatusEventCode eventCode, String datamart, Object eventData) {
        val message = DatabindCodec.mapper().writeValueAsString(eventData);
        val options = new DeliveryOptions();
        options.addHeader(DataHeader.DATAMART.getValue(), datamart);
        options.addHeader(DataHeader.STATUS_EVENT_CODE.getValue(), eventCode.name());
        getVertx().eventBus()
            .send(DataTopic.STATUS_EVENT_PUBLISH.getValue(), message, options);
    }

    Vertx getVertx();
}
