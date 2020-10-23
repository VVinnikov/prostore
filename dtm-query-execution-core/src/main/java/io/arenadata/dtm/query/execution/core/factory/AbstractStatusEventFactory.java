package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.status.PublishStatusEventRequest;
import io.arenadata.dtm.common.status.StatusEventKey;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.UUID;

public abstract class AbstractStatusEventFactory<IN, OUT> implements StatusEventFactory<OUT> {
    private final Class<IN> inClass;

    protected AbstractStatusEventFactory(Class<IN> inClass) {
        this.inClass = inClass;
    }

    protected abstract OUT createEventMessage(StatusEventKey eventKey, IN eventData);

    @Override
    @SneakyThrows
    public PublishStatusEventRequest<OUT> create(@NonNull String datamart, @NonNull String eventData) {
        val eventKey = getEventKey(datamart);
        IN readValue = DatabindCodec.mapper().readValue(eventData, inClass);
        return new PublishStatusEventRequest<>(eventKey, createEventMessage(eventKey, readValue));
    }

    @NotNull
    private StatusEventKey getEventKey(String datamart) {
        return new StatusEventKey(datamart, LocalDateTime.now(), getEventCode(), UUID.randomUUID());
    }
}
