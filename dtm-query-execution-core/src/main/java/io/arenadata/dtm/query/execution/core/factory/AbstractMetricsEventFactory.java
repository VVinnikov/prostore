package io.arenadata.dtm.query.execution.core.factory;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.SneakyThrows;

public abstract class AbstractMetricsEventFactory<IN> implements MetricsEventFactory<IN> {

    private final Class<IN> inClass;
    private final DtmConfig dtmSettings;

    public AbstractMetricsEventFactory(Class<IN> inClass, DtmConfig dtmSettings) {
        this.inClass = inClass;
        this.dtmSettings = dtmSettings;
    }

    @SneakyThrows
    @Override
    public IN create(String eventData) {
        return DatabindCodec.mapper().readValue(eventData, inClass);
    }
}
