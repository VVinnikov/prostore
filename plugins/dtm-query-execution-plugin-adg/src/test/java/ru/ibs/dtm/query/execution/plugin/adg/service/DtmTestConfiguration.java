package ru.ibs.dtm.query.execution.plugin.adg.service;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.jackson.DatabindCodec;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import ru.ibs.dtm.common.dto.ActualDeltaRequest;
import ru.ibs.dtm.common.service.DeltaService;

import java.util.Collections;
import java.util.List;

@TestConfiguration
public class DtmTestConfiguration {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        SimpleModule simpleModule = new SimpleModule();
        ObjectMapper mapper = DatabindCodec.mapper();
        mapper.registerModule(simpleModule);
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        return mapper;
    }

    @Bean("adgDeltaService")
    public DeltaService deltaService() {
        return new DeltaService() {
            @Override
            public void getDeltaOnDateTime(ActualDeltaRequest actualDeltaRequest, Handler<AsyncResult<Long>> handler) {
                //TODO заглушка
                handler.handle(Future.succeededFuture(1L));
            }

            @Override
            public void getDeltasOnDateTimes(List<ActualDeltaRequest> list, Handler<AsyncResult<List<Long>>> handler) {
                //TODO заглушка
                handler.handle(Future.succeededFuture(Collections.singletonList(1L)));
            }
        };
    }

}
