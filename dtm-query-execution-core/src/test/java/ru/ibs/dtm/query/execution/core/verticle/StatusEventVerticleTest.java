package ru.ibs.dtm.query.execution.core.verticle;

import io.github.jklingsporn.vertx.jooq.classic.async.AsyncClassicGenericQueryExecutor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;
import ru.ibs.dtm.common.delta.QueryDeltaResult;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.common.reader.QueryResult;
import ru.ibs.dtm.common.status.PublishStatusEventRequest;
import ru.ibs.dtm.common.status.StatusEventCode;
import ru.ibs.dtm.common.status.delta.OpenDeltaEvent;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import ru.ibs.dtm.kafka.core.service.kafka.KafkaStatusEventPublisher;
import ru.ibs.dtm.query.execution.core.CoreTestConfiguration;
import ru.ibs.dtm.query.execution.core.dao.ServiceDbFacade;
import ru.ibs.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import ru.ibs.dtm.query.execution.core.dto.delta.DeltaRecord;
import ru.ibs.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import ru.ibs.dtm.query.execution.core.service.delta.impl.BeginDeltaExecutor;
import ru.ibs.dtm.query.execution.core.utils.QueryResultUtils;
import ru.ibs.dtm.query.execution.plugin.api.delta.DeltaRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.delta.query.BeginDeltaQuery;
import ru.ibs.dtm.query.execution.plugin.api.request.DatamartRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ActiveProfiles("test")
@SpringBootTest(classes = {CoreTestConfiguration.class, StatusEventVerticle.class})
@ExtendWith(VertxExtension.class)
class StatusEventVerticleTest {
    public static final long EXPECTED_SIN_ID = 2L;
    public static final String EXPECTED_DATAMART = "test_datamart";
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    @MockBean
    KafkaAdminClient kafkaAdminClient;
    @MockBean
    KafkaConsumerMonitor kafkaConsumerMonitor;
    @MockBean
    KafkaProducer<String, String> jsonCoreKafkaProducer;
    @MockBean
    ServiceDbFacade serviceDbFacade;
    @MockBean
    DeltaServiceDao deltaServiceDao;
    @MockBean
    DeltaQueryResultFactory deltaQueryResultFactory;
    @MockBean
    KafkaStatusEventPublisher kafkaStatusEventPublisher;
    @MockBean
    AsyncClassicGenericQueryExecutor executor;
    @Autowired
    private BeginDeltaExecutor beginDeltaExecutor;

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(EXPECTED_DATAMART);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setLoadId(0L);
        delta.setLoadProcId("load-proc-1");
        delta.setDatamartMnemonic(req.getDatamartMnemonic());
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
    }

    @Test
    void publishDeltaOpenEvent(VertxTestContext testContext) throws InterruptedException {
        req.setSql("BEGIN DELTA");
        val deltaQuery = new BeginDeltaQuery();
        deltaQuery.setDeltaNum(null);
        val datamartRequest = new DatamartRequest(req);
        val context = new DeltaRequestContext(datamartRequest);
        context.setDeltaQuery(deltaQuery);
        val queryDeltaResult = new QueryResult();
        queryDeltaResult.setRequestId(req.getRequestId());
        queryDeltaResult.setResult(createResult("2020-06-15T05:06:55", EXPECTED_SIN_ID));

        when(deltaServiceDao.writeNewDeltaHot(any(), any())).thenReturn(Future.succeededFuture(1L));
        when(deltaQueryResultFactory.create(any(), any())).thenReturn(queryDeltaResult);
        Mockito.doAnswer(invocation -> {
            try {
                final PublishStatusEventRequest<OpenDeltaEvent> request = invocation.getArgument(0);
                final Handler<AsyncResult<?>> handler = invocation.getArgument(1);
                handler.handle(Future.succeededFuture());
                assertNotNull(request);
                assertNotNull(request.getEventKey());
                assertNotNull(request.getEventMessage());
                assertEquals(StatusEventCode.DELTA_OPEN, request.getEventKey().getEvent());
                assertEquals(EXPECTED_DATAMART, request.getEventKey().getDatamart());
                assertEquals(EXPECTED_SIN_ID, request.getEventMessage().getDeltaNum());
                testContext.completeNow();
            } catch (Exception ex) {
                testContext.failNow(ex);
            }
            return null;
        }).when(kafkaStatusEventPublisher).publish(any(), any());
        beginDeltaExecutor.execute(context, handler -> {
        });
        assertThat(testContext.awaitCompletion(5, TimeUnit.SECONDS)).isTrue();
    }

    private List<Map<String, Object>> createResult(String statusDate, Long sinId) {
        return QueryResultUtils.createResultWithSingleRow(Arrays.asList("statusDate", "sinId"), Arrays.asList(statusDate, sinId));
    }

}
