package io.arenadata.dtm.query.execution.core.verticle;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.common.status.PublishStatusEventRequest;
import io.arenadata.dtm.common.status.StatusEventCode;
import io.arenadata.dtm.common.status.delta.OpenDeltaEvent;
import io.arenadata.dtm.kafka.core.configuration.kafka.KafkaZookeeperProperties;
import io.arenadata.dtm.kafka.core.repository.ZookeeperKafkaProviderRepositoryImpl;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaStatusEventPublisher;
import io.arenadata.dtm.kafka.core.service.kafka.RestConsumerMonitorImpl;
import io.arenadata.dtm.query.execution.core.CoreTestConfiguration;
import io.arenadata.dtm.query.execution.core.configuration.properties.ServiceDbZookeeperProperties;
import io.arenadata.dtm.query.execution.core.converter.CoreTypeToSqlTypeConverter;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.dao.ServiceDbFacadeImpl;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.DeltaServiceDao;
import io.arenadata.dtm.query.execution.core.dao.delta.zookeeper.impl.DeltaServiceDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.ServiceDbDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.EntityDaoImpl;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.impl.ServiceDbDaoImpl;
import io.arenadata.dtm.query.execution.core.dto.delta.DeltaRecord;
import io.arenadata.dtm.query.execution.core.dto.delta.query.BeginDeltaQuery;
import io.arenadata.dtm.query.execution.core.factory.DeltaQueryResultFactory;
import io.arenadata.dtm.query.execution.core.service.RestoreStateService;
import io.arenadata.dtm.query.execution.core.service.delta.impl.BeginDeltaExecutor;
import io.arenadata.dtm.query.execution.core.service.impl.RestoreStateServiceImpl;
import io.arenadata.dtm.query.execution.core.utils.DeltaQueryUtil;
import io.arenadata.dtm.query.execution.core.utils.QueryResultUtils;
import io.arenadata.dtm.query.execution.core.verticle.impl.TaskVerticleExecutorImpl;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ActiveProfiles("test")
@SpringBootTest(classes = {CoreTestConfiguration.class,
        StatusEventVerticle.class,
        TaskVerticleExecutorImpl.class,
        ServiceDbZookeeperProperties.class,
        KafkaZookeeperProperties.class
})
@ExtendWith(VertxExtension.class)
class StatusEventVerticleTest {
    public static final long EXPECTED_SIN_ID = 2L;
    public static final String EXPECTED_DATAMART = "test_datamart";
    private final QueryRequest req = new QueryRequest();
    private final DeltaRecord delta = new DeltaRecord();
    @MockBean
    KafkaAdminClient kafkaAdminClient;
    KafkaConsumerMonitor kafkaConsumerMonitor = mock(RestConsumerMonitorImpl.class);
    @MockBean
    KafkaProducer<String, String> jsonCoreKafkaProducer;
    ServiceDbFacade serviceDbFacade = mock(ServiceDbFacadeImpl.class);
    DeltaServiceDao deltaServiceDao = mock(DeltaServiceDaoImpl.class);
    ServiceDbDao serviceDbDao = mock(ServiceDbDaoImpl.class);
    EntityDao entityDao = mock(EntityDaoImpl.class);
    RestoreStateService restoreStateService = mock(RestoreStateServiceImpl.class);
    @MockBean
    @Qualifier("beginDeltaQueryResultFactory")
    DeltaQueryResultFactory deltaQueryResultFactory;
    @MockBean
    KafkaStatusEventPublisher kafkaStatusEventPublisher;
    @Autowired
    private BeginDeltaExecutor beginDeltaExecutor;

    @BeforeEach
    void beforeAll() {
        req.setDatamartMnemonic(EXPECTED_DATAMART);
        req.setRequestId(UUID.fromString("6efad624-b9da-4ba1-9fed-f2da478b08e8"));
        delta.setDatamart(req.getDatamartMnemonic());
        when(serviceDbFacade.getDeltaServiceDao()).thenReturn(deltaServiceDao);
        when(serviceDbFacade.getServiceDbDao()).thenReturn(serviceDbDao);
        when(serviceDbDao.getEntityDao()).thenReturn(entityDao);
        when(restoreStateService.restoreState()).thenReturn(Future.succeededFuture());
    }

    @Test
    @Disabled
    void publishDeltaOpenEvent(VertxTestContext testContext) throws InterruptedException {
        //FIXME
        req.setSql("BEGIN DELTA");
        long deltaNum = 1L;
        BeginDeltaQuery deltaQuery = BeginDeltaQuery.builder()
                .datamart("test")
                .request(req)
                .build();

        QueryResult queryResult = new QueryResult();
        queryResult.setRequestId(req.getRequestId());
        queryResult.setResult(createResult(deltaNum));

        when(deltaServiceDao.writeNewDeltaHot(any(), any())).thenReturn(Future.succeededFuture(1L));
        when(deltaQueryResultFactory.create(any()))
                .thenReturn(queryResult);

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
        beginDeltaExecutor.execute(deltaQuery, handler -> {
        });
        assertThat(testContext.awaitCompletion(10, TimeUnit.SECONDS)).isTrue();
    }

    private List<Map<String, Object>> createResult(Long deltaNum) {
        return QueryResultUtils.createResultWithSingleRow(Collections.singletonList(DeltaQueryUtil.NUM_FIELD),
                Collections.singletonList(deltaNum));
    }

}
