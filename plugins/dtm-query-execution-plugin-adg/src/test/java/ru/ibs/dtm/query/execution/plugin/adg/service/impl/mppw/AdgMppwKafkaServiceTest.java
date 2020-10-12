package ru.ibs.dtm.query.execution.plugin.adg.service.impl.mppw;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;
import ru.ibs.dtm.common.plugin.exload.QueryLoadParam;
import ru.ibs.dtm.common.reader.QueryRequest;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.AdgConnectorApiProperties;
import ru.ibs.dtm.query.execution.plugin.adg.configuration.AdgMppwKafkaProperties;
import ru.ibs.dtm.query.execution.plugin.adg.factory.impl.AdgHelperTableNamesFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adg.factory.impl.AdgMppwKafkaContextFactoryImpl;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.TtKafkaError;
import ru.ibs.dtm.query.execution.plugin.adg.model.cartridge.response.TtLoadDataKafkaResponse;
import ru.ibs.dtm.query.execution.plugin.adg.service.TtCartridgeClient;
import ru.ibs.dtm.query.execution.plugin.api.mppw.MppwRequestContext;
import ru.ibs.dtm.query.execution.plugin.api.request.MppwRequest;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
@EnabledIfEnvironmentVariable(named = "skipITs", matches = "false")
class AdgMppwKafkaServiceTest {

    private final TtCartridgeClient client = mock(TtCartridgeClient.class);
    private final AdgMppwKafkaService service = getAdgMppwKafkaService();

    @BeforeEach
    public void before() {
        Mockito.clearInvocations(client);
    }

    @Test
    void allGoodInitTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context, ar -> {
            assertTrue(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(1)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(1)).transferDataToScdTable(any(), any());
            verify(client, VerificationModeFactory.times(0)).cancelSubscription(any(), any());
        });
    }

    @Test
    void allGoodCancelTest() {
        val context = getRequestContext();
        context.getRequest().setIsLoadStart(false);
        allGoodApiMock();
        service.execute(context, ar -> {
            assertTrue(ar.succeeded());
            verify(client, VerificationModeFactory.times(0)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(0)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
            verify(client, VerificationModeFactory.times(1)).cancelSubscription(any(), any());
        });
    }

    @Test
    void badSubscriptionTest() {
        val context = getRequestContext();
        val service = getAdgMppwKafkaService();
        badSubscribeApiMock1();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(0)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
            verify(client, VerificationModeFactory.times(0)).cancelSubscription(any(), any());
        });
    }

    @Test
    void badSubscriptionTest2() {
        val context = getRequestContext();
        badSubscribeApiMock2();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(0)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
            verify(client, VerificationModeFactory.times(0)).cancelSubscription(any(), any());
        });
    }

    @Test
    void badLoadDataTest() {
        val context = getRequestContext();
        badLoadDataApiMock();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(1)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
        });
    }

    @Test
    void badTransferDataTest() {
        val context = getRequestContext();
        badTransferDataApiMock();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(1)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(1)).transferDataToScdTable(any(), any());
        });
    }

    @Test
    void badCancelTest() {
        val context = getRequestContext();
        context.getRequest().setIsLoadStart(false);
        badCancelApiMock();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(0)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(0)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any(), any());
        });
    }

    @Test
    void goodAndBadTransferDataTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context, ar -> assertTrue(ar.succeeded()));
        badTransferDataApiMock();
        service.execute(context, ar -> {
            assertFalse(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(2)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(2)).transferDataToScdTable(any(), any());
        });
    }

    @Test
    void good2TransferDataTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context, ar -> assertTrue(ar.succeeded()));
        service.execute(context, ar -> {
            assertTrue(ar.succeeded());
            verify(client, VerificationModeFactory.times(1)).subscribe(any(), any());
            verify(client, VerificationModeFactory.times(2)).loadData(any(), any());
            verify(client, VerificationModeFactory.times(2)).transferDataToScdTable(any(), any());
        });
    }

    private AdgMppwKafkaService getAdgMppwKafkaService() {
        val tableNamesFactory = new AdgHelperTableNamesFactoryImpl();
        val connectorApiProperties = new AdgConnectorApiProperties();
        connectorApiProperties.setUrl("https://localhost");
        connectorApiProperties.setKafkaLoadDataUrl("/dataload");
        connectorApiProperties.setKafkaSubscriptionUrl("/sbscription");
        connectorApiProperties.setTransferDataToScdTableUrl("/transferDataToScdTablePath");
        val mppwKafkaProperties = new AdgMppwKafkaProperties();
        mppwKafkaProperties.setMaxNumberOfMessagesPerPartition(200);
        return new AdgMppwKafkaService(
                new AdgMppwKafkaContextFactoryImpl(tableNamesFactory),
                client,
                mppwKafkaProperties
        );
    }

    private MppwRequestContext getRequestContext() {
        val queryRequest = new QueryRequest();
        queryRequest.setSystemName("env1");
        queryRequest.setDatamartMnemonic("test");
        val queryLoadParam = new QueryLoadParam();
        queryLoadParam.setDatamart("test");
        queryLoadParam.setDeltaHot(1L);
        queryLoadParam.setTableName("tbl1");
        val mppwRequest = new MppwRequest(
                queryRequest, true, null
                //queryLoadParam,
               // new JsonObject().put("name", "val")
        );
        mppwRequest.setIsLoadStart(true);//FIXME
        //mppwRequest.setTopic("topic1");
        return new MppwRequestContext(mppwRequest);
    }

    private void badSubscribeApiMock1() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture("subscribe error"));
            return null;
        }).when(client).subscribe(any(), any());
    }

    private void badSubscribeApiMock2() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new TtKafkaError("error", "connector error")));
            return null;
        }).when(client).subscribe(any(), any());
    }

    private void badLoadDataApiMock() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).subscribe(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<TtLoadDataKafkaResponse>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new TtKafkaError("error", "connector error")));
            return null;
        }).when(client).loadData(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).cancelSubscription(any(), any());
    }

    private void badTransferDataApiMock() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).subscribe(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<TtLoadDataKafkaResponse>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(new TtLoadDataKafkaResponse(100L)));
            return null;
        }).when(client).loadData(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture("transferDataToScdTable error"));
            return null;
        }).when(client).transferDataToScdTable(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).cancelSubscription(any(), any());
    }

    private void badCancelApiMock() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.failedFuture(new TtKafkaError("error", "connector error")));
            return null;
        }).when(client).cancelSubscription(any(), any());
    }

    private void allGoodApiMock() {
        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).subscribe(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<TtLoadDataKafkaResponse>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture(new TtLoadDataKafkaResponse(100L)));
            return null;
        }).when(client).loadData(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).transferDataToScdTable(any(), any());

        doAnswer(invocation -> {
            Handler<AsyncResult<Void>> handler = invocation.getArgument(1);
            handler.handle(Future.succeededFuture());
            return null;
        }).when(client).cancelSubscription(any(), any());
    }

}
