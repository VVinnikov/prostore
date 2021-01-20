package io.arenadata.dtm.query.execution.plugin.adg.service.impl.mppw;

import io.arenadata.dtm.common.dto.KafkaBrokerInfo;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.AdgConnectorApiProperties;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.AdgMppwKafkaProperties;
import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgHelperTableNamesFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adg.factory.impl.AdgMppwKafkaContextFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.AdgCartridgeError;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.TtLoadDataKafkaResponse;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.arenadata.dtm.query.execution.plugin.api.exception.DataSourceException;
import io.arenadata.dtm.query.execution.plugin.api.mppw.MppwRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaParameter;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.MppwKafkaRequest;
import io.arenadata.dtm.query.execution.plugin.api.mppw.kafka.UploadExternalEntityMetadata;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.internal.verification.VerificationModeFactory;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AdgMppwKafkaServiceTest {

    private final AdgCartridgeClient client = mock(AdgCartridgeClient.class);
    private final AdgMppwKafkaService service = getAdgMppwKafkaService();

    @BeforeEach
    public void before() {
        Mockito.clearInvocations(client);
    }

    @Test
    void allGoodInitTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).cancelSubscription(any());
                });
    }

    @Test
    void allGoodCancelTest() {
        val context = getRequestContext();
        context.setIsLoadStart(false);
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(client, VerificationModeFactory.times(0)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).loadData(any());
                    verify(client, VerificationModeFactory.times(1)).cancelSubscription(any());
                });
    }

    @Test
    void badSubscriptionTest() {
        val context = getRequestContext();
        val service = getAdgMppwKafkaService();
        badSubscribeApiMock1();
        service.execute(context)
                .onComplete(ar -> {
                    assertFalse(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).loadData(any());
                    verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any());
                    verify(client, VerificationModeFactory.times(0)).cancelSubscription(any());
                });
    }

    @Test
    void badSubscriptionTest2() {
        val context = getRequestContext();
        badSubscribeApiMock2();
        service.execute(context)
                .onComplete(ar -> {
                    assertFalse(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).loadData(any());
                    verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any());
                    verify(client, VerificationModeFactory.times(0)).cancelSubscription(any());
                });
    }

    @Test
    void badLoadDataTest() {
        val context = getRequestContext();
        badLoadDataApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertEquals(ar.result(), QueryResult.emptyResult());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).transferDataToScdTable(any());
                });
    }

    @Test
    void badTransferDataTest() {
        val context = getRequestContext();
        badTransferDataApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertEquals(ar.result(), QueryResult.emptyResult());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                });
    }

    @Test
    void badCancelTest() {
        val context = getRequestContext();
        context.setIsLoadStart(false);
        badCancelApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertFalse(ar.succeeded());
                    verify(client, VerificationModeFactory.times(0)).subscribe(any());
                    verify(client, VerificationModeFactory.times(0)).loadData(any());
                });
    }

    @Test
    void goodAndBadTransferDataTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        badTransferDataApiMock();
        service.execute(context)
                .onComplete(ar -> {
                    assertFalse(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(1)).transferDataToScdTable(any());
                });
    }

    @Test
    void good2TransferDataTest() {
        val context = getRequestContext();
        allGoodApiMock();
        service.execute(context)
                .onComplete(ar -> assertTrue(ar.succeeded()));
        service.execute(context)
                .onComplete(ar -> {
                    assertTrue(ar.succeeded());
                    verify(client, VerificationModeFactory.times(1)).subscribe(any());
                    verify(client, VerificationModeFactory.times(1)).transferDataToScdTable(any());
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

    private MppwRequest getRequestContext() {
        return MppwKafkaRequest.builder()
                .envName("env1")
                .datamartMnemonic("test")
                .isLoadStart(true)
                .sysCn(1L)
                .destinationTableName("tbl1")
                .uploadMetadata(UploadExternalEntityMetadata.builder()
                        .name("ext_tab")
                        .externalSchema(getExternalTableSchema())
                        .uploadMessageLimit(1000)
                        .locationPath("kafka://kafka-1.dtm.local:9092/topic")
                        .format(Format.AVRO)
                        .build())
                .brokers(Collections.singletonList(new KafkaBrokerInfo("kafka.host", 9092)))
                .topic("topic1")
                .build();
    }

    private String getExternalTableSchema() {
        return "{\"type\":\"record\",\"name\":\"accounts\",\"namespace\":\"dm2\",\"fields\":[{\"name\":\"column1\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"column2\",\"type\":[\"null\",\"long\"],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"column3\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null,\"defaultValue\":\"null\"},{\"name\":\"sys_op\",\"type\":\"int\",\"default\":0}]}";
    }

    private void badSubscribeApiMock1() {
        when(client.subscribe(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("subscribe error")));
    }

    private void badSubscribeApiMock2() {
        when(client.subscribe(any()))
                .thenReturn(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
    }

    private void badLoadDataApiMock() {
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());
        when(client.loadData(any()))
                .thenReturn(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
        when(client.cancelSubscription(any())).thenReturn(Future.succeededFuture());
    }

    private void badTransferDataApiMock() {
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());
        when(client.loadData(any()))
                .thenReturn(Future.succeededFuture(new TtLoadDataKafkaResponse(100L)));
        when(client.transferDataToScdTable(any()))
                .thenReturn(Future.failedFuture(new DataSourceException("transferDataToScdTable error")));
        when(client.cancelSubscription(any())).thenReturn(Future.succeededFuture());
    }

    private void badCancelApiMock() {
        when(client.cancelSubscription(any()))
                .thenReturn(Future.failedFuture(new AdgCartridgeError("error", "connector error")));
    }

    private void allGoodApiMock() {
        when(client.subscribe(any())).thenReturn(Future.succeededFuture());
        when(client.loadData(any())).thenReturn(Future.succeededFuture(new TtLoadDataKafkaResponse(100L)));
        when(client.transferDataToScdTable(any())).thenReturn(Future.succeededFuture());
        when(client.cancelSubscription(any())).thenReturn(Future.succeededFuture());
    }

}
