package io.arenadata.dtm.query.execution.plugin.adp.check;

import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.query.execution.plugin.adp.check.service.AdpCheckVersionService;
import io.arenadata.dtm.query.execution.plugin.adp.connector.service.AdpConnectorClient;
import io.arenadata.dtm.query.execution.plugin.adp.db.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.check.CheckVersionRequest;
import io.vertx.core.Future;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.*;

import static io.arenadata.dtm.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory.COMPONENT_NAME_COLUMN;
import static io.arenadata.dtm.query.execution.plugin.adp.check.factory.AdpVersionQueriesFactory.VERSION_COLUMN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class AdpCheckVersionServiceTest {

    private final DatabaseExecutor databaseExecutor = mock(DatabaseExecutor.class);
    private final AdpConnectorClient connectorClient = mock(AdpConnectorClient.class);
    private final AdpCheckVersionService checkVersionService = new AdpCheckVersionService(databaseExecutor, connectorClient);
    private final CheckVersionRequest request = new CheckVersionRequest(UUID.randomUUID(),"env", "dtm");

    @Test
    void checkVersionSuccess() {
        val adpComponentName = "ADP";
        val adpComponentVersion = "ADP version";
        val expectedAdpVersionInfo = new VersionInfo(adpComponentName, adpComponentVersion);
        val mpprComponentName = "MPPR Connector";
        val mpprComponentVersion = "MPPR version";
        val expectedMpprVersionInfo = new VersionInfo(mpprComponentName, mpprComponentVersion);
        val mppwComponentName = "MPPW Connector";
        val mppwComponentVersion = "MPPW version";
        val expectedMppwVersionInfo = new VersionInfo(mppwComponentName, mppwComponentVersion);
        Map<String, Object> row = new HashMap<>();
        row.put(COMPONENT_NAME_COLUMN, adpComponentName);
        row.put(VERSION_COLUMN, adpComponentVersion);
        when(databaseExecutor.execute(anyString(), any())).thenReturn(Future.succeededFuture(Collections.singletonList(row)));
        when(connectorClient.getMpprVersion()).thenReturn(Future.succeededFuture(Collections.singletonList(expectedMpprVersionInfo)));
        when(connectorClient.getMppwVersion()).thenReturn(Future.succeededFuture(Collections.singletonList(expectedMppwVersionInfo)));

        checkVersionService.checkVersion(request)
                .onComplete(result -> {
                    assertTrue(result.succeeded());
                    val versions = result.result();
                    assertThat(expectedAdpVersionInfo).isEqualToComparingFieldByField(versions.get(0));
                    assertThat(expectedMpprVersionInfo).isEqualToComparingFieldByField(versions.get(1));
                    assertThat(expectedMppwVersionInfo).isEqualToComparingFieldByField(versions.get(2));
                });
        verify(databaseExecutor).execute(anyString(), any());
        verify(connectorClient).getMpprVersion();
        verify(connectorClient).getMppwVersion();
    }

    @Test
    void checkVersionAdpVersionError() {
        testWithError("receiving ADP version failed", 0);
    }

    @Test
    void checkVersionMpprVersionError() {
        testWithError("receiving MPPR version failed", 1);
    }

    @Test
    void checkVersionMppwVersionError() {
        testWithError("receiving MPPW version failed", 2);
    }

    private void testWithError(String error, int failedMockNumber) {
        Future<List<Map<String, Object>>> adpMockResult = Future.succeededFuture(new ArrayList<>());
        Future<List<VersionInfo>> mpprMockResult = Future.succeededFuture(new ArrayList<>());
        Future<List<VersionInfo>> mppwMockResult = Future.succeededFuture(new ArrayList<>());

        switch (failedMockNumber) {
            case 0:
                adpMockResult = Future.failedFuture(error);
                break;
            case 1:
                mpprMockResult = Future.failedFuture(error);
                break;
            default:
                mppwMockResult = Future.failedFuture(error);
                break;
        }

        when(databaseExecutor.execute(anyString(), any())).thenReturn(adpMockResult);
        when(connectorClient.getMpprVersion()).thenReturn(mpprMockResult);
        when(connectorClient.getMppwVersion()).thenReturn(mppwMockResult);

        checkVersionService.checkVersion(request)
                .onComplete(result -> {
                    assertTrue(result.failed());
                    assertEquals(error, result.cause().getMessage());
                });
        verify(databaseExecutor).execute(anyString(), any());
        verify(connectorClient).getMpprVersion();
        verify(connectorClient).getMppwVersion();
    }
}
