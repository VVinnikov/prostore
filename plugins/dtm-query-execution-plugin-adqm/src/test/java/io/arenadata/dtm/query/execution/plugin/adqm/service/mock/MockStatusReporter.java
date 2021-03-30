package io.arenadata.dtm.query.execution.plugin.adqm.service.mock;

import io.arenadata.dtm.query.execution.plugin.adqm.dto.StatusReportDto;
import io.arenadata.dtm.query.execution.plugin.adqm.service.StatusReporter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MockStatusReporter implements StatusReporter {
    private final Map<String, StatusReportDto> expectedPayloads;
    private final Set<String> calls = new HashSet<>();

    public MockStatusReporter(@NonNull final Map<String, StatusReportDto> expectedPayloads) {
        this.expectedPayloads = new HashMap<>(expectedPayloads);
    }

    @Override
    public void onStart(StatusReportDto payload) {
        calls.add("start");
        StatusReportDto expectedPayload = expectedPayloads.get("start");
        assertEquals(expectedPayload, payload);
    }

    @Override
    public void onFinish(StatusReportDto payload) {
        calls.add("finish");
        StatusReportDto expectedPayload = expectedPayloads.get("finish");
        assertEquals(expectedPayload, payload);
    }

    @Override
    public void onError(StatusReportDto payload) {
        calls.add("error");
        StatusReportDto expectedPayload = expectedPayloads.get("error");
        assertEquals(expectedPayload, payload);
    }

    public boolean wasCalled(String action) {
        return calls.contains(action);
    }
}
