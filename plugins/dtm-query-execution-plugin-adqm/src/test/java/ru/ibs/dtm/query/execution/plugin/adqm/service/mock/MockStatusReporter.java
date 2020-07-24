package ru.ibs.dtm.query.execution.plugin.adqm.service.mock;

import io.vertx.core.json.JsonObject;
import lombok.NonNull;
import org.junit.jupiter.api.Assertions;
import ru.ibs.dtm.query.execution.plugin.adqm.service.StatusReporter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MockStatusReporter implements StatusReporter {
    private final Map<String, JsonObject> expectedPayloads;
    private final Set<String> calls = new HashSet<>();

    public MockStatusReporter(@NonNull final Map<String, JsonObject> expectedPayloads) {
        this.expectedPayloads = new HashMap<>(expectedPayloads);
    }

    @Override
    public void onStart(JsonObject payload) {
        calls.add("start");
        JsonObject expectedPayload = expectedPayloads.get("start");
        assertEquals(expectedPayload, payload);
    }

    @Override
    public void onFinish(JsonObject payload) {
        calls.add("finish");
        JsonObject expectedPayload = expectedPayloads.get("finish");
        assertEquals(expectedPayload, payload);
    }

    @Override
    public void onError(JsonObject payload) {
        calls.add("error");
        JsonObject expectedPayload = expectedPayloads.get("error");
        assertEquals(expectedPayload, payload);
    }

    public boolean wasCalled(String action) {
        return calls.contains(action);
    }
}
