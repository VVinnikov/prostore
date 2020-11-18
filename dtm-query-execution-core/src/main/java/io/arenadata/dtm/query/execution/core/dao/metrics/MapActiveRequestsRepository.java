package io.arenadata.dtm.query.execution.core.dao.metrics;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Repository("mapActiveRequestsRepository")
public class MapActiveRequestsRepository implements ActiveRequestsRepository<RequestMetrics> {

    private final Map<UUID, RequestMetrics> requestsMap = new ConcurrentHashMap<>();

    @Override
    public void add(RequestMetrics request) {
        requestsMap.put(request.getRequestId(), request);
    }

    @Override
    public void remove(RequestMetrics request) {
        requestsMap.remove(request.getRequestId());
    }

    @Override
    public RequestMetrics get(UUID requestId) {
        return requestsMap.get(requestId);
    }

    @Override
    public List<RequestMetrics> getList() {
        return new ArrayList<>(requestsMap.values());
    }

    @Override
    public void deleteAll() {
        requestsMap.clear();
    }
}
