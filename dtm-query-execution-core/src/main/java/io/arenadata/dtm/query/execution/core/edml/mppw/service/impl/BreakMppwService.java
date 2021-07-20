package io.arenadata.dtm.query.execution.core.edml.mppw.service.impl;

import io.arenadata.dtm.query.execution.core.delta.dto.request.BreakMppwRequest;
import io.vertx.core.impl.ConcurrentHashSet;
import org.springframework.stereotype.Service;

import java.util.Set;

@Service
public class BreakMppwService {

    private final Set<BreakMppwRequest> breakMppwRequests = new ConcurrentHashSet<>();

    public void breakMppw(String datamart, long sysCn) {
        BreakMppwRequest request = BreakMppwRequest
                .builder()
                .datamart(datamart)
                .sysCn(sysCn)
                .build();
        breakMppwRequests.add(request);
    }

    public boolean shouldBreakMppw(String datamart, long sysCn) {
        BreakMppwRequest request = BreakMppwRequest
                .builder()
                .datamart(datamart)
                .sysCn(sysCn)
                .build();
        return breakMppwRequests.remove(request);
    }

}
