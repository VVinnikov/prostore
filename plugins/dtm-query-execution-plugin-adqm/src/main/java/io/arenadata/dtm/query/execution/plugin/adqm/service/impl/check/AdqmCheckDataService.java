package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.check;

import io.arenadata.dtm.query.execution.plugin.api.check.CheckContext;
import io.arenadata.dtm.query.execution.plugin.api.service.check.CheckDataService;
import io.vertx.core.Future;
import org.springframework.stereotype.Service;

@Service("adqmCheckDataService")
public class AdqmCheckDataService implements CheckDataService {
    @Override
    public Future<Long> checkDataByCount(CheckContext context) {
        return null;
    }

    @Override
    public Future<Long> checkDataByHashInt32(CheckContext context) {
        return null;
    }
}
