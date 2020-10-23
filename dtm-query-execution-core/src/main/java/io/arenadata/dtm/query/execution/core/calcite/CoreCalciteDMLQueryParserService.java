package io.arenadata.dtm.query.execution.core.calcite;

import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("coreCalciteDMLQueryParserService")
public class CoreCalciteDMLQueryParserService extends CalciteDMLQueryParserService {
    public CoreCalciteDMLQueryParserService(
            @Qualifier("coreCalciteContextProvider") CalciteContextProvider contextProvider,
            @Qualifier("coreVertx") Vertx vertx
    ) {
        super(contextProvider, vertx);
    }
}
