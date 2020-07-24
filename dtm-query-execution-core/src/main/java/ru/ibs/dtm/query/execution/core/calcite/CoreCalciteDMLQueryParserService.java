package ru.ibs.dtm.query.execution.core.calcite;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;

@Service("coreCalciteDMLQueryParserService")
public class CoreCalciteDMLQueryParserService extends CalciteDMLQueryParserService {
    public CoreCalciteDMLQueryParserService(
            @Qualifier("coreCalciteContextProvider") CalciteContextProvider contextProvider,
            @Qualifier("coreVertx") Vertx vertx
    ) {
        super(contextProvider, vertx);
    }
}
