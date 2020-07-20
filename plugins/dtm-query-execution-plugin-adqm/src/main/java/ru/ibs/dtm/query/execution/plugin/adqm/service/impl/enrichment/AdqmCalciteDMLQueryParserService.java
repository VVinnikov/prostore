package ru.ibs.dtm.query.execution.plugin.adqm.service.impl.enrichment;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;

@Service("adqmCalciteDMLQueryParserService")
public class AdqmCalciteDMLQueryParserService extends CalciteDMLQueryParserService {
    public AdqmCalciteDMLQueryParserService(
            @Qualifier("adqmCalciteContextProvider") CalciteContextProvider contextProvider,
            @Qualifier("adqmVertx") Vertx vertx
    ) {
        super(contextProvider, vertx);
    }
}
