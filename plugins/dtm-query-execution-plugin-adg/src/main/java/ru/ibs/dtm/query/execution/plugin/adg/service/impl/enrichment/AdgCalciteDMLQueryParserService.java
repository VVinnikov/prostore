package ru.ibs.dtm.query.execution.plugin.adg.service.impl.enrichment;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;

@Service("adgCalciteDMLQueryParserService")
public class AdgCalciteDMLQueryParserService extends CalciteDMLQueryParserService {
    public AdgCalciteDMLQueryParserService(
            @Qualifier("adgCalciteContextProvider") CalciteContextProvider contextProvider,
            @Qualifier("coreVertx") Vertx vertx
    ) {
        super(contextProvider, vertx);
    }
}
