package io.arenadata.dtm.query.execution.plugin.adp.calcite.service;

import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adpCalciteDMLQueryParserService")
public class AdpCalciteDMLQueryParserService extends CalciteDMLQueryParserService {

    @Autowired
    public AdpCalciteDMLQueryParserService(
            @Qualifier("adpCalciteContextProvider") CalciteContextProvider contextProvider,
            @Qualifier("coreVertx") Vertx vertx) {
        super(contextProvider, vertx);
    }
}
