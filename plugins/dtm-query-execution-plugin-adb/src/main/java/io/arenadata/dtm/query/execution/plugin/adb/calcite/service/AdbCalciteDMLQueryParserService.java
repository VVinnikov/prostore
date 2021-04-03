package io.arenadata.dtm.query.execution.plugin.adb.calcite.service;

import io.arenadata.dtm.query.calcite.core.provider.CalciteContextProvider;
import io.arenadata.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;
import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adbCalciteDMLQueryParserService")
public class AdbCalciteDMLQueryParserService extends CalciteDMLQueryParserService {

    @Autowired
    public AdbCalciteDMLQueryParserService(
            @Qualifier("adbCalciteContextProvider") CalciteContextProvider contextProvider,
            @Qualifier("coreVertx") Vertx vertx) {
        super(contextProvider, vertx);
    }
}
