package ru.ibs.dtm.query.execution.plugin.adb.service.impl.enrichment;

import io.vertx.core.Vertx;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import ru.ibs.dtm.query.calcite.core.provider.CalciteContextProvider;
import ru.ibs.dtm.query.calcite.core.service.impl.CalciteDMLQueryParserService;

@Service("adbCalciteDMLQueryParserService")
public class AdbCalciteDMLQueryParserService extends CalciteDMLQueryParserService {
    public AdbCalciteDMLQueryParserService(
            @Qualifier("adbCalciteContextProvider") CalciteContextProvider contextProvider,
            @Qualifier("coreVertx") Vertx vertx
    ) {
        super(contextProvider, vertx);
    }
}
