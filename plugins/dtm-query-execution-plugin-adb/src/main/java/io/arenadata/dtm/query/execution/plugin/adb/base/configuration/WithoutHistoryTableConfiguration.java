package io.arenadata.dtm.query.execution.plugin.adb.base.configuration;

import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataByHashFieldValueFactory;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataQueryFactory;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.impl.AdbCheckDataWithoutHistoryFactory;
import io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.TruncateHistoryDeleteQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.impl.TruncateHistoryDeleteQueriesWithoutHistoryFactory;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl.AdbDmlQueryExtendServiceWithActualTableOnly;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl.MppwWithoutHistoryTableRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.factory.RollbackWithoutHistoryTableRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "adb.with-history-table", havingValue = "false", matchIfMissing = true)
public class WithoutHistoryTableConfiguration {

    public WithoutHistoryTableConfiguration() {
        log.info("Without history table");
    }

    @Bean
    public RollbackRequestFactory<AdbRollbackRequest> adbRollbackRequestFactory() {
        return new RollbackWithoutHistoryTableRequestFactory();
    }

    @Bean
    public MppwRequestFactory<AdbKafkaMppwTransferRequest> adbMppwRequestFactory() {
        return new MppwWithoutHistoryTableRequestFactory();
    }

    @Bean
    public QueryExtendService adbDmlExtendService() {
        return new AdbDmlQueryExtendServiceWithActualTableOnly();
    }

    @Bean
    public AdbCheckDataQueryFactory adbCheckDataFactory(AdbCheckDataByHashFieldValueFactory checkDataByHashFieldValueFactory) {
        return new AdbCheckDataWithoutHistoryFactory(checkDataByHashFieldValueFactory);
    }

    @Bean
    public TruncateHistoryDeleteQueriesFactory adbTruncateHistoryQueryFactory(@Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        return new TruncateHistoryDeleteQueriesWithoutHistoryFactory(sqlDialect);
    }
}
