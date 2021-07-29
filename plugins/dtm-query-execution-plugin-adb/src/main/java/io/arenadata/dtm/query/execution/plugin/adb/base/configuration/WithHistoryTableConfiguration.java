package io.arenadata.dtm.query.execution.plugin.adb.base.configuration;

import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataByHashFieldValueFactory;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.AdbCheckDataQueryFactory;
import io.arenadata.dtm.query.execution.plugin.adb.check.factory.impl.AdbCheckDataWithHistoryFactory;
import io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.TruncateHistoryDeleteQueriesFactory;
import io.arenadata.dtm.query.execution.plugin.adb.ddl.factory.impl.TruncateHistoryDeleteQueriesWithHistoryFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.AdbDmlQueryExtendWithHistoryService;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl.MppwWithHistoryTableRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.factory.RollbackWithHistoryTableRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlDialect;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "adb.with-history-table", havingValue = "true")
public class WithHistoryTableConfiguration {

    public WithHistoryTableConfiguration() {
        log.info("With history table");
    }

    @Bean
    public RollbackRequestFactory<AdbRollbackRequest> adbRollbackRequestFactory() {
        return new RollbackWithHistoryTableRequestFactory();
    }

    @Bean
    public MppwRequestFactory<AdbKafkaMppwTransferRequest> adbMppwRequestFactory() {
        return new MppwWithHistoryTableRequestFactory();
    }

    @Bean
    public QueryExtendService adbDmlExtendService() {
        return new AdbDmlQueryExtendWithHistoryService();
    }

    @Bean
    public AdbCheckDataQueryFactory adbCheckDataFactory(AdbCheckDataByHashFieldValueFactory checkDataByHashFieldValueFactory) {
        return new AdbCheckDataWithHistoryFactory(checkDataByHashFieldValueFactory);
    }

    @Bean
    public TruncateHistoryDeleteQueriesFactory adbTruncateHistoryQueryFactory(@Qualifier("adbSqlDialect") SqlDialect sqlDialect){
        return new TruncateHistoryDeleteQueriesWithHistoryFactory(sqlDialect);
    }

}
