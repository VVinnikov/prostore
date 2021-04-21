package io.arenadata.dtm.query.execution.plugin.adb.base.configuration;

import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.QueryExtendService;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl.AdbDmlQueryExtendServiceWithHistory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.AdbKafkaMppwTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.MppwRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.factory.impl.MppwWithHistoryTableRequestFactory;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.dto.AdbRollbackRequest;
import io.arenadata.dtm.query.execution.plugin.adb.rollback.factory.RollbackWithHistoryTableRequestFactory;
import io.arenadata.dtm.query.execution.plugin.api.factory.RollbackRequestFactory;
import lombok.extern.slf4j.Slf4j;
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
        return new AdbDmlQueryExtendServiceWithHistory();
    }

}
