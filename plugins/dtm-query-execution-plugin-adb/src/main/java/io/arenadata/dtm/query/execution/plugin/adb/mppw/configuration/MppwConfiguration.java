package io.arenadata.dtm.query.execution.plugin.adb.mppw.configuration;

import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MppwConfiguration {

    @Bean("adbMppwService")
    public MppwService mpprService(List<AdbMppwExecutor> executors) {
        return new MppwServiceImpl<>(executors);
    }
}
