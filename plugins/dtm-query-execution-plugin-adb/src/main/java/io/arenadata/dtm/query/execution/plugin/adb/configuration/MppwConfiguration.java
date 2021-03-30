package io.arenadata.dtm.query.execution.plugin.adb.configuration;

import io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppw.AdbMppwExecutor;
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
