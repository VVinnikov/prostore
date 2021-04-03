package io.arenadata.dtm.query.execution.plugin.adb.mppr.configuration;

import io.arenadata.dtm.query.execution.plugin.adb.mppr.AdbMpprExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MpprConfiguration {

    @Bean("adbMpprService")
    public MpprService mpprService(List<AdbMpprExecutor> executors) {
        return new MpprServiceImpl<>(executors);
    }
}
