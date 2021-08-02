package io.arenadata.dtm.query.execution.plugin.adp.mppr.configuration;

import io.arenadata.dtm.query.execution.plugin.adp.mppr.AdpMpprExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MpprConfiguration {

    @Bean("adpMpprService")
    public MpprService mpprService(List<AdpMpprExecutor> executors) {
        return new MpprServiceImpl<>(executors);
    }
}
