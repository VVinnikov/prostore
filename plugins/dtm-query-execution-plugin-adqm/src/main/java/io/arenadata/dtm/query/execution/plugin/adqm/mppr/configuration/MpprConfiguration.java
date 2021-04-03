package io.arenadata.dtm.query.execution.plugin.adqm.mppr.configuration;

import io.arenadata.dtm.query.execution.plugin.adqm.mppr.AdqmMpprExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MpprConfiguration {

    @Bean("adqmMpprService")
    public MpprService mpprService(List<AdqmMpprExecutor> executors) {
        return new MpprServiceImpl<>(executors);
    }
}
