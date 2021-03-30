package io.arenadata.dtm.query.execution.plugin.adg.configuration;

import io.arenadata.dtm.query.execution.plugin.adg.service.AdgMpprExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppr.MpprServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MpprConfiguration {

    @Bean("adgMpprService")
    public MpprService mpprService(List<AdgMpprExecutor> executors) {
        return new MpprServiceImpl<>(executors);
    }
}
