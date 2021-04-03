package io.arenadata.dtm.query.execution.plugin.adg.mppw.configuration;

import io.arenadata.dtm.query.execution.plugin.adg.mppw.AdgMppwExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MppwConfiguration {

    @Bean("adgMppwService")
    public MppwService mpprService(List<AdgMppwExecutor> executors) {
        return new MppwServiceImpl<>(executors);
    }
}
