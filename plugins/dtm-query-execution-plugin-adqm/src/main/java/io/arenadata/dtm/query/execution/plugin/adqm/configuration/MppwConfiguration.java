package io.arenadata.dtm.query.execution.plugin.adqm.configuration;

import io.arenadata.dtm.query.execution.plugin.adqm.service.AdqmMppwExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MppwConfiguration {

    @Bean("adqmMppwService")
    public MppwService mpprService(List<AdqmMppwExecutor> executors) {
        return new MppwServiceImpl<>(executors);
    }
}
