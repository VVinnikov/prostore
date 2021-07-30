package io.arenadata.dtm.query.execution.plugin.adp.mppw.configuration;

import io.arenadata.dtm.query.execution.plugin.adp.mppw.AdpMppwExecutor;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwService;
import io.arenadata.dtm.query.execution.plugin.api.service.mppw.MppwServiceImpl;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class MppwConfiguration {

    @Bean("adpMppwService")
    public MppwService mpprService(List<AdpMppwExecutor> executors) {
        return new MppwServiceImpl<>(executors);
    }
}
