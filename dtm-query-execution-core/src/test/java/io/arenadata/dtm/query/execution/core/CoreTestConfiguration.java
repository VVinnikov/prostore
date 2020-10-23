package io.arenadata.dtm.query.execution.core;

import io.arenadata.dtm.query.execution.core.utils.BeanNameGenerator;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.admin.SpringApplicationAdminJmxAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;

/**
 * Отделяем основной контекст CORE от плагинов делая его независимым для тестирования
 */
@Profile("test")
@EnableAutoConfiguration(exclude = {SpringApplicationAdminJmxAutoConfiguration.class})
@ComponentScan(basePackages = {
        "io.arenadata.dtm.query.execution.core.calcite",
        "io.arenadata.dtm.query.execution.core.transformer",
        "io.arenadata.dtm.query.execution.core.utils",
        "io.arenadata.dtm.query.execution.core.service",
        "io.arenadata.dtm.query.execution.core.dao",
        "io.arenadata.dtm.query.execution.core.registry",
        "io.arenadata.dtm.query.execution.core.factory",
        "io.arenadata.dtm.query.execution.core.configuration",
        "io.arenadata.dtm.kafka.core.configuration"}, nameGenerator = BeanNameGenerator.class)
public class CoreTestConfiguration {
}
