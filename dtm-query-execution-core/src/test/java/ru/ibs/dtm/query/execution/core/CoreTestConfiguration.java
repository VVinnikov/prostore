package ru.ibs.dtm.query.execution.core;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.admin.SpringApplicationAdminJmxAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;
import ru.ibs.dtm.query.execution.core.utils.BeanNameGenerator;

/**
 * Отделяем основной контекст CORE от плагинов делая его независимым для тестирования
 */
@Profile("test")
@EnableAutoConfiguration(exclude = {SpringApplicationAdminJmxAutoConfiguration.class})
@ComponentScan(basePackages = {
        "ru.ibs.dtm.query.execution.core.calcite",
        "ru.ibs.dtm.query.execution.core.transformer",
        "ru.ibs.dtm.query.execution.core.utils",
        "ru.ibs.dtm.query.execution.core.service",
        "ru.ibs.dtm.query.execution.core.dao",
        "ru.ibs.dtm.query.execution.core.registry",
        "ru.ibs.dtm.query.execution.core.factory",
        "ru.ibs.dtm.query.execution.core.configuration",
        "ru.ibs.dtm.kafka.core.configuration"}, nameGenerator = BeanNameGenerator.class)
public class CoreTestConfiguration {
}
