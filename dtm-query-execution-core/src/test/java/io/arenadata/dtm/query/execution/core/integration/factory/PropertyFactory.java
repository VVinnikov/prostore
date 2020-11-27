package io.arenadata.dtm.query.execution.core.integration.factory;

import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.support.EncodedResource;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class PropertyFactory {

    static final YamlPropertySourceFactory factory = new YamlPropertySourceFactory();

    public static PropertySource<?> createPropertySource(String fileName) {
        try (InputStream inputStream = PropertyFactory.class.getClassLoader()
                .getResourceAsStream(fileName)) {
            assert inputStream != null;
            return factory.createPropertySource("core.properties",
                    new EncodedResource(new InputStreamResource(inputStream)));
        } catch (IOException e) {
            log.error("Error in reading properties file", e);
            throw new RuntimeException(e);
        }
    }
}
