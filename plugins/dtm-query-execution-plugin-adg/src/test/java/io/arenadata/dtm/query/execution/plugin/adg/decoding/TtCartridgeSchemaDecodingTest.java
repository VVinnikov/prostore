package io.arenadata.dtm.query.execution.plugin.adg.decoding;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import lombok.val;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

public class TtCartridgeSchemaDecodingTest {

    ObjectMapper yamlMapper = new AppConfiguration().yamlMapper();

    @Test
    void testEmptySchemaDecode() {
        try {
            val yaml = yamlMapper.readValue("spaces: []", OperationYaml.class);
        } catch (JsonProcessingException e) {
            fail(e);
        }
    }
}
