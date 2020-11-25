package io.arenadata.dtm.query.execution.core.integration.util;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class FileUtil {

    public static String getFileContent(String fileName) throws IOException {
        try (InputStream inputStream = FileUtil.class.getClassLoader().getResourceAsStream(fileName)) {
            assert inputStream != null;
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }
}
