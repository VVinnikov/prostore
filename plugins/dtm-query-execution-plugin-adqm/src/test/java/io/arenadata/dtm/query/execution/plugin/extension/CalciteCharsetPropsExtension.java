package io.arenadata.dtm.query.execution.plugin.extension;

import org.apache.calcite.util.ConversionUtil;
import org.junit.jupiter.api.extension.Extension;

import java.nio.charset.Charset;

public class CalciteCharsetPropsExtension implements Extension {
    private static final Charset DEFAULT_CHARSET = Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME);

    static {
        System.setProperty("saffron.default.charset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.nationalcharset", DEFAULT_CHARSET.name());
        System.setProperty("saffron.default.collation.name", String.format("%s$en_US", DEFAULT_CHARSET.name()));
    }
}
