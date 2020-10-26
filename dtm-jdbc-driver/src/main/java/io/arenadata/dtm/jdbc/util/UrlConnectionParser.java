package io.arenadata.dtm.jdbc.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Optional;
import java.util.Properties;

import static io.arenadata.dtm.jdbc.util.DriverConstants.*;

public class UrlConnectionParser {
    private static final Logger LOGGER = LoggerFactory.getLogger(UrlConnectionParser.class);

    public static void parseURL(String url, Properties info) {

        String urlServer = "http://" + url.replaceFirst(CONNECT_URL_PREFIX, "");
        URI uri = URI.create(urlServer);
        String host = getHost(uri)
                .orElseThrow(() -> new IllegalArgumentException("Jdbc url must contain the host and port db: " + url));
        info.setProperty(SCHEMA_PROPERTY, getSchema(uri));
        info.setProperty(HOST_PROPERTY, host);

        String query = uri.getQuery();
        if (query != null) {
            String[] args = query.split("&");
            for (String token : args) {
                if (token.isEmpty()) {
                    continue;
                }
                int pos = token.indexOf('=');
                if (pos == - 1) {
                    info.setProperty(token, "");
                } else {
                    info.setProperty(token.substring(0, pos), token.substring(pos + 1));
                }
            }
        }
    }

    private static Optional<String> getHost(URI uri) {
        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            return Optional.empty();
        }
        if (uri.getPort() != - 1) {
            host += ":" + uri.getPort();
        }
        return Optional.of(host);
    }

    private static String getSchema(URI uri) {
        String path = uri.getPath();
        return path == null || path.isEmpty() ? "" : path.substring(1);
    }
}
