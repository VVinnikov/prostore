package io.arenadata.dtm.jdbc.util;

import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.Properties;

import static io.arenadata.dtm.jdbc.util.DriverConstants.*;

@Slf4j
public class UrlConnectionParser {

    public static Properties parseURL(String url, Properties info) {
        Properties urlProperties = new Properties(info);
        if (!url.startsWith(CONNECT_URL_PREFIX)) {
            return null;
        }
        String urlServer = "http://" + url.replaceFirst(CONNECT_URL_PREFIX, "");
        URI uri = URI.create(urlServer);

        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            log.error("JDBC URL must contain the host and port db: {}", url);
            return null;
        }
        int port = uri.getPort();
        if (port < 1 || port > 65535) {
            log.error("JDBC URL port: {} not valid (1:65535) ", port);
            return null;
        } else {
            host += ":" + uri.getPort();
        }

        urlProperties.setProperty(SCHEMA_PROPERTY, getSchema(uri));
        urlProperties.setProperty(HOST_PROPERTY, host);

        String query = uri.getQuery();
        if (query != null) {
            String[] args = query.split("&");
            for (String token : args) {
                if (token.isEmpty()) {
                    continue;
                }
                int pos = token.indexOf('=');
                if (pos == - 1) {
                    urlProperties.setProperty(token, "");
                } else {
                    urlProperties.setProperty(token.substring(0, pos), token.substring(pos + 1));
                }
            }
        }
        return urlProperties;
    }

    private static String getSchema(URI uri) {
        String path = uri.getPath();
        return path == null || path.isEmpty() ? "" : path.substring(1);
    }
}
