package ru.ibs.dtm.jdbc.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import ru.ibs.dtm.common.model.ddl.SystemMetadata;
import ru.ibs.dtm.jdbc.core.QueryRequest;
import ru.ibs.dtm.jdbc.core.QueryResult;
import ru.ibs.dtm.jdbc.model.ColumnInfo;
import ru.ibs.dtm.jdbc.model.SchemaInfo;
import ru.ibs.dtm.jdbc.model.TableInfo;
import ru.ibs.dtm.jdbc.util.DtmException;
import ru.ibs.dtm.jdbc.util.ResponseException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.apache.http.util.TextUtils.isEmpty;
import static ru.ibs.dtm.jdbc.util.DriverConstants.HOST_PROPERTY;

/**
 * Контроллер взаимодействия с Сервисом чтения по REST
 */
@Slf4j
public class CoordinatorReaderService implements Protocol {

    private static final String GET_META_URL = "/meta";
    private static final String GET_ENTITIES_URL = "/meta/%s/entities";
    private static final String GET_ATTRIBUTES_URL = "/meta/%s/entity/%s/attributes";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final CloseableHttpClient client;
    private final String backendHostUrl;
    private String schema;

    @SneakyThrows
    public CoordinatorReaderService(CloseableHttpClient client, String dbHost, String schema) {
        if (isEmpty(dbHost)) {
            throw new DtmException(String.format("Unable to create connection because parameter '%s' is not specified", HOST_PROPERTY));
        }
        this.backendHostUrl = "http://" + dbHost;
        this.schema = schema;
        this.client = client;
    }

    @Override
    public List<SchemaInfo> getDatabaseSchemas() {
        try {
            HttpGet httpGet = new HttpGet(backendHostUrl + GET_META_URL);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();
                return MAPPER.readValue(content, new TypeReference<List<SchemaInfo>>() {
                });
            }
        } catch (IOException e) {
            log.error("Error loading database schemas.", e.getCause());
        }

        return Collections.emptyList();
    }

    @Override
    public List<TableInfo> getDatabaseTables(String schemaPattern) {
        try {
            String uri = String.format(backendHostUrl + GET_ENTITIES_URL, schemaPattern);
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();

                return MAPPER.readValue(content, new TypeReference<List<TableInfo>>() {
                });
            }
        } catch (IOException e) {
            log.error("Error loading schema tables {}", schemaPattern, e.getCause());
        }

        return Collections.emptyList();
    }

    @Override
    public List<ColumnInfo> getDatabaseColumns(String schema, String tableName) {
        try {
            String uri = String.format(backendHostUrl + GET_ATTRIBUTES_URL, schema, tableName);
            HttpGet httpGet = new HttpGet(uri);
            try (CloseableHttpResponse response = client.execute(httpGet)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();
                return MAPPER.readValue(content, new TypeReference<List<ColumnInfo>>() {
                });
            }
        } catch (IOException e) {
            log.error("Error loading columns of table {} schema {}", tableName, schema, e.getCause());
        }

        return Collections.emptyList();
    }

    @Override
    public QueryResult executeQuery(String sql) throws SQLException {
        try {
            HttpPost httpPost = new HttpPost(backendHostUrl + "/query/execute");
            QueryRequest queryRequest = prepareQueryRequest(sql);
            String queryRequestJson = MAPPER.writeValueAsString(queryRequest);
            log.debug("Preparing the query query [{}]", queryRequestJson);
            httpPost.setEntity(new StringEntity(queryRequestJson, ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse response = client.execute(httpPost)) {
                checkResponseStatus(response);
                InputStream content = response.getEntity().getContent();
                QueryResult result = MAPPER.readValue(content, new TypeReference<QueryResult>() {
                });
                setUsedSchemaIfExists(result);
                log.info("Request received response {}", result);
                return result;
            }
        } catch (Exception e) {
            String errMsg = String.format("Error executing query [%s]: %s", sql, e.getMessage());
            log.error(errMsg, e);
            throw new SQLException(errMsg, e);
        }
    }

    private void setUsedSchemaIfExists(QueryResult result) throws DtmException {
        if (result.getMetadata() != null && result.getMetadata().size() == 1
                && SystemMetadata.SCHEMA == result.getMetadata().get(0).getSystemMetadata()) {
            if (!result.isEmpty()) {
                final Optional<Object> schemaOptional = result.getResult().get(0).values().stream().findFirst();
                if (schemaOptional.isPresent()) {
                    this.schema = schemaOptional.get().toString();
                } else {
                    throw new DtmException("Schema value not found!");
                }
            } else {
                throw new DtmException("Empty result for using schema!");
            }
        }
    }

    @SneakyThrows
    private void checkResponseStatus(CloseableHttpResponse response) {
        if (HttpStatus.SC_OK != response.getStatusLine().getStatusCode()) {
            try {
                String res = MAPPER.readValue(response.getEntity().getContent(), ResponseException.class)
                        .getExceptionMessage();
                log.error("The system returned an unsuccessful response: {}", res);
                throw new DtmException(res != null && !res.isEmpty() ? res :
                        String.format("The system returned an unsuccessful response: %s", response.getStatusLine().getReasonPhrase()));
            } catch (DtmException e) {
                throw e;
            } catch (Exception e) {
                log.error("The system returned an unsuccessful response: {}", response.getStatusLine().getReasonPhrase());
                throw new DtmException(String.format("The system returned an unsuccessful response:%s", response.getStatusLine().getReasonPhrase()));
            }
        }
    }

    private QueryRequest prepareQueryRequest(String sql) {
        String schema = this.schema;
        QueryRequest queryRequest = new QueryRequest(schema, sql);
        log.info("Sql query generated {}", queryRequest);
        return queryRequest;
    }
}
