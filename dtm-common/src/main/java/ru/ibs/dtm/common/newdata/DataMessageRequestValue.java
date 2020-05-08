package ru.ibs.dtm.common.newdata;

public class DataMessageRequestValue {
    String jsonSchema;
    String body;

    public DataMessageRequestValue() {
    }

    public DataMessageRequestValue(String metaData, String body) {
        this.jsonSchema = metaData;
        this.body = body;
    }

    public String getSchema() {
        return jsonSchema;
    }

    public void setSchema(String metaData) {
        this.jsonSchema = metaData;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
