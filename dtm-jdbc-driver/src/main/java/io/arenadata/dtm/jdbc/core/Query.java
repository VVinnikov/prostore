package io.arenadata.dtm.jdbc.core;

public class Query {

    private final String nativeSql;
    private final boolean isBlank;

    public Query(String nativeSql, boolean isBlank) {
        this.nativeSql = nativeSql;
        this.isBlank = isBlank;
    }

    public String getNativeSql() {
        return nativeSql;
    }

    public boolean isBlank() {
        return isBlank;
    }

}
