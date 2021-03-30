package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.jdbc.ext.DtmResultSet;

public class ResultSetWrapper {
    private final DtmResultSet resultSet;
    private ResultSetWrapper next;

    public ResultSetWrapper(DtmResultSet resultSet) {
        this.resultSet = resultSet;
    }

    public DtmResultSet getResultSet() {
        return resultSet;
    }

    public ResultSetWrapper getNext() {
        return this.next;
    }

    public void append(ResultSetWrapper newResult) {
        ResultSetWrapper tail;
        for (tail = this; tail.next != null; tail = tail.next) {
        }

        tail.next = newResult;
    }
}
