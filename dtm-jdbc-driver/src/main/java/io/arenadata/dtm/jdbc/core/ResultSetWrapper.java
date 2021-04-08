package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.jdbc.ext.DtmResultSet;

public class ResultSetWrapper {
    private final DtmResultSet resultSet;
    private final long updateCount;
    private ResultSetWrapper next;

    public ResultSetWrapper(DtmResultSet resultSet) {
        this.resultSet = resultSet;
        this.updateCount = resultSet.getRowsSize();
    }

    public DtmResultSet getResultSet() {
        return resultSet;
    }

    public ResultSetWrapper getNext() {
        return this.next;
    }

    public void append(ResultSetWrapper newResult) {
        ResultSetWrapper tail = this;
        while (tail.next != null) {
            tail = tail.next;
        }
        tail.next = newResult;
    }

    public long getUpdateCount() {
        return updateCount;
    }
}
