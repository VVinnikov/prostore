package io.arenadata.dtm.common.dto;

import java.util.List;

/*Запрос на перенос данных в историческую таблицу*/
public class HistoryTransferRequest {
    private int deltaOk;
    private int deltaHot;
    private String tableName;
    private String datemartMnemonic;
    private String requestId;
    private List<String> columns;

    public HistoryTransferRequest() {
    }

    public HistoryTransferRequest(int deltaOk, int deltaHot, String tableName, String datemartMnemonic, String requestId, List<String> columns) {
        this.deltaOk = deltaOk;
        this.deltaHot = deltaHot;
        this.tableName = tableName;
        this.datemartMnemonic = datemartMnemonic;
        this.requestId = requestId;
        this.columns = columns;
    }

    public int getDeltaOk() {
        return deltaOk;
    }

    public void setDeltaOk(int deltaOk) {
        this.deltaOk = deltaOk;
    }

    public int getDeltaHot() {
        return deltaHot;
    }

    public void setDeltaHot(int deltaHot) {
        this.deltaHot = deltaHot;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatemartMnemonic() {
        return datemartMnemonic;
    }

    public void setDatemartMnemonic(String datemartMnemonic) {
        this.datemartMnemonic = datemartMnemonic;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}