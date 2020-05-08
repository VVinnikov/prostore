package ru.ibs.dtm.common.service;

import java.util.List;
import java.util.Set;

/*Запрос на очистку горячих записей*/
public class HotDataRemoveRequest {
    private Set<String> tableList;

    public HotDataRemoveRequest() {
    }

    public HotDataRemoveRequest(Set<String> tableList) {
        this.tableList = tableList;
    }

    public Set<String> getTableList() {
        return tableList;
    }

    public void setTableList(Set<String> tableList) {
        this.tableList = tableList;
    }
}
