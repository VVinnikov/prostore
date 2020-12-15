package io.arenadata.dtm.common.dto;

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
