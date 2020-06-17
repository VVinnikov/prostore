package ru.ibs.dtm.common.delta;

public class QueryDeltaResult {

    private String statusDate;
    private Long sinId;

    public QueryDeltaResult(String statusDate, Long sinId) {
        this.statusDate = statusDate;
        this.sinId = sinId;
    }

    public String getStatusDate() {
        return statusDate;
    }

    public void setStatusDate(String statusDate) {
        this.statusDate = statusDate;
    }

    public Long getSinId() {
        return sinId;
    }

    public void setSinId(Long sinId) {
        this.sinId = sinId;
    }
}
