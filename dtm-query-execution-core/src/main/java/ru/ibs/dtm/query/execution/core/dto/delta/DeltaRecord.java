package ru.ibs.dtm.query.execution.core.dto.delta;

import com.fasterxml.jackson.annotation.JsonInclude;
import ru.ibs.dtm.common.delta.DeltaLoadStatus;

import java.time.LocalDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeltaRecord {

    private Long loadId;
    private String datamartMnemonic;
    private LocalDateTime sysDate;
    private LocalDateTime statusDate;
    private Long sinId;
    private String loadProcId;
    private DeltaLoadStatus status;

    public DeltaRecord() {
    }

    public DeltaRecord(Long loadId, String datamartMnemonic, LocalDateTime sysDate, LocalDateTime statusDate, Long sinId, String loadProcId, DeltaLoadStatus status) {
        this.loadId = loadId;
        this.datamartMnemonic = datamartMnemonic;
        this.sysDate = sysDate;
        this.statusDate = statusDate;
        this.sinId = sinId;
        this.loadProcId = loadProcId;
        this.status = status;
    }

    public Long getLoadId() {
        return loadId;
    }

    public void setLoadId(Long loadId) {
        this.loadId = loadId;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

    public void setDatamartMnemonic(String datamartMnemonic) {
        this.datamartMnemonic = datamartMnemonic;
    }

    public LocalDateTime getSysDate() {
        return sysDate;
    }

    public void setSysDate(LocalDateTime sysDate) {
        this.sysDate = sysDate;
    }

    public LocalDateTime getStatusDate() {
        return statusDate;
    }

    public void setStatusDate(LocalDateTime statusDate) {
        this.statusDate = statusDate;
    }

    public Long getSinId() {
        return sinId;
    }

    public void setSinId(Long sinId) {
        this.sinId = sinId;
    }

    public String getLoadProcId() {
        return loadProcId;
    }

    public void setLoadProcId(String loadProcId) {
        this.loadProcId = loadProcId;
    }

    public DeltaLoadStatus getStatus() {
        return status;
    }

    public void setStatus(DeltaLoadStatus status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "DeltaRecord{" +
                "loadId=" + loadId +
                ", datamartMnemonic='" + datamartMnemonic + '\'' +
                ", sysDate=" + sysDate +
                ", statusDate=" + statusDate +
                ", sinId=" + sinId +
                ", loadProcId='" + loadProcId + '\'' +
                ", status=" + status +
                '}';
    }
}
