package ru.ibs.dtm.query.execution.plugin.adqm.configuration.kafka;

public class KafkaAdminProperty {
    /**
     * Топик запроса.
     */
    String adqmUploadRq = "";
    /**
     * Топик ответа от запроса к ADQM.
     */
    String adqmUploadRs = "";
    /**
     * Топик ошибок от запроса к ADQM.
     */
    String adqmUploadErr = "";

    public String getAdqmUploadRq() {
        return adqmUploadRq;
    }

    public void setAdqmUploadRq(String adqmUploadRq) {
        this.adqmUploadRq = adqmUploadRq;
    }

    public String getAdqmUploadRs() {
        return adqmUploadRs;
    }

    public void setAdqmUploadRs(String adqmUploadRs) {
        this.adqmUploadRs = adqmUploadRs;
    }

    public String getAdqmUploadErr() {
        return adqmUploadErr;
    }

    public void setAdqmUploadErr(String adqmUploadErr) {
        this.adqmUploadErr = adqmUploadErr;
    }
}
