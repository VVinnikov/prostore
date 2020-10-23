package io.arenadata.dtm.common.eventbus;

/*Запрос на обработку ошибок*/
public class ErrorHandlerRequest {
    private Integer sinId;
    private String datamartMnemonic;

    public ErrorHandlerRequest() {
    }

    public Integer getSinId() {
        return sinId;
    }

    public String getDatamartMnemonic() {
        return datamartMnemonic;
    }

    public ErrorHandlerRequest addSinId(Integer sinId) {
        this.sinId = sinId;
        return this;
    }

    public ErrorHandlerRequest addDatamartMnemonic(String datamartMnemonic) {
        this.datamartMnemonic = datamartMnemonic;
        return this;
    }
}
