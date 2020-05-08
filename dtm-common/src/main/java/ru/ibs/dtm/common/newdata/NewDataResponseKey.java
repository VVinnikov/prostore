package ru.ibs.dtm.common.newdata;

import java.util.UUID;

public class NewDataResponseKey {
    private UUID loadProcID;
    private int sinId;
    private String schemaName;

    public NewDataResponseKey(UUID loadProcID, int sinId, String schemaName) {
        this.loadProcID = loadProcID;
        this.sinId = sinId;
        this.schemaName = schemaName;
    }

    public NewDataResponseKey() {
    }

    public UUID getLoadProcID() {
        return loadProcID;
    }

    public void setLoadProcID(UUID loadProcID) {
        this.loadProcID = loadProcID;
    }

    public int getSinId() {
        return sinId;
    }

    public void setSinId(int sinId) {
        this.sinId = sinId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}