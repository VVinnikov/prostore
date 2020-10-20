package ru.ibs.dtm.common.delta;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SelectOnInterval {
    private Long selectOnFrom;
    private Long selectOnTo;

    public String getIntervalStr(){
        return "(" + selectOnFrom + "," + selectOnTo + ")";
    }
}
