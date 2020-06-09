package ru.ibs.dtm.common.schema.codec;

import lombok.SneakyThrows;
import lombok.val;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

import java.util.ArrayList;
import java.util.List;

public class AvroDecoder {
    private final SpecificDatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(SpecificData.get());

    @SneakyThrows
    public List<GenericRecord> readValues(byte[] encodedData) {
        val values = new ArrayList<GenericRecord>();
        try (val sin = new SeekableByteArrayInput(encodedData)) {
            try (val reader = new DataFileReader<>(sin, datumReader)) {
                for (GenericRecord genericRecord : reader) {
                    values.add(genericRecord);
                }
            }
        }
        return values;
    }
}
