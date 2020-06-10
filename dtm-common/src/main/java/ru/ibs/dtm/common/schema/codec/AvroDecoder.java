package ru.ibs.dtm.common.schema.codec;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

import java.util.ArrayList;
import java.util.List;

@Slf4j
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
        } catch (Exception e) {
            log.error("AVRO deserialization error", e);
            throw e;
        }
        return values;
    }
}
