package io.arenadata.dtm.common.schema.codec;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class AvroDecoder extends AvroSerdeHelper {
    private final DtmSpecificDatumReader<GenericRecord> datumReader = new DtmSpecificDatumReader<>(SpecificData.get());

    @SneakyThrows
    public List<GenericRecord> decode(byte[] encodedData) {
        val values = new ArrayList<GenericRecord>();
        try (val sin = new SeekableByteArrayInput(encodedData)) {
            try (val reader = new DataFileReader<>(sin, datumReader)) {
                while (reader.hasNext()) {
                    values.add(reader.next());
                }
            }
        } catch (Exception e) {
            log.error("AVRO deserialization error", e);
            throw e;
        }
        return values;
    }
}
