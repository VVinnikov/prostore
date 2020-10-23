package io.arenadata.dtm.common.schema.codec;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;

public class DtmSpecificDatumReader<T> extends SpecificDatumReader<T> {
    public DtmSpecificDatumReader() {
    }

    public DtmSpecificDatumReader(Class<T> c) {
        super(c);
    }

    public DtmSpecificDatumReader(Schema schema) {
        super(schema);
    }

    public DtmSpecificDatumReader(Schema writer, Schema reader) {
        super(writer, reader);
    }

    public DtmSpecificDatumReader(Schema writer, Schema reader, SpecificData data) {
        super(writer, reader, data);
    }

    public DtmSpecificDatumReader(SpecificData data) {
        super(data);
    }

    @Override
    protected Class findStringClass(Schema schema) {
        return String.class;
    }
}
