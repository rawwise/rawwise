package io.github.rawwise.model.record.benchmark;

import io.github.rawwise.engine.storage.codec.FixedOffsetBodyCodec;
import io.github.rawwise.engine.storage.codec.TlvBodyCodec;
import io.github.rawwise.model.record.ColumnGroup;
import io.github.rawwise.model.record.FixedOffsetCodec;
import io.github.rawwise.model.record.IndexedOffsetCodec;
import io.github.rawwise.model.record.PrimitiveType;
import io.github.rawwise.model.record.RawWiseType;
import io.github.rawwise.model.record.Schema;
import io.github.rawwise.model.record.StructField;
import io.github.rawwise.model.record.StructType;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 0)
public class CodecBenchmark {

    private static final FixedOffsetBodyCodec FIXED_OFFSET_CODEC = new FixedOffsetBodyCodec();
    private static final TlvBodyCodec TLV_CODEC = new TlvBodyCodec();

    @Param({"5", "20", "50"})
    private int columnCount;

    private Schema schema;
    private ColumnGroup columnGroup;
    private Map<String, Object> sampleRow;
    private byte[] fixedOffsetEncoded;
    private byte[] indexedOffsetEncoded;
    private byte[] tlvEncoded;
    private byte[] rawEncoded;
    private String lastFieldName;
    private String middleFieldName;
    private String firstFieldName;

    @Setup(Level.Trial)
    public void setup() {
        List<StructField> fields = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        List<RawWiseType> colTypes = new ArrayList<>();
        sampleRow = new LinkedHashMap<>();

        for (int i = 0; i < columnCount; i++) {
            String name = "col_" + i;
            RawWiseType type;
            Object value;

            int mod = i % 5;
            switch (mod) {
                case 0 -> {
                    type = PrimitiveType.INT;
                    value = i * 100;
                }
                case 1 -> {
                    type = PrimitiveType.LONG;
                    value = (long) i * 10000;
                }
                case 2 -> {
                    type = PrimitiveType.STRING;
                    value = "value_" + i + "_padding_to_make_it_realistic";
                }
                case 3 -> {
                    type = PrimitiveType.DOUBLE;
                    value = i * 1.5;
                }
                default -> {
                    type = PrimitiveType.INT;
                    value = i % 2;
                }
            }

            fields.add(new StructField(name, type));
            colNames.add(name);
            colTypes.add(type);
            sampleRow.put(name, value);
        }

        schema = new Schema(new StructType(fields), 1);
        columnGroup =
                new ColumnGroup(
                        "bench_cg", colNames, colTypes, ColumnGroup.Encoding.FIXED_OFFSET, 1);

        fixedOffsetEncoded = FixedOffsetCodec.INSTANCE.encode(columnGroup, sampleRow);
        indexedOffsetEncoded = IndexedOffsetCodec.INSTANCE.encode(columnGroup, sampleRow);
        tlvEncoded = TLV_CODEC.encode(schema, sampleRow);
        rawEncoded = rawEncode(sampleRow);

        firstFieldName = "col_0";
        middleFieldName = "col_" + (columnCount / 2);
        lastFieldName = "col_" + (columnCount - 1);
    }

    private byte[] rawEncode(Map<String, Object> row) {
        int size = 0;
        for (Object v : row.values()) {
            if (v instanceof Integer) size += 4;
            else if (v instanceof Long) size += 8;
            else if (v instanceof Double) size += 8;
            else if (v instanceof Boolean) size += 1;
            else if (v instanceof String s) size += 4 + s.getBytes(StandardCharsets.UTF_8).length;
        }
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        for (Object v : row.values()) {
            if (v instanceof Integer i) buf.putInt(i);
            else if (v instanceof Long l) buf.putLong(l);
            else if (v instanceof Double d) buf.putDouble(d);
            else if (v instanceof String s) {
                byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
                buf.putInt(bytes.length);
                buf.put(bytes);
            }
        }
        return buf.array();
    }

    @Benchmark
    public void fixedOffset_encode(Blackhole bh) {
        byte[] encoded = FixedOffsetCodec.INSTANCE.encode(columnGroup, sampleRow);
        bh.consume(encoded);
    }

    @Benchmark
    public void fixedOffset_decode(Blackhole bh) {
        Map<String, Object> decoded =
                FixedOffsetCodec.INSTANCE.decode(columnGroup, fixedOffsetEncoded);
        bh.consume(decoded);
    }

    @Benchmark
    public void fixedOffset_decodeField_first(Blackhole bh) {
        Object value =
                FixedOffsetCodec.INSTANCE.decodeField(
                        columnGroup, fixedOffsetEncoded, firstFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void fixedOffset_decodeField_middle(Blackhole bh) {
        Object value =
                FixedOffsetCodec.INSTANCE.decodeField(
                        columnGroup, fixedOffsetEncoded, middleFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void fixedOffset_decodeField_last(Blackhole bh) {
        Object value =
                FixedOffsetCodec.INSTANCE.decodeField(
                        columnGroup, fixedOffsetEncoded, lastFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void indexedOffset_encode(Blackhole bh) {
        byte[] encoded = IndexedOffsetCodec.INSTANCE.encode(columnGroup, sampleRow);
        bh.consume(encoded);
    }

    @Benchmark
    public void indexedOffset_decode(Blackhole bh) {
        Map<String, Object> decoded =
                IndexedOffsetCodec.INSTANCE.decode(columnGroup, indexedOffsetEncoded);
        bh.consume(decoded);
    }

    @Benchmark
    public void indexedOffset_decodeField_first(Blackhole bh) {
        Object value =
                IndexedOffsetCodec.INSTANCE.decodeField(
                        columnGroup, indexedOffsetEncoded, firstFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void indexedOffset_decodeField_middle(Blackhole bh) {
        Object value =
                IndexedOffsetCodec.INSTANCE.decodeField(
                        columnGroup, indexedOffsetEncoded, middleFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void indexedOffset_decodeField_last(Blackhole bh) {
        Object value =
                IndexedOffsetCodec.INSTANCE.decodeField(
                        columnGroup, indexedOffsetEncoded, lastFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void tlv_encode(Blackhole bh) {
        byte[] encoded = TLV_CODEC.encode(schema, sampleRow);
        bh.consume(encoded);
    }

    @Benchmark
    public void tlv_decode(Blackhole bh) {
        Map<String, Object> decoded = TLV_CODEC.decode(schema, tlvEncoded);
        bh.consume(decoded);
    }

    @Benchmark
    public void tlv_decodeField_first(Blackhole bh) {
        Object value = TLV_CODEC.decodeField(schema, tlvEncoded, firstFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void tlv_decodeField_middle(Blackhole bh) {
        Object value = TLV_CODEC.decodeField(schema, tlvEncoded, middleFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void tlv_decodeField_last(Blackhole bh) {
        Object value = TLV_CODEC.decodeField(schema, tlvEncoded, lastFieldName);
        bh.consume(value);
    }

    @Benchmark
    public void raw_encode(Blackhole bh) {
        byte[] encoded = rawEncode(sampleRow);
        bh.consume(encoded);
    }

    @Benchmark
    public void raw_decode(Blackhole bh) {
        ByteBuffer buf = ByteBuffer.wrap(rawEncoded).order(ByteOrder.BIG_ENDIAN);
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < columnCount; i++) {
            int mod = i % 5;
            switch (mod) {
                case 0 -> result.put("col_" + i, buf.getInt());
                case 1 -> result.put("col_" + i, buf.getLong());
                case 2 -> {
                    int len = buf.getInt();
                    byte[] bytes = new byte[len];
                    buf.get(bytes);
                    result.put("col_" + i, new String(bytes, StandardCharsets.UTF_8));
                }
                case 3 -> result.put("col_" + i, buf.getDouble());
                default -> result.put("col_" + i, buf.getInt());
            }
        }
        bh.consume(result);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt =
                new OptionsBuilder()
                        .include(CodecBenchmark.class.getSimpleName())
                        .forks(0)
                        .warmupIterations(3)
                        .measurementIterations(5)
                        .build();
        new Runner(opt).run();
    }
}
