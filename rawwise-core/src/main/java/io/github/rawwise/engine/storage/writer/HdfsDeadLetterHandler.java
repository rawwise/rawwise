package io.github.rawwise.engine.storage.writer;

import io.github.rawwise.model.record.WriteEntry;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;

@Slf4j
public class HdfsDeadLetterHandler implements DeadLetterHandler {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final FileSystem fs;
    private final String hdfsBasePath;
    private final Base64.Encoder base64Encoder = Base64.getEncoder();

    public HdfsDeadLetterHandler(String hdfsUri, String hdfsBasePath) {
        this.hdfsBasePath = hdfsBasePath;
        try {
            Configuration conf = new Configuration();
            // If needed, HDFS user permission proxy can be set here
            // System.setProperty("HADOOP_USER_NAME", "hadoop_user");
            this.fs = FileSystem.get(URI.create(hdfsUri), conf);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize HDFS FileSystem", e);
        }
    }

    @Override
    public void save(List<WriteEntry> failedData, Exception cause) {
        if (failedData == null || failedData.isEmpty()) {
            return;
        }
        // 1. Generate file path
        // Directory structure: base_path/instance_id(or default)/yyyy-MM-dd/uuid.seq
        // This facilitates date-based cleanup or replay
        String datePath = LocalDateTime.now().format(DATE_FORMATTER);
        String fileName =
                "dlq_"
                        + System.currentTimeMillis()
                        + "_"
                        + UUID.randomUUID().toString().substring(0, 8)
                        + ".seq";
        Path filePath = new Path(hdfsBasePath, datePath + "/" + fileName);
        SequenceFile.Writer writer = null;
        try {
            // 2. Create SequenceFile Writer
            // Use Snappy compression to save space
            SequenceFile.Writer.Option optPath = SequenceFile.Writer.file(filePath);
            SequenceFile.Writer.Option optKey = SequenceFile.Writer.keyClass(BytesWritable.class);
            SequenceFile.Writer.Option optVal = SequenceFile.Writer.valueClass(BytesWritable.class);
            SequenceFile.Writer.Option optCompress =
                    SequenceFile.Writer.compression(
                            SequenceFile.CompressionType.BLOCK,
                            new org.apache.hadoop.io.compress.SnappyCodec());
            writer = SequenceFile.createWriter(fs.getConf(), optPath, optKey, optVal, optCompress);
            // 3. Write error metadata
            // Write exception info as a special record, Key is "ERROR_META"
            String errorMsg =
                    String.format("Time: %s | Error: %s", LocalDateTime.now(), cause.getMessage());
            writer.append(
                    new BytesWritable("ERROR_META".getBytes()),
                    new BytesWritable(errorMsg.getBytes()));
            // 4. Batch write data
            // Key: original Key (byte[])
            // Value: original Value (byte[])
            BytesWritable keyWritable = new BytesWritable();
            BytesWritable valWritable = new BytesWritable();
            for (WriteEntry entry : failedData) {
                keyWritable.set(entry.key(), 0, entry.key().length);
                valWritable.set(entry.encodedValue(), 0, entry.encodedValue().length);
                writer.append(keyWritable, valWritable);
            }
        } catch (Exception e) {
            // If even HDFS is down, can only log
            // In production, recommend degrading to local disk write or sending alerts
            log.error("CRITICAL: Failed to write Dead Letter Queue to HDFS! Path: {}", filePath, e);
        } finally {
            IOUtils.closeStream(writer);
        }
    }
}
