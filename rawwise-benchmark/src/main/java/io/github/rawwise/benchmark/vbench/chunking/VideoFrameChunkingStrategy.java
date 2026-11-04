package io.github.rawwise.benchmark.vbench.chunking;

import io.github.rawwise.axiom.pipeline.derivation.ChunkingStrategy;
import io.github.rawwise.model.record.Chunk;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Video frame splitting strategy: consumes RawWise generic ChunkingStrategy SPI.
 *
 * <p>realactualproductiondeployment:pass ffmpeg/JCodec frame extraction,eachframe → IMAGE_REGION
 * Chunk.
 *
 * <p>This implementation is a stub: splits video bytes by fixed size, simulating video frame
 * splitting. useat: - E2E test(no dependency ffmpeg) - placeholder implementation (replaced by
 * subclass in production)
 *
 * <p>params convention: - frame_rate: int (frame rate, default 24) - bytes_per_frame: int (bytes
 * per frame, for stub use, default 1024) - max_frames: int (max frame count limit, default 1000)
 *
 * <p>Production replacement path: public class FfmpegVideoFrameChunkingStrategy extends
 * VideoFrameChunkingStrategy {... }
 */
@Slf4j
public class VideoFrameChunkingStrategy implements ChunkingStrategy {

    public static final String PARAM_FRAME_RATE = "frame_rate";
    public static final String PARAM_BYTES_PER_FRAME = "bytes_per_frame";
    public static final String PARAM_MAX_FRAMES = "max_frames";

    @Override
    public String name() {
        return "video-frame";
    }

    private static final int DEFAULT_FRAME_RATE = 24;
    private static final int DEFAULT_BYTES_PER_FRAME = 1024;
    private static final int DEFAULT_MAX_FRAMES = 1000;

    @Override
    public Chunk.ChunkType supportedType() {
        return Chunk.ChunkType.IMAGE_REGION;
    }

    @Override
    public List<byte[]> split(byte[] content, Map<String, Object> params) {
        if (content == null || content.length == 0) {
            return List.of();
        }

        int frameRate = extractInt(params, PARAM_FRAME_RATE, DEFAULT_FRAME_RATE);
        int bytesPerFrame = extractInt(params, PARAM_BYTES_PER_FRAME, DEFAULT_BYTES_PER_FRAME);
        int maxFrames = extractInt(params, PARAM_MAX_FRAMES, DEFAULT_MAX_FRAMES);

        if (frameRate <= 0 || bytesPerFrame <= 0) {
            throw new IllegalArgumentException("frame_rate and bytes_per_frame must be positive");
        }

        List<byte[]> frames = new ArrayList<>();
        int frameIndex = 0;
        int offset = 0;
        while (offset < content.length && frameIndex < maxFrames) {
            int end = Math.min(offset + bytesPerFrame, content.length);
            int frameSize = end - offset;
            byte[] frame = encodeFrame(content, offset, frameSize, frameIndex, frameRate);
            frames.add(frame);
            offset = end;
            frameIndex++;
        }

        log.debug(
                "VideoFrameChunkingStrategy split {} bytes into {} frames (frame_rate={})",
                content.length,
                frames.size(),
                frameRate);
        return frames;
    }

    /**
     * Encode single frame: header (frame_index, frame_rate, frame_size) + raw bytes Production
     * should output JPEG/PNG encoded image bytes.
     */
    private byte[] encodeFrame(byte[] source, int offset, int size, int frameIndex, int frameRate) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(bos)) {
            dos.writeInt(frameIndex);
            dos.writeInt(frameRate);
            dos.writeInt(size);
            dos.write(source, offset, size);
            return bos.toByteArray();
        } catch (IOException e) {
            log.error("Failed to encode frame {}", frameIndex, e);
            byte[] fallback = new byte[size];
            System.arraycopy(source, offset, fallback, 0, size);
            return fallback;
        }
    }

    public static FrameHeader decodeFrameHeader(byte[] frame) {
        if (frame == null || frame.length < 12) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.wrap(frame, 0, 12);
        int frameIndex = buf.getInt();
        int frameRate = buf.getInt();
        int frameSize = buf.getInt();
        return new FrameHeader(frameIndex, frameRate, frameSize);
    }

    private int extractInt(Map<String, Object> params, String key, int defaultValue) {
        if (params == null) {
            return defaultValue;
        }
        Object val = params.get(key);
        if (val == null) {
            return defaultValue;
        }
        if (val instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(val.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public record FrameHeader(int frameIndex, int frameRate, int frameSize) {}
}
