package io.github.rawwise.engine.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.github.rawwise.engine.storage.spi.Storage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class StorageInputFileTest {

    @Mock private Storage storage;

    @Mock private Storage.InputHandle inputHandle;

    private Storage.StoragePath testPath;

    @BeforeEach
    void setUp() throws IOException {
        MockitoAnnotations.openMocks(this);
        testPath = new Storage.StoragePath("file", "/data/test.parquet");
        when(storage.status(testPath))
                .thenReturn(new Storage.StorageStatus(testPath, true, false, 1024L, 1000L));
    }

    @Test
    void getLengthReturnsFileSizeFromStorageStatus() throws IOException {
        StorageInputFile inputFile = new StorageInputFile(storage, testPath);

        assertThat(inputFile.getLength()).isEqualTo(1024L);
    }

    @Test
    void constructorQueriesStorageStatusForLength() throws IOException {
        new StorageInputFile(storage, testPath);

        verify(storage).status(testPath);
    }

    @Test
    void newStreamOpensReadHandleViaStorage() throws IOException {
        when(storage.openRead(testPath)).thenReturn(inputHandle);
        when(inputHandle.stream()).thenReturn(new ByteArrayInputStream(new byte[0]));

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        inputFile.newStream();

        verify(storage).openRead(testPath);
    }

    @Test
    void seekableStreamReadReturnsCorrectByteValue() throws IOException {
        byte[] data = {0x41, 0x42, 0x43};
        setupInputHandleWith(data);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        int b = stream.read();
        assertThat(b).isEqualTo(0x41);
    }

    @Test
    void seekableStreamReadReturnsMinusOneAtEof() throws IOException {
        byte[] data = {};
        setupInputHandleWith(data);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        assertThat(stream.read()).isEqualTo(-1);
    }

    @Test
    void seekableStreamReadByteArrayReadsCorrectData() throws IOException {
        byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
        setupInputHandleWith(data);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        byte[] buf = new byte[3];
        int n = stream.read(buf, 0, 3);
        assertThat(n).isEqualTo(3);
        assertThat(buf).containsExactly(0x01, 0x02, 0x03);
    }

    @Test
    void seekableStreamReadFullyReadsExactBytes() throws IOException {
        byte[] data = {0x0A, 0x0B, 0x0C, 0x0D};
        setupInputHandleWith(data);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        byte[] buf = new byte[4];
        stream.readFully(buf);
        assertThat(buf).containsExactly(0x0A, 0x0B, 0x0C, 0x0D);
    }

    @Test
    void seekableStreamReadFullyWithOffsetReadsCorrectly() throws IOException {
        byte[] data = {0x01, 0x02, 0x03, 0x04};
        setupInputHandleWith(data);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        byte[] buf = new byte[6];
        stream.readFully(buf, 1, 4);
        assertThat(buf[0]).isEqualTo((byte) 0);
        assertThat(buf[1]).isEqualTo((byte) 0x01);
        assertThat(buf[4]).isEqualTo((byte) 0x04);
        assertThat(buf[5]).isEqualTo((byte) 0);
    }

    @Test
    void seekableStreamReadFullyThrowsOnUnexpectedEof() throws IOException {
        byte[] data = {0x01};
        setupInputHandleWith(data);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        byte[] buf = new byte[10];
        assertThatThrownBy(() -> stream.readFully(buf))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unexpected end of stream");
    }

    @Test
    void seekableStreamReadByteBufferWithArrayReadsCorrectly() throws IOException {
        byte[] data = {0x10, 0x20, 0x30};
        setupInputHandleWith(data);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        ByteBuffer buf = ByteBuffer.allocate(3);
        int n = stream.read(buf);
        assertThat(n).isEqualTo(3);
        buf.flip();
        assertThat(buf.get()).isEqualTo((byte) 0x10);
        assertThat(buf.get()).isEqualTo((byte) 0x20);
        assertThat(buf.get()).isEqualTo((byte) 0x30);
    }

    @Test
    void seekableStreamReadFullyByteBufferReadsAll() throws IOException {
        byte[] data = {0x11, 0x22, 0x33, 0x44};
        setupInputHandleWith(data);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        ByteBuffer buf = ByteBuffer.allocate(4);
        stream.readFully(buf);
        buf.flip();
        assertThat(buf.remaining()).isEqualTo(4);
    }

    @Test
    void seekableStreamSeekUpdatesPosition() throws IOException {
        byte[] data = {0x01, 0x02, 0x03, 0x04, 0x05};
        when(storage.openRead(testPath)).thenReturn(inputHandle);
        when(inputHandle.stream()).thenReturn(new ByteArrayInputStream(data));
        when(inputHandle.position()).thenReturn(0L, 2L);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        stream.seek(2);
        verify(inputHandle).seek(2);
    }

    @Test
    void seekableStreamGetPosReturnsCurrentPosition() throws IOException {
        when(storage.openRead(testPath)).thenReturn(inputHandle);
        when(inputHandle.stream()).thenReturn(new ByteArrayInputStream(new byte[10]));
        when(inputHandle.position()).thenReturn(5L);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        assertThat(stream.getPos()).isEqualTo(5L);
    }

    @Test
    void seekableStreamCloseDelegatesToHandle() throws Exception {
        when(storage.openRead(testPath)).thenReturn(inputHandle);
        when(inputHandle.stream()).thenReturn(new ByteArrayInputStream(new byte[0]));

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();
        stream.close();

        verify(inputHandle).close();
    }

    @Test
    void seekableStreamAvailableReturnsRemainingBytes() throws IOException {
        byte[] data = new byte[100];
        when(storage.openRead(testPath)).thenReturn(inputHandle);
        when(inputHandle.stream()).thenReturn(new ByteArrayInputStream(data));
        when(inputHandle.position()).thenReturn(30L);

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        assertThat(stream.available()).isEqualTo(1024 - 30);
    }

    @Test
    void seekableStreamCloseWrapsNonIOException() throws Exception {
        when(storage.openRead(testPath)).thenReturn(inputHandle);
        when(inputHandle.stream()).thenReturn(new ByteArrayInputStream(new byte[0]));
        doThrow(new RuntimeException("handle error")).when(inputHandle).close();

        StorageInputFile inputFile = new StorageInputFile(storage, testPath);
        org.apache.parquet.io.SeekableInputStream stream = inputFile.newStream();

        assertThatThrownBy(stream::close)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Failed to close storage input handle")
                .hasCauseInstanceOf(RuntimeException.class);
    }

    private void setupInputHandleWith(byte[] data) throws IOException {
        when(storage.openRead(testPath)).thenReturn(inputHandle);
        when(inputHandle.stream()).thenReturn(new ByteArrayInputStream(data));
        when(inputHandle.position()).thenReturn(0L);
    }
}
