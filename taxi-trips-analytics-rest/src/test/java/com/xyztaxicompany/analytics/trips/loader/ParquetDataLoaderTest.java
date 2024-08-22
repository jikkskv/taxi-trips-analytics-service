package com.xyztaxicompany.analytics.trips.loader;

import com.xyztaxicompany.analytics.trips.repo.TimeSeriesDataFetchService;
import com.xyztaxicompany.analytics.trips.trips.TripInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ParquetDataLoaderTest {

    @Mock
    private Resource parquetFileResource;

    @Mock
    private TimeSeriesDataFetchService<TripInfo> timeSeriesDataFetchService;

    @InjectMocks
    private ParquetDataLoader parquetDataLoader;

    @Test
    void testLoadFile_success() throws IOException {
        File mockFile = mock(File.class);
        when(parquetFileResource.getFile()).thenReturn(mockFile);
        when(mockFile.getPath()).thenReturn("/mock/path/to/parquet-file.parquet");
        assertThrows(Exception.class, () -> parquetDataLoader.loadFile());
    }

    @Test
    void testLoadFile_exception() throws IOException {
        when(parquetFileResource.getFile()).thenThrow(new IOException("File not found"));
        parquetDataLoader.loadFile();
        verify(timeSeriesDataFetchService, never()).pushData(anyList(), any());
    }
}
