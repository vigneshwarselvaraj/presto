/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DefaultOrcWriterFlushPolicy;
import com.facebook.presto.orc.DwrfEncryptionProvider;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcReaderOptions;
import com.facebook.presto.orc.OrcSelectiveRecordReader;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.StorageStripeMetadataSource;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP_MICROSECONDS;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TimestampColumnWriterTest
{
    private static final File testFile = new File("timestamp_testfile");
    private static final List<String> COLUMN_NAMES = ImmutableList.of("Int1", "Int2", "milliseconds", "microseconds");
    private static final List<Type> COLUMN_TYPES = ImmutableList.of(INTEGER, INTEGER, TIMESTAMP, TIMESTAMP_MICROSECONDS);

    @Test
    public void testTimestamp()
    {
        try {
            OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                    .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder().withStripeMaxRowCount(100).build())
                    .withDwrfStripeCacheEnabled(true)
                    .withDwrfStripeCacheMode(DwrfStripeCacheMode.INDEX_AND_FOOTER)
                    .withDwrfStripeCacheMaxSize(DataSize.valueOf("8MB"))
                    .build();

            OrcWriter writer = new OrcWriter(
                    new OutputStreamDataSink(new FileOutputStream(testFile)),
                    ImmutableList.of("Int1", "Int2", "milliseconds", "microseconds"),
                    COLUMN_TYPES,
                    DWRF,
                    ZLIB,
                    Optional.empty(),
                    NO_ENCRYPTION,
                    writerOptions,
                    ImmutableMap.of(),
                    HIVE_STORAGE_TIME_ZONE,
                    true,
                    BOTH,
                    new OrcWriterStats());

            // write 4 stripes with 4 values each
            int count = 0;
            for (int stripe = 0; stripe < 4; stripe++) {
                BlockBuilder[] blockBuilders = new BlockBuilder[4];
                blockBuilders[0] = INTEGER.createBlockBuilder(null, 4);
                blockBuilders[1] = INTEGER.createBlockBuilder(null, 4);
                blockBuilders[2] = TIMESTAMP.createBlockBuilder(null, 4);
                blockBuilders[3] = TIMESTAMP_MICROSECONDS.createBlockBuilder(null, 4);

                for (int row = 0; row < 5; row++) {
                    blockBuilders[0].writeInt(count);
                    blockBuilders[1].writeInt(Integer.MAX_VALUE);
                    Instant instant = Instant.ofEpochSecond(1_651_061_532,
                            123_456_789 + (1_000_000 * (stripe + row)));
                    System.out.println("Time is " + Timestamp.from(instant));
                    long millis = TimeUnit.SECONDS.toMillis(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMillis(instant.getNano());
                    TIMESTAMP.writeLong(blockBuilders[2], millis);
                    long micros = TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
                    System.out.println("MICROS to write is " + micros);
                    TIMESTAMP_MICROSECONDS.writeLong(blockBuilders[3], micros);
                    count++;
                }

                Block[] blocks = new Block[blockBuilders.length];
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = blockBuilders[i].build();
                }
                writer.write(new Page(blocks));
            }

            writer.close();
            // fail();
            readFile(testFile);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void readFile(File file)
            throws IOException
    {
        OrcDataSource orcDataSource =
                new FileOrcDataSource(
                        file,
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        true);

        OrcReader orcReader =
                new OrcReader(
                        orcDataSource,
                        OrcEncoding.DWRF,
                        new StorageOrcFileTailSource(),
                        new StorageStripeMetadataSource(),
                        NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                        new OrcReaderOptions(
                                new DataSize(1, MEGABYTE),
                                new DataSize(1, MEGABYTE),
                                new DataSize(256, MEGABYTE),
                                /*zstdJni*/ true,
                                /*mapNullKeys*/ true,
                                /*timestampMicros*/ true,
                                /*appendRowNumber*/ false),
                        false,
                        DwrfEncryptionProvider.NO_ENCRYPTION,
                        DwrfKeyProvider.EMPTY,
                        new RuntimeStats());

        System.out.printf("File has %s rows\n", orcReader.getFooter().getNumberOfRows());
        System.out.printf("Column types in the file %s\n", orcReader.getFooter().getTypes());
        System.out.printf("Stripes in the file %s\n", orcReader.getFooter().getStripes());

        ImmutableMap.Builder<Integer, Type> includedColumns = ImmutableMap.builder();
        ImmutableList.Builder<Integer> outputColumns = ImmutableList.builder();

        for (int i = 0; i < COLUMN_TYPES.size(); i++) {
            Type type = COLUMN_TYPES.get(i);
            includedColumns.put(i, type);
            outputColumns.add(i);
        }

        OrcSelectiveRecordReader recordReader =
                orcReader.createSelectiveRecordReader(
                        includedColumns.build(),
                        outputColumns.build(),
                        ImmutableMap.of(),
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        OrcPredicate.TRUE,
                        0,
                        orcDataSource.getSize(),
                        DateTimeZone.forID("America/Bahia_Banderas"),
                        false,
                        NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                        Optional.empty(),
                        1024);

        Page page;
        int rows = 0;
        while ((page = recordReader.getNextPage()) != null) {
            Block col1Block = page.getBlock(0);
            Block col2Block = page.getBlock(1);
            Block col3Block = page.getBlock(2);
            Block col4Block = page.getBlock(3);

            for (int pageRow = 0; pageRow < page.getPositionCount(); pageRow++) {
                int col1Value = col1Block.getInt(pageRow);
                int col2Value = col2Block.getInt(pageRow);
                long micros = col3Block.getLong(pageRow);
                long millis = micros / 1000_000;
                long nanos = (micros % 1000_000) * 1000;
                Timestamp timestamp = Timestamp.from(Instant.ofEpochSecond(millis, nanos));
                long micros2 = col4Block.getLong(pageRow);
                long millis2 = micros2 / 1000_000;
                // multiplying by 1000 to fill zeros for nano precision
                long nanos2 = (micros2 % 1000_000) * 1000;
                // System.out.println("entire micros value is " + micros);
                Timestamp timestamp2 = Timestamp.from(Instant.ofEpochSecond(millis2, nanos2));
                // System.out.printf("Row %s: col1 = %s, col2 = %s, col3 = %s, col4 = %s\n", rows, col1Value, col2Value, timestamp, timestamp2);
                System.out.printf("Micro timestamp = %s, Milli timestamp = %s\n", timestamp2, timestamp);
                rows++;
            }
        }
        System.out.printf("Total rows read: %s", rows);
    }
}
