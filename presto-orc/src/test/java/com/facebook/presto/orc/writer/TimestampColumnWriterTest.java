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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DefaultOrcWriterFlushPolicy;
import com.facebook.presto.orc.OrcWriter;
import com.facebook.presto.orc.OrcWriterOptions;
import com.facebook.presto.orc.OrcWriterStats;
import com.facebook.presto.orc.TempFile;
import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;

public class TimestampColumnWriterTest
{
    private static final File testFile = new File("timestamp_testfile");

    @Test
    public void testTimestamp()
    {
        TempFile outputFile = new TempFile();
        try {
            Type type = INTEGER;
            List<Type> types = ImmutableList.of(type, type, type);
            OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                    .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder().withStripeMaxRowCount(100).build())
                    .withDwrfStripeCacheEnabled(true)
                    .withDwrfStripeCacheMode(DwrfStripeCacheMode.INDEX_AND_FOOTER)
                    .withDwrfStripeCacheMaxSize(DataSize.valueOf("8MB"))
                    .build();

            OrcWriter writer = new OrcWriter(
                    new OutputStreamDataSink(new FileOutputStream(testFile)),
                    ImmutableList.of("Int1", "Int2", "Int3"),
                    types,
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

            // write 4 stripes with 100 values each
            int count = 0;
            for (int stripe = 0; stripe < 4; stripe++) {
                BlockBuilder[] blockBuilders = new BlockBuilder[3];
                for (int i = 0; i < blockBuilders.length; i++) {
                    blockBuilders[i] = type.createBlockBuilder(null, 100);
                }

                for (int row = 0; row < 100; row++) {
                    blockBuilders[0].writeInt(count);
                    blockBuilders[1].writeInt(Integer.MAX_VALUE);
                    blockBuilders[2].writeInt(count * 10);
                    count++;
                }

                Block[] blocks = new Block[blockBuilders.length];
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = blockBuilders[i].build();
                }
                writer.write(new Page(blocks));
            }

            writer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
