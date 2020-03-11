package com.github.benmanes.caffeine.cache.simulator.parser.lecar;

import com.github.benmanes.caffeine.cache.simulator.parser.TextTraceReader;
import com.github.benmanes.caffeine.cache.simulator.parser.TraceReader;

import java.io.IOException;
import java.util.stream.LongStream;

public final class FloridaWebTraceReader extends TextTraceReader implements TraceReader.KeyOnlyTraceReader {

    public FloridaWebTraceReader(String filePath) {
        super(filePath);
    }

    @Override
    public LongStream keys() throws IOException {
        return lines().flatMapToLong(line -> {
            String[] array = line.split(" ", 9);
            long startBlock = Long.parseLong(array[3]);
            int numBlocks = Integer.parseInt(array[4]);
            char readWrite = Character.toLowerCase(array[5].charAt(0));
            //return LongStream.range(startBlock, startBlock + numBlocks);
            return (readWrite == 'w')
                    ? LongStream.empty()
                    : LongStream.range(startBlock, startBlock + numBlocks);
        });
    }
}
