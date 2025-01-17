package learn.flink.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class InfiniteStringSource<String> extends DataGeneratorSource<String> {

    public InfiniteStringSource(final GeneratorFunction<Long, String> generatorFunction, final long count, final TypeInformation<String> typeInfo) {
        super(generatorFunction, count, typeInfo);
    }

    public InfiniteStringSource(final GeneratorFunction<Long, String> generatorFunction, final long count, final RateLimiterStrategy rateLimiterStrategy, final TypeInformation<String> typeInfo) {
        super(generatorFunction, count, rateLimiterStrategy, typeInfo);
    }
}
