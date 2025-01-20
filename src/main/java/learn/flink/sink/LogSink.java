package learn.flink.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

@Slf4j
public class LogSink<T> extends RichSinkFunction<T> {

    @Override
    public void invoke(final T value, final Context context) throws Exception {
//        final StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
        log.info("value: {}", value);
    }
}
