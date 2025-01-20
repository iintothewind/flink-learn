package learn.flink.function;

import lombok.RequiredArgsConstructor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.Configuration;

@RequiredArgsConstructor
public class RateLimitMap<T> extends RichMapFunction<T, T> {

    private final int rate;
    private transient GuavaFlinkConnectorRateLimiter rateLimiter;


    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        rateLimiter = new GuavaFlinkConnectorRateLimiter();
        // rate per local task =  rate / runtimeContext.getTaskInfo().getNumberOfParallelSubtasks()
        rateLimiter.setRate(rate);
        rateLimiter.open(getRuntimeContext());
    }

    @Override
    public T map(final T value) throws Exception {
        rateLimiter.acquire(rate);
        return value;
    }

}
