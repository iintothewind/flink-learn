package learn.flink.function;

import java.util.Objects;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Collector;

public class UniqFlatMapFunction<T> extends RichFlatMapFunction<T, T> {

    private ValueState<Boolean> keyChecker;

    @Override
    public void open(final OpenContext openContext) throws Exception {
        keyChecker = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("dpChecker", Boolean.class));
    }

    @Override
    public void flatMap(final T value, final Collector<T> out) throws Exception {
        if (Objects.isNull(keyChecker.value())) {
            out.collect(value);
            keyChecker.update(true);
        }
    }

}
