package learn.flink;

import java.time.Duration;
import learn.training.TaxiRideGenerator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.junit.jupiter.api.Test;

@Slf4j
public class TaxiRideTest {

    @Test
    @SneakyThrows
    public void testGenerateTaxiRide01() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new TaxiRideGenerator())
            .map(t -> Tuple2.of(t.taxiId, 1))
            .returns(TypeInformation.of(new TypeHint<>() {
            }))
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(3L)))
            .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
            .print();

        env.execute();
    }

}
