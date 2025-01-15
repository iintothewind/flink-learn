package learn.flink;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

public class LogSinkTest {

    @Test
    @SneakyThrows
    public void testLogSink01() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a DataStream
        env.fromData("Hello", "Flink", "Logging")
            .print();

        // Execute the Flink job
        env.execute("Flink Logback Sink Example");
    }

    @Test
    @SneakyThrows
    public void testLogSink02() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a DataStream
        env.fromData("Hello", "Flink", "Logging")
            .addSink(new LogSink<>())
            .name("log-sink");

        // Execute the Flink job
        env.execute("Flink Logback Sink Example");
    }

}
