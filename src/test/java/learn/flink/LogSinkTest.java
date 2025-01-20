package learn.flink;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import learn.flink.function.RateLimitMap;
import learn.flink.function.UniqFlatMapFunction;
import learn.flink.sink.LogSink;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LogSinkTest {

    @BeforeAll
    static void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("./target/output"));
    }

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
            .filter(line -> line.contains("in"))
            .addSink(new LogSink<>())
            .name("log-sink");

        // Execute the Flink job
        final JobExecutionResult result = env.execute("Flink Logback Sink Example");
        System.out.println(result);
    }


    @Test
    @SneakyThrows
    public void testLogSink03() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a DataStream
        env.fromData("Hello", "Flink", "Logging", "")
            .filter(line -> Objects.nonNull(line) && line.contains("in"))
            .map(String::toUpperCase)
            .sinkTo(FileSink.forRowFormat(Path.fromLocalFile(new File("./target/output")), new SimpleStringEncoder<String>()).build());
//            .sinkTo(FileSink.forBulkFormat(Path.fromLocalFile(new File(".")), AvroWriters.forReflectRecord(String.class)).build());

        // Execute the Flink job
        env.execute("Flink Logback Sink Example");
    }

    @Test
    @SneakyThrows
    public void testLogSink04() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a DataStream
        env.fromData("Hello", "Flink", "Logging", "", "Flink", "Flink")
            .map(ln -> new Tuple2<>(ln, 1))
            .returns(TypeInformation.of(new TypeHint<>() {
            }))
            .keyBy(line -> line.f0)
            .sum(1)
            .writeAsText(Path.fromLocalFile(new File("./target/output")).getPath());
//            .sinkTo(FileSink.forRowFormat(Path.fromLocalFile(new File(".")), new SimpleStringEncoder<String>()).build());
//            .sinkTo(FileSink.forBulkFormat(Path.fromLocalFile(new File(".")), AvroWriters.forReflectRecord(String.class)).build());

        // Execute the Flink job
        env.execute("Flink Logback Sink Example");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }


    @Test
    @SneakyThrows
    public void testLogSink05() {

        try (final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

            final SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env
                .fromData("aa bb cc", "dd ee aa", "bb ff cc gg")
//              .socketTextStream("192.168.0.129", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(final String line, final Collector<Tuple2<String, Integer>> out) throws Exception {
                        Arrays.stream(line.split(" ")).forEach(w -> out.collect(Tuple2.of(w, 1)));
                    }
                })
                .keyBy(value -> value.f0)
//              .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1)))
                .sum(1)
                .name("windowSum");
//                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));

            dataStream.print();

            env.execute("Window WordCount");
        }

    }

    @Test
    @SneakyThrows
    public void testLogSink051() {

        try (final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

            final SingleOutputStreamOperator<String> dataStream = env
                .fromData("aa bb cc", "dd ee aa", "bb ff cc gg")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(final String line, final Collector<Tuple2<String, Integer>> out) throws Exception {
                        Arrays.stream(line.split(" ")).forEach(w -> out.collect(Tuple2.of(w, 1)));
                    }
                })
                .keyBy(value -> value.f0)
                .flatMap(new UniqFlatMapFunction<>())
                .map(t -> t.f0);

            dataStream.print();

            env.execute("Window WordCount");
        }

    }


    @Test
    @SneakyThrows
    public void testLogSink06() {

        final DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> String.format("Number: %s", index), Integer.MAX_VALUE, Types.STRING);
//      final DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> "aaa", Integer.MAX_VALUE, Types.STRING);

        try (final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

            final SingleOutputStreamOperator<List<String>> dataStream = env
                .fromSequence(1, 99)
                .map(i -> String.format("Number: %s", i))
                .returns(TypeInformation.of(new TypeHint<>() {
                }))
                .map((MapFunction<String, String>) value -> {
                    TimeUnit.MILLISECONDS.sleep(1000L);
                    return value;
                })
                .returns(TypeInformation.of(new TypeHint<>() {
                }))
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1)))
                .process(new ProcessAllWindowFunction<String, List<String>, TimeWindow>() {
                    @Override
                    public void process(final ProcessAllWindowFunction<String, List<String>, TimeWindow>.Context context, final Iterable<String> elements, final Collector<List<String>> out) throws Exception {
                        final List<String> lst = new ArrayList<>();
                        for (String element : elements) {
                            lst.add(element);
                        }
                        out.collect(lst);
                    }
                })
                .name("processWindow");

//         dataStream.addSink(new LogSink<>());
            dataStream.print();

            env.execute("windowList");
        }

    }

    @Test
    @SneakyThrows
    public void testLogSink07() {

        final DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> String.format("Number: %s", index), Integer.MAX_VALUE, Types.STRING);
//      final DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> "aaa", Integer.MAX_VALUE, Types.STRING);

        try (final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            final SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env
                .fromSequence(1, 100) // Simulated infinite sequence
                .map(i -> "aaa") // Generate the word "aaa"
                .returns(TypeInformation.of(new TypeHint<>() {
                }))
                .map((MapFunction<String, Tuple2<String, Integer>>) value -> {
                    TimeUnit.MILLISECONDS.sleep(1000L);
                    return new Tuple2<>(value, 1);
                })
                .returns(TypeInformation.of(new TypeHint<>() {
                }))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Tuple2<String, Integer>>forMonotonousTimestamps()
                    .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())) // Using system time as event time
                .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(1)))
//                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1)))
                .sum(1) // Sum the counts globally
                .keyBy(t -> t.f0)
                .sum(1)
                .name("windowSum");
//                .reduce((t1, t2) -> new Tuple2<>("total", t1.f1 + t2.f1));

            dataStream.addSink(new LogSink<>());

            env.execute("Window WordCount");
        }
    }

    @Test
    @SneakyThrows
    public void testLogSink08() {

        final DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> String.format("Number: %s", index), Integer.MAX_VALUE, Types.STRING);
//      final DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> "aaa", Integer.MAX_VALUE, Types.STRING);

        try (final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            final SingleOutputStreamOperator<List<String>> dataStream = env
                .fromSequence(1, 100) // Simulated infinite sequence
                .map(i -> String.format("number: %s", i)) // Generate the word "aaa"
                .returns(TypeInformation.of(new TypeHint<>() {
                }))
                .map(value -> {
                    TimeUnit.MILLISECONDS.sleep(1000L);
                    return value;
                })
                .returns(TypeInformation.of(new TypeHint<>() {
                }))
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1)))
                .aggregate(new AggregateFunction<String, List<String>, List<String>>() {

                    @Override
                    public List<String> createAccumulator() {
                        return List.of();
                    }

                    @Override
                    public List<String> add(final String value, final List<String> accumulator) {
                        return Stream.concat(accumulator.stream(), Stream.of(value)).collect(Collectors.toList());
                    }

                    @Override
                    public List<String> getResult(final List<String> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public List<String> merge(final List<String> a, final List<String> b) {
                        return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
                    }
                })
                .name("windowConcat");
//                .reduce((t1, t2) -> new Tuple2<>("total", t1.f1 + t2.f1));

            dataStream.addSink(new LogSink<>());

            env.execute("Window WordCount");
        }
    }


    @Test
    @SneakyThrows
    public void testLogSink09() {

        final DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> String.format("Number: %s", index), Integer.MAX_VALUE, Types.STRING);
//      final DataGeneratorSource<String> source = new DataGeneratorSource<>(index -> "aaa", Integer.MAX_VALUE, Types.STRING);

        try (final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            final SingleOutputStreamOperator<List<String>> dataStream = env
                .fromSequence(1, 100) // Simulated infinite sequence
                .map(i -> i % 2 == 0 ? "a" : "b") // Generate the word
                .returns(TypeInformation.of(new TypeHint<>() {
                }))
                .map(value -> {
                    TimeUnit.MILLISECONDS.sleep(1000L);
                    return value;
                })
                .returns(TypeInformation.of(new TypeHint<>() {
                }))
                .keyBy(s -> s)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1)))
                .aggregate(new AggregateFunction<String, List<String>, List<String>>() {

                    @Override
                    public List<String> createAccumulator() {
                        return List.of();
                    }

                    @Override
                    public List<String> add(final String value, final List<String> accumulator) {
                        return Stream.concat(accumulator.stream(), Stream.of(value)).collect(Collectors.toList());
                    }

                    @Override
                    public List<String> getResult(final List<String> accumulator) {
                        return accumulator;
                    }

                    @Override
                    public List<String> merge(final List<String> a, final List<String> b) {
                        return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
                    }
                })
                .name("windowConcat");

            dataStream.addSink(new LogSink<>());

            env.execute("Window WordCount");
        }
    }

    @Test
    @SneakyThrows
    public void testLogSink10() {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a DataStream
        final KeyedStream<String, String> stream1 = env.fromData("1", "2", "3")
            .keyBy(x -> x);

        final KeyedStream<Integer, String> stream2 = env.fromData(1, 2, 3)
            .keyBy(String::valueOf);

        stream1.connect(stream2)
            .flatMap(new RichCoFlatMapFunction<String, Integer, Tuple2<String, Integer>>() {
                private transient ValueState<Tuple2<String, Integer>> tupleState;

                @Override
                public void open(final Configuration parameters) throws Exception {
                    tupleState = getRuntimeContext().getState(
                        new ValueStateDescriptor<>("tupleState", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                        })));
                }

                @Override
                public void flatMap1(final String value, final Collector<Tuple2<String, Integer>> out) throws Exception {
                    final Tuple2<String, Integer> current = tupleState.value();
                    if (Objects.nonNull(current) && Objects.nonNull(current.f1)) {
                        out.collect(new Tuple2<>(value, current.f1));
                        tupleState.clear();
                    }
                    tupleState.update(Tuple2.of(value, null));
                }

                @Override
                public void flatMap2(final Integer value, final Collector<Tuple2<String, Integer>> out) throws Exception {
                    final Tuple2<String, Integer> current = tupleState.value();
                    if (Objects.nonNull(current) && Objects.nonNull(current.f0)) {
                        out.collect(new Tuple2<>(current.f0, value));
                        tupleState.clear();
                    }
                    tupleState.update(Tuple2.of(null, value));
                }
            })
            .print();

        // Execute the Flink job
        env.execute("functionTest");
    }

    @Test
    @SneakyThrows
    public void testJoin01() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromSequence(1, 99)
            .map(i -> Tuple2.of(String.valueOf(i), i + 1)) // Generate the word
            .returns(TypeInformation.of(new TypeHint<>() {
            }))
            .map(value -> {
                TimeUnit.MILLISECONDS.sleep(1000L);
                return value;
            })
            .returns(TypeInformation.of(new TypeHint<>() {
            }));
        final SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.fromSequence(1, 99)
            .map(i -> Tuple2.of(String.valueOf(i), i - 1)) // Generate the word
            .returns(TypeInformation.of(new TypeHint<>() {
            }))
            .map(value -> {
                TimeUnit.MILLISECONDS.sleep(1000L);
                return value;
            })
            .returns(TypeInformation.of(new TypeHint<>() {
            }));

        stream1
            .join(stream2)
            .where(t -> t.f0) // Key selector for stream1
            .equalTo(t -> t.f0) // Key selector for stream2
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1L))) // Time window
            .apply((t1, t2) -> "Joined: " + t1 + " & " + t2)
            .print();

        env.execute("operatorTest");
    }
}
