package learn.flink;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

}
