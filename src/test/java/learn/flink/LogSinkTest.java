package learn.flink;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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

      try (final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment()) {

         final SingleOutputStreamOperator<Tuple2<String, Integer>> dataStream = env
                 .fromData("aa bb cc", "dd ee aa", "bb ff cc gg")
//              .socketTextStream("192.168.0.129", 9999)
                 .flatMap(new Splitter())
                 .keyBy(value -> value.f0)
//              .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(1)))
                 .sum(1);

         dataStream.print();

         env.execute("Window WordCount");
      }

   }

}
