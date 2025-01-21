package learn.flink;

import java.util.concurrent.TimeUnit;
import learn.flink.model.SystemUser;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.source.JdbcSource;
import org.apache.flink.connector.jdbc.source.reader.extractor.ResultExtractor;
import org.apache.flink.connector.jdbc.split.JdbcGenericParameterValuesProvider;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

@Slf4j
public class JdbcConnectorTest {

   public final static String user = "root";
   public final static String password = "ulala@123";
   public final static String driver = "com.mysql.cj.jdbc.Driver";
   public final static String jdbcUrl = "jdbc:mysql://3.231.250.228:13307/ulala_main?useSSL=false&characterEncoding=UTF-8&serverTimezone=America/Vancouver&allowPublicKeyRetrieval=true";

   public static class SystemUserResultExtractor implements ResultExtractor<SystemUser> {
      @Override
      public SystemUser extract(final ResultSet resultSet) throws SQLException {
         return SystemUser.builder()
                 .id(resultSet.getLong("id"))
                 .username(resultSet.getString("username"))
                 .password(resultSet.getString("password"))
                 .email(resultSet.getString("email"))
                 .build();
      }
   }

   @Test
   @SneakyThrows
   public void testJdbcSource01() {

      Serializable[][] queryParameters1 = new Serializable[1][2];
      queryParameters1[0] = new Serializable[]{99L, 9999L};

      final JdbcSource<Row> jdbcSource = JdbcSource.<Row>builder()
              .setResultExtractor(rs -> {
                 final Row row = Row.withNames();
                 row.setField("id", rs.getLong("id"));
                 row.setField("username", rs.getString("username"));
                 row.setField("password", rs.getString("password"));
                 row.setField("email", rs.getString("email"));
                 return row;
              })
              .setTypeInformation(new TypeHint<Row>() {
              }.getTypeInfo())
              .setUsername(user)
              .setPassword(password)
              .setDriverName(driver)
              .setDBUrl(jdbcUrl)
              .setSql("SELECT * from system_users su where su.id <= ? and su.dept_id <= ?")
              .setJdbcParameterValuesProvider(new JdbcGenericParameterValuesProvider(queryParameters1))
              .build();

      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.fromSource(jdbcSource, WatermarkStrategy.forMonotonousTimestamps(), "jdbcSource")
              .map(row -> SystemUser.builder()
                      .id(row.getFieldAs("id"))
                      .username(row.getFieldAs("username"))
                      .password(row.getFieldAs("password"))
                      .email(row.getFieldAs("email"))
                      .build())
              .print();


      env.execute("jdbcTest");
   }


   @Test
   @SneakyThrows
   public void testJdbcSource02() {

      Serializable[][] queryParameters1 = new Serializable[1][2];
      queryParameters1[0] = new Serializable[]{99L, 9999L};

      final JdbcSource<SystemUser> jdbcSource = JdbcSource.<SystemUser>builder()
              .setResultExtractor(new SystemUserResultExtractor())
              .setTypeInformation(new TypeHint<SystemUser>() {
              }.getTypeInfo())
              .setUsername(user)
              .setPassword(password)
              .setDriverName(driver)
              .setDBUrl(jdbcUrl)
              .setSql("SELECT * from system_users su where su.id <= ? and su.dept_id <= ?")
              .setJdbcParameterValuesProvider(new JdbcGenericParameterValuesProvider(queryParameters1))
              .build();

      final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      env.fromSource(jdbcSource, WatermarkStrategy.forMonotonousTimestamps(), "jdbcSource").print();


      env.execute("jdbcTest");
   }

   @Test
    public void testMaxBy() {

       final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       env.fromSequence(1L, 99L)
           .map(i -> SystemUser.builder().id(i).username(String.format("usr%s", i)).password(String.format("pwd%s", i)).email(String.format("usr%s@test.ca", i)).build())
           .returns(TypeInformation.of(new TypeHint<>() {
           }))
           .map( value -> {
               TimeUnit.MILLISECONDS.sleep(1000L);
               return value;
           })
           .returns(TypeInformation.of(new TypeHint<>() {
           }))
           .keyBy(u->u.getId())
           .max("")

   }

}
