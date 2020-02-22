package bigdata.clebeg.cn;

import bigdata.clebeg.cn.model.UserBehavior;
import bigdata.clebeg.cn.utils.DBUtil;
import bigdata.clebeg.cn.utils.FlinkSourceUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JoinDimFromMysql {
    private final static String csvSourceFile = "UserBehaviorTest.csv";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<UserBehavior> source = FlinkSourceUtil.userBehaviorSource(env, csvSourceFile);
        SingleOutputStreamOperator<Tuple2<UserBehavior, String>> finalStream = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(UserBehavior userBehavior) {
                return userBehavior.eventTime * 1000;
            }
        }).map(new JoinDimMapFunction());
        finalStream.print().setParallelism(1);
        env.execute("JoinDimFromMysql");
    }
}

class JoinDimMapFunction extends RichMapFunction<UserBehavior, Tuple2<UserBehavior, String>> {
    Connection connection = null;
    private String url = "jdbc:mysql://localhost:3306/stream_tutorial?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
    private String username = "root";
    private String password = "123456";
    private String sql = "select event_name from t_event_dim where event_id = ?";
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DBUtil.connect(url, username, password);
    }
    public JoinDimMapFunction() {
    }
    @Override
    public Tuple2<UserBehavior, String> map(UserBehavior userBehavior) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, userBehavior.eventId);

        ResultSet resultSet = preparedStatement.executeQuery();
        String eventName = "";
        while (resultSet.next()) {
            System.out.println(resultSet);
            eventName = resultSet.getString("event_name");
        }
        return Tuple2.of(userBehavior, eventName);
    }

    @Override
    public void close() throws Exception {
        super.close();
        DBUtil.close(connection);
    }
}
