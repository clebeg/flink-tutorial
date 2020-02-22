package bigdata.clebeg.cn.utils;

import bigdata.clebeg.cn.model.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

public class FlinkSourceUtil {
    public static DataStream<UserBehavior> userBehaviorSource(StreamExecutionEnvironment env, String userBehaviorFile) {
        URL inputCsv = FlinkSourceUtil.class.getClassLoader().getResource(userBehaviorFile);
        System.out.printf("input csv path = %s", inputCsv.getPath());
        SingleOutputStreamOperator<UserBehavior> inputSource = env.readTextFile(inputCsv.getPath())
                .map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String line) throws Exception {
                String[] fields = line.split(",");
                long userId = Long.parseLong(fields[0]);
                long itemId = Long.parseLong(fields[1]);
                long categoryId = Long.parseLong(fields[2]);
                String eventId = fields[3];
                long eventTime = Long.parseLong(fields[4]);
                return UserBehavior.apply(userId, itemId, categoryId, eventId, eventTime);
            }
        });
        return inputSource;
    }
}
