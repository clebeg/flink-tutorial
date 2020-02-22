package bigdata.clebeg.cn.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;

/**
 * 学习使用时间窗口 使用的 数据时间 使用滚动或者滑动窗口
 * 数据格式 暂时不设置水位线
 * 1997-01-01 12:00:00,flink,1
 * 1997-01-01 12:00:01,flink,1
 * 1997-01-01 12:00:02,flink,1
 * 1997-01-01 12:00:07,flink,1
 * 1997-01-01 12:00:08,flink,1
 * 1997-01-01 12:00:14,flink,1
 */
public class EventTimeWindowWordCnt {

    public static void main(String[] args) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 要使用event_time 必须显示设置
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> source = env.socketTextStream("0.0.0.0", 9999);

        // 必须先从数据中解析出 Long 类型的时间戳
        /** AscendingTimestampExtractor 默认时间是按正序到达 water mark 延迟1毫秒
        SingleOutputStreamOperator<String> eventTimeStream = source.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String line) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        });*/
        // BoundedOutOfOrdernessTimestampExtractor 周期性的生成 watermark 默认是 200 毫秒生成一次
        SingleOutputStreamOperator<String> eventTimeStream = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream =
                eventTimeStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String lines) throws Exception {
                String[] fields = lines.split(",");
                String word = fields[1];
                Integer num = Integer.parseInt(fields[2]);
                return Tuple2.of(word, num);
            }
        });
        int parallelism = mapStream.getParallelism();
        System.out.println("map stream parallelism = " + parallelism);

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapStream.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindowStream =
                keyedStream.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<Tuple2<String, Integer>> finalStream = timeWindowStream.sum(1);
        finalStream.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                Long cur = context.currentWatermark();
                RuntimeContext cont = getRuntimeContext();
                int index = cont.getIndexOfThisSubtask();
                System.out.println(cur + ":" + index + ">" + value);
            }
        });
        env.execute("EventTimeWindowWordCnt");
    }
}
