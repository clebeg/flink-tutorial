package bigdata.clebeg.cn.quickstart;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 学习使用时间窗口 使用的 系统时间 使用滚动或者滑动窗口
 * 数据格式:
 * flink,1
 * spark,2
 * hadoop,3
 * hue,4
 */
public class TimeWindowWordCnt {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("0.0.0.0", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream =
                source.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String lines) throws Exception {
                String[] fields = lines.split(",");
                String word = fields[0];
                Integer num = Integer.parseInt(fields[1]);
                return Tuple2.of(word, num);
            }
        });
        int parallelism = mapStream.getParallelism();
        System.out.println("map stream parallelism = " + parallelism);

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapStream.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindowStream =
                keyedStream.timeWindow(Time.seconds(5), Time.seconds(1));
        SingleOutputStreamOperator<Tuple2<String, Integer>> finalStream = timeWindowStream.sum(1);
        finalStream.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                Long cur = context.timestamp()/1000;
                RuntimeContext cont = getRuntimeContext();
                int index = cont.getIndexOfThisSubtask();
                System.out.println(cur + ":" + index + ">" + value);
            }
        });
        env.execute("TimeWindowWordCnt");
    }
}
