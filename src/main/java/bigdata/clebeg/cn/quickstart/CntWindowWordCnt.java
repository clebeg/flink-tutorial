package bigdata.clebeg.cn.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class CntWindowWordCnt {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream(args[0], 9999);
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne =
                source.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });
        KeyedStream<Tuple2<String, Long>, Tuple> keyByStream = wordAndOne.keyBy(0);
        WindowedStream<Tuple2<String, Long>, Tuple, GlobalWindow> windowStream = keyByStream.countWindow(5);
        SingleOutputStreamOperator<Tuple2<String, Long>> outputStream = windowStream.sum(1);
        outputStream.print().setParallelism(1);
        env.execute("CntWindowWordCnt");
    }
}
