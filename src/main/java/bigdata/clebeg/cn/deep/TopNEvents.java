package bigdata.clebeg.cn.deep;

import bigdata.clebeg.cn.model.UserBehavior;
import bigdata.clebeg.cn.utils.FlinkSourceUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.MinMaxPriorityQueue;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;

public class TopNEvents {
    private final static String userBehaviorCsv = "UserBehavior.csv";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<UserBehavior> source = FlinkSourceUtil.userBehaviorSource(env, userBehaviorCsv);
        SingleOutputStreamOperator<UserBehavior> eventStream = source.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(UserBehavior userBehavior) {
                return userBehavior.eventTime * 1000;
            }
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior userBehavior) throws Exception {
                return userBehavior.eventId.equalsIgnoreCase("pv");
            }
        });
//        eventStream.print().setParallelism(1);
        KeyedStream<UserBehavior, Tuple> keyedStream = eventStream.keyBy("itemId");
        SingleOutputStreamOperator<Tuple3<Long, Long, Long>> aggStream = keyedStream.timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new MyAggFun(), new WindowFunction<Long, Tuple3<Long, Long, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple key, TimeWindow timeWindow, Iterable<Long> it, Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
                        Long itemId = ((Tuple1<Long>) key).f0;
                        Long pv = it.iterator().next();
                        long endTime = timeWindow.getEnd();
                        collector.collect(Tuple3.of(itemId, pv, endTime));
                    }
                });
        SingleOutputStreamOperator<String> finalStream = aggStream.keyBy(2).process(
                new KeyedProcessFunction<Tuple, Tuple3<Long, Long, Long>, String>() {

            ValueState<MinMaxPriorityQueue> topN = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                topN = getRuntimeContext().getState(new ValueStateDescriptor<MinMaxPriorityQueue>("top_n", MinMaxPriorityQueue.class));
            }

            @Override
            public void processElement(Tuple3<Long, Long, Long> input, Context context, Collector<String> collector) throws Exception {
                if (topN.value() == null) {
                    MinMaxPriorityQueue<Tuple3<Long, Long, Long>> queue = MinMaxPriorityQueue.orderedBy(new Comparator<Tuple3<Long, Long, Long>>() {
                        @Override
                        public int compare(Tuple3<Long, Long, Long> o1, Tuple3<Long, Long, Long> o2) {
                            return - o1.f1.compareTo(o2.f1);
                        }
                    }).maximumSize(5).create();
                    queue.add(input);
                    topN.update(queue);
                } else {
                    MinMaxPriorityQueue<Tuple3<Long, Long, Long>> queue = topN.value();
                    queue.add(input);
                    topN.update(queue);
                }
                context.timerService().registerEventTimeTimer(input.f2 + 1);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
                MinMaxPriorityQueue<Tuple3<Long, Long, Long>> queue = topN.value();
                StringBuilder sb = new StringBuilder();
                for (Tuple3<Long, Long, Long> tuple : queue) {
                    sb.append(tuple.toString() + "|");
                }
                out.collect(sb.toString());
            }
        });
        finalStream.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                Long curTime = context.timestamp();
                System.out.println("curTime=" + curTime + ":" + value);
            }
        });
        env.execute("TopNEvents");
    }
}

class MyAggFun implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long agg) {
        return agg + 1L;
    }

    @Override
    public Long getResult(Long res) {
        return res;
    }

    @Override
    public Long merge(Long acc1, Long acc2) {
        return acc1 + acc2;
    }
}