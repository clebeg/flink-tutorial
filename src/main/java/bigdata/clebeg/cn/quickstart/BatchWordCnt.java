package bigdata.clebeg.cn.quickstart;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCnt {
    public static void main(String[] args) throws Exception {
        // build env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> inputDS = env.readTextFile("src/main/resources/helloworld.txt");
        AggregateOperator<Tuple2<String, Integer>> finalDS = inputDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : sentence.split("\\s")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        }).groupBy(0).sum(1);

        finalDS.print();
    }
}
