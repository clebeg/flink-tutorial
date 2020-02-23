package bigdata.clebeg.cn.deep;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AsyncDimJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socket = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple4<String, String, Double, Double>> resultStream =
                AsyncDataStream.unorderedWait(socket, new ToAddressAsyncFun(args[0]), 0,
                        TimeUnit.MICROSECONDS, 10);
        resultStream.print().setParallelism(1);
        env.execute("AsyncDimJoinExample");
    }
}


class ToAddressAsyncFun extends RichAsyncFunction<String, Tuple4<String, String, Double, Double>> {
    // transient 表示不参与序列化
    private transient CloseableHttpAsyncClient asyncClient = null;
    private String apiKey = "";
    private static String urlBegin = "https://apis.map.qq.com/ws/geocoder/v1/?address=";
    private static String urlEnd = "&key=";
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RequestConfig conf = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000) // 设置建立http连接的时间不超过 3s
                .setConnectionRequestTimeout(3000) // 设置 http 请求时间最大 3s
                .build();
        asyncClient = HttpAsyncClients.custom()
                .setMaxConnPerRoute(20) // 设置最多同时异步请求 20 个，超过就阻塞
                .setDefaultRequestConfig(conf)
                .build();
        asyncClient.start();
    }

    public ToAddressAsyncFun() {
    }

    public ToAddressAsyncFun(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public void close() throws Exception {
        super.close();
        asyncClient.close();
    }

    @Override
    public void asyncInvoke(String address, ResultFuture<Tuple4<String, String, Double, Double>> resultFuture) {
        String requestUrl = urlBegin + address + urlEnd + apiKey;
        HttpGet request = new HttpGet(requestUrl);
        Future<HttpResponse> future = asyncClient.execute(request, null);

        CompletableFuture.supplyAsync(() -> {
            try {
                HttpResponse response = future.get();

                if (response.getStatusLine().getStatusCode() != 200) {
                    return Tuple3.of("", 0.0, 0.0);
                }
                String resContent = IOUtils.toString(response.getEntity().getContent());
                JSONObject jsonObj = JSON.parseObject(resContent);
                if (!jsonObj.containsKey("result")) {
                    return Tuple3.of("", 0.0, 0.0);
                }
                JSONObject result = jsonObj.getJSONObject("result");
                String title = result.getString("title");
                JSONObject location = result.getJSONObject("location");
                Double lng = location.getDouble("lng");
                Double lat = location.getDouble("lat");
                return Tuple3.of(title, lng, lat);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("BadHere");
                return Tuple3.of("", 0.0, 0.0);
            }
        }).thenAccept(res -> resultFuture.complete(Collections.singleton(Tuple4.of(address, res.f0, res.f1, res.f2))));
    }
}