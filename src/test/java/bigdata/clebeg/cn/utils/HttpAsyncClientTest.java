package bigdata.clebeg.cn.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import junit.framework.TestSuite;
import org.apache.commons.io.IOUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class HttpAsyncClientTest extends TestSuite {
    @Test
    public void testHttpGet() throws ExecutionException, InterruptedException, IOException {
        try (CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault()) {
            httpclient.start();
            HttpGet request = new HttpGet("http://httpbin.org/get");
            Future<HttpResponse> future = httpclient.execute(request, null);
            HttpResponse response = future.get();
            System.out.println("Response: " + response.getStatusLine());
            System.out.println("Shutting down");
        }
        System.out.println("Done");
    }
    @Test
    public void testTencentApi() throws ExecutionException, InterruptedException, IOException {
        try (CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault()) {
            String address = "湖北省武汉市华南农业市场";
            httpclient.start();
            HttpGet request = new HttpGet("https://apis.map.qq.com/ws/geocoder/v1/?address=" + address + "&key=OB4BZ-D4W3U-B7VVO-4PJWW-6TKDJ-WPB77");
            Future<HttpResponse> future = httpclient.execute(request, null);
            HttpResponse response = future.get();
            System.out.println("Response: " + response.getStatusLine());
            String resContent = IOUtils.toString(response.getEntity().getContent());
            System.out.println(resContent);
            System.out.println("Shutting down");
        }
        System.out.println("Done");
    }

    @Test
    public void testParseTencentApiRes() {
        String resJson = "{\n" +
                "    \"status\":0,\n" +
                "    \"message\":\"query ok\",\n" +
                "    \"result\":{\n" +
                "        \"title\":\"海淀西大街74号\",\n" +
                "        \"location\":{\n" +
                "            \"lng\":116.30676,\n" +
                "            \"lat\":39.98296\n" +
                "        },\n" +
                "        \"ad_info\":{\n" +
                "            \"adcode\":\"110108\"\n" +
                "        },\n" +
                "        \"address_components\":{\n" +
                "            \"province\":\"北京市\",\n" +
                "            \"city\":\"北京市\",\n" +
                "            \"district\":\"海淀区\",\n" +
                "            \"street\":\"海淀西大街\",\n" +
                "            \"street_number\":\"74\"\n" +
                "        },\n" +
                "        \"similarity\":0.8,\n" +
                "        \"deviation\":1000,\n" +
                "        \"reliability\":7,\n" +
                "        \"level\":9\n" +
                "    }\n" +
                "}";
        JSONObject jsonObj = JSON.parseObject(resJson);
        if (jsonObj.containsKey("result")) {
            JSONObject result = jsonObj.getJSONObject("result");
            String title = result.getString("title");
            JSONObject location = result.getJSONObject("location");
            Double lng = location.getDouble("lng");
            Double lat = location.getDouble("lat");

            Tuple3<String, Double, Double> res = Tuple3.of(title, lng, lat);
            System.out.println(res);
        }
    }
}
