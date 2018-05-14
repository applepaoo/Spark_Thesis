import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Scanner;

public class Producer_PowerData_CC_HBase {
    public static void main(String[] args) throws JSONException, IOException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "140.128.98.31:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        Producer<String, String> producer = new KafkaProducer<>(props);
        System.out.println("準備傳送");


        //先抓網頁API的JSON

        URLConnection connection = new URL("http://icems.thu.edu.tw/config/getpower.php").openConnection();
        Scanner scanner = new Scanner(connection.getInputStream());

        String PowerData_CC = scanner.useDelimiter("\\A").next();
        //System.out.println(PowerData_CC);


        //在測試抓JSON的資料


        JSONArray k;
        JSONObject i;

        k = new JSONArray(PowerData_CC);//長度為18


        for (int p = 0; p < k.length() - 1; p++) {

            i = k.getJSONObject(p);

            if (i.getDouble("11") == 0 || i.getDouble("14") == 0 || i.getDouble("8") == 0) {

                double v = 0;

                producer.send(new ProducerRecord<String, String>("PowerData_CC_HBase", i.getString("0"),
                        i.getString("1").substring(0, 19) + "," //時間
                                + i.getString("0") + "," //電表ID
                                + v + "," //V值
                                + i.getString("8") + "," //I
                                + i.getString("14") + "," //PF
                                + i.getString("11") //P

                ));

                System.out.println(new ProducerRecord<String, String>("PowerData_CC_HBase", i.getString("0"),
                        i.getString("1").substring(0, 19) + "," //時間
                                + i.getString("0") + "," //電表ID
                                + v + "," //V值
                                + i.getString("8") + "," //I
                                + i.getString("14") + "," //PF
                                + i.getString("11") //P

                ));


            } else {

                double v = i.getDouble("11") / i.getDouble("14") / i.getDouble("8");
                DecimalFormat df = new DecimalFormat("##.00"); //V值取小數點後兩位
                v = Double.parseDouble(df.format(v));

                producer.send(new ProducerRecord<String, String>("PowerData_CC_HBase", i.getString("0"),
                        i.getString("1").substring(0, 19) + "," //時間
                                + i.getString("0") + "," //電表ID
                                + v + "," //V值
                                + i.getString("8") + "," //I
                                + i.getString("14") + "," //PF
                                + i.getString("11") //P

                ));

                System.out.println(new ProducerRecord<String, String>("PowerData_CC_HBase", i.getString("0"),
                        i.getString("1").substring(0, 19) + "," //時間
                                + i.getString("0") + "," //電表ID
                                + v + "," //V值
                                + i.getString("8") + "," //I
                                + i.getString("14") + "," //PF
                                + i.getString("11")  //P

                ));


            }
        }

        int count_data = k.length() - 1;
        System.out.println("共傳送了" + count_data + "筆資料");
        producer.close();


    }
}
