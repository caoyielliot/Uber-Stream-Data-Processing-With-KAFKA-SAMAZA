import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

/**
 * @author caoyi
 *
 * write code to create streams and feed messages to the streams remotely to your samza cluster.
 */
public class DataProducer {

    public static void main(String[] args) {
        /*
            Task 1:
            In Task 1, you need to read the content in the tracefile we give to you, 
            and create two streams, feed the messages in the tracefile to different 
            streams based on the value of "type" field in the JSON string.

            Please note that you're working on an ec2 instance, but the streams should
            be sent to your samza cluster. Make sure you can consume the topics on the
            master node of your samza cluster before make a submission. 
        */

         // Reader from trace file to create the two streams
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.31.28.167:9092");
        // The acks config controls the criteria under which requests are considered complete. The "all" setting we have specified will
        // result in blocking on the full commit of the record, the slowest but most durable setting.
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        //{"blockId":5648,"driverId":9898,"latitude":40.7905811,"longitude":-73.9739574,"type":"ENTERING_BLOCK","status":"AVAILABLE","rating":1.62,"salary":4,"gender":"M"}
        //{"driverId":9898,"blockId":5648,"latitude":40.7905811,"longitude":-73.9739574,"type":"DRIVER_LOCATION"}
        int count = 0;
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File("/home/yc2/Project4_3/DataProducer/tracefile")));
            String line = null;
            while ((line = br.readLine()) != null) {
             //   System.out.println(line);
                if (line.isEmpty()) {
                    continue;
                }
                // get the "type" field from the json string
                String topic = getTopic(line);
                if (topic.equals("nil")) {
                    continue;
                }
                if (topic.equals("driver-locations")) {
                    count++;
                }
                String partition = getPartition(line);
                // producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
                ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, partition, line);
                producer.send(rec, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e != null)
                            e.printStackTrace();
                        System.out.println(metadata + ": The offset of the record we just sent is: " + metadata.offset());
                    }
                });
            }
            System.out.println(count);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.close();
    }
    private static  String getTopic (String line) {
        String pattern = "$.type";
        String topic = JsonPath.read(line, pattern).toString();
        if (topic.equals("DRIVER_LOCATION")) {
            return "driver-locations";
        }
        if (topic.equals("LEAVING_BLOCK")) {
            return "events";
        }
        if (topic.equals("ENTERING_BLOCK")) {
            return "events";
        }
        if (topic.equals("RIDE_REQUEST")) {
            return "events";
        }
        if (topic.equals("RIDE_COMPLETE")) {
            return "events";
        }
        return "nil";
    }
    private static String getPartition (String line) {
        String pattern = "$.blockId";
        String partition = JsonPath.read(line, pattern).toString();
        return partition;
    }
}
