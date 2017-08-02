package com.cloudcomputing.samza.pitt_cabs;

/**
 * Created by caoyi on 16/12/7.
 *
 *
 *
 */
import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import scala.util.parsing.combinator.testing.Str;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 *
 *
 * This class to solve task2, though I didnt find how to serialize a class type, thus I used String as k, v of store
 *
 *
 * key should be "blockId" + "driverId"
 *
 * value should be the info of driver eg.
 *      sb.append(latitude).append("\t").append(longitude).append("\t").append(salary).append("\t").append(gender).append("\t").append(rating);

 *
 *
 *
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {

    /* Define per task state here. (kv stores etc) */
    private double MAX_MONEY = 100.0;
    private KeyValueStore<String, String> driverInfo;
    // private KeyValueStore<String, String> price;
    private KeyValueStore<String, String> counts;


    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
        // get the driver info
        // the name of ""
        driverInfo = (KeyValueStore<String, String>) context.getStore("driver-loc");
        // price = (KeyValueStore<String, String>) context.getStore("price");
        counts = (KeyValueStore<String, String>) context.getStore("count");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        // The main part of your code. Remember that all the messages for a
        // particular partition
        // come here (somewhat like MapReduce). So for task 1 messages for a
        // blockId will arrive
        // at one task only, thereby enabling you to do stateful stream
        // processing.
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
            // Handle Driver Location messages
            processDriver((Map<String, Object>) envelope.getMessage());

        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // Handle Event messages
            processEvent((Map<String, Object>) envelope.getMessage(), collector);

        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }


    /*This method is used to process driver-locations stream*/
    private void processDriver(Map<String, Object> map) {
        Integer blockId = (Integer) map.get("blockId");
        Integer driverId = (Integer) map.get("driverId");
        String key = blockId + ":" + driverId;
        // process driver events to update t
        Double latitude = (Double) map.get("latitude");
        Double longitude = (Double) map.get("longitude");
        if (driverInfo.get(key) == null) {
            return;
        }
        String driver = driverInfo.get(key);
        String[] properties = driver.split("\t");
        StringBuilder sb = new StringBuilder();
        sb.append(latitude).append("\t").append(longitude).append("\t");
        for (int i = 2; i < properties.length; i++) {
            sb.append(properties[i]).append("\t");
        }
        sb.deleteCharAt(sb.length() - 1);
        driverInfo.put(key, sb.toString());
    }
    /*This method is used to process events stream*/
    private  void processEvent (Map<String, Object> map, MessageCollector collector) {
        String type = (String) map.get("type");
        if (type.equals("LEAVING_BLOCK")) {
            processLeave(map);
        } else if (type.equals("ENTERING_BLOCK")) {
            processEnter(map);
        } else if (type.equals("RIDE_REQUEST")) {
            processRequest(map, collector);
        } else if (type.equals("RIDE_COMPLETE")) {
            processComplete(map);
        }
    }


    private void processLeave (Map<String, Object> map) {

        Integer blockId = (Integer) map.get("blockId");
        Integer driverId = (Integer) map.get("driverId");
        String key = blockId + ":" + driverId;
        driverInfo.delete(key);
    }

    private void processEnter(Map<String, Object> map) {
        /// update driverInfo
        Integer blockId = (Integer) map.get("blockId");
        Integer driverId = (Integer) map.get("driverId");
        String key = blockId + ":" + driverId;
        String status = (String) map.get("status");
        if (status.equals("AVAILABLE")) {
            Double latitude = (Double) map.get("latitude");
            Double longitude = (Double) map.get("longitude");
            Integer salary = (Integer) map.get("salary");
            String gender = (String) map.get("gender");
            Double rating = (Double) map.get("rating");
            StringBuilder sb = new StringBuilder();
            sb.append(latitude).append("\t").append(longitude).append("\t").append(salary).append("\t").append(gender).append("\t").append(rating);
            driverInfo.put(key, sb.toString());
        }

    }
    private void processRequest (Map<String, Object> map, MessageCollector collector) {
        Integer blockId = (Integer) map.get("blockId");
        Integer clientId = (Integer) map.get("clientId");
        // ref:https://github.com/ept/newsfeed/blob/master/src/main/java/com/martinkl/samza/newsfeed/FanOutTask.java
        KeyValueIterator<String, String> iterator = driverInfo.range(blockId + ":", blockId + ";");

        int curr_count = 0;
        Double score = Double.MIN_VALUE;
        Integer driverId = null;

        while (iterator.hasNext()) {
            curr_count++;
            Entry<String, String> driver = iterator.next();
            double driverScore = countScore(map, driver);
            if (score < driverScore) {
                score = driverScore;
                driverId = getDriverId(driver);
            }
        }

        // then save into the db
        String previous = counts.get(String.valueOf(blockId));
        Double SPF;
        if (previous == null) {
            SPF = 1.0;
            counts.put(blockId.toString(), Integer.toString(curr_count));
        } else {
            // the second request
            counts.put(blockId.toString(), Integer.toString(curr_count));
            curr_count += Integer.parseInt(previous);
            Double A = curr_count / 2.0;
            if (A >= 4.5) {
                SPF = 1.0;
            } else {
                Double SF = (2 * (4.5 - A) / (2.25 - 1));
                SPF =  1 + SF;
            }
        }

        // then save into the db
        Map<String, Object> result = new HashMap<String, Object>();
        result.put("clientId", String.valueOf(clientId));
        if (driverId != null) {
            result.put("driverId", driverId);
            String key = blockId + "\t" + driverId;
            driverInfo.delete(key);
        } else {
            result.put("driverId", 0);
        }
        result.put("priceFactor", SPF);

        System.out.println("clientId:" + clientId  + " driverId:" + driverId + " priceFator:" + SPF);
        collector.send(new OutgoingMessageEnvelope(DriverMatchConfig.MATCH_STREAM,
                null, null, result));

    }

    private Integer getDriverId ( Entry<String, String> driver) {
        return Integer.parseInt(driver.getKey().split(":")[1]);
    }

    private Double countScore(Map<String, Object> map, Entry<String, String> driver) {
        // count score ...
        Double cli_latitude = (Double) map.get("latitude");
        Double cli_longitude = (Double) map.get("longitude");
        String gender_preference = (String) map.get("gender_preference");
//   sb.append(latitude).append("\t").append(longitude).append("\t").append(salary).append("\t").append(gender).append("\t").append(rating);
        String[] properties = driver.getValue().split("\t");
        Double lat = Double.parseDouble(properties[0]);
        Double log = Double.parseDouble(properties[1]);
        String driverGender = properties[3];
        Double rating = Double.parseDouble(properties[4]);
        Integer salary = Integer.parseInt(properties[2]);

        Double client_driver_distance = Math.sqrt(Math.pow(lat - cli_latitude, 2) + Math.pow(log - cli_longitude, 2));
        Double distance_score = Math.pow(1 * Math.E, -1 * client_driver_distance);
        Double rating_score = rating / 5.0;
        Double salary_score = 1 - salary / MAX_MONEY;
        int gender_score = 0;
        if (driverGender.equals(gender_preference) || gender_preference.equals("N")) {
            gender_score = 1;
        }
        Double match_score = distance_score * 0.4 + gender_score * 0.2 + rating_score * 0.2 + salary_score * 0.2;
        return match_score;
    }

    /*the method to process complete*/
    private void processComplete(Map<String, Object> map) {

        Integer blockId = (Integer) map.get("blockId");
        Integer driverId = (Integer) map.get("driverId");
        String key = blockId + ":" + driverId;
        Double latitude = (Double) map.get("latitude");
        Double longitude = (Double) map.get("longitude");
        Integer salary = (Integer) map.get("salary");
        String gender = (String) map.get("gender");
        Double rating = (Double) map.get("rating");
        StringBuilder sb = new StringBuilder();
        sb.append(latitude).append("\t").append(longitude).append("\t").append(salary).append("\t").append(gender).append("\t").append(rating);
        driverInfo.put(key, sb.toString());
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        // this function is called at regular intervals, not required for this
        // project
    }

}
