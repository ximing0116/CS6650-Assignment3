import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

public class Metrics {

    private Queue<RequestMetrics> metricsList;

    public Metrics(Queue<RequestMetrics> metricsList) {
        this.metricsList = metricsList;
    }

    public void computeAndPrintStats(String requestType) {
        List<Long> latencies = metricsList.stream()
                .filter(m -> m.requestType.equals(requestType))
                .map(m -> m.latency)
                .sorted()
                .collect(Collectors.toList());

        long total = latencies.stream().mapToLong(Long::longValue).sum();
        double mean = (double) total / latencies.size();
        double median = latencies.size() % 2 == 0 ?
                (latencies.get(latencies.size()/2 - 1) + latencies.get(latencies.size()/2)) / 2.0 :
                latencies.get(latencies.size()/2);
        long p99 = latencies.get((int)(0.99 * latencies.size()));
        long min = latencies.get(0);
        long max = latencies.get(latencies.size() - 1);

        System.out.println("====================================================");
        System.out.println(requestType + " Stats:");
        System.out.println("Mean response time (ms): " + mean);
        System.out.println("Median response time (ms): " + median);
        System.out.println("p99 response time (ms): " + p99);
        System.out.println("Min response time (ms): " + min);
        System.out.println("Max response time (ms): " + max);
    }

    public void saveMetricsToFile(String filename) {
        try (PrintWriter writer = new PrintWriter(new File(filename))) {
            writer.println("Start Time, Request Type, Latency, Response Code");
            metricsList.forEach(writer::println);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}

