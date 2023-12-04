import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleClient {
    public static final boolean DEBUG = true;
    public static final AtomicInteger SUCCESSFUL_REQUESTS = new AtomicInteger(0);
    public static final AtomicInteger FAILED_NONE_OK = new AtomicInteger(0);
    public static final AtomicInteger FAILED_WITH_EXCEPTION = new AtomicInteger(0);
    public static final Queue<RequestMetrics> METRICS_LIST = new ConcurrentLinkedQueue<>();
    public static ObjectMapper objectMapper = new ObjectMapper();

    private static void debugPrintln(String message) {
        if (DEBUG) {
            System.out.println(message);
        }
    }

    private static PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();

    public static class AlbumResponse {
        private String albumId;
        private int imageSize;

        // getters and setters
        public String getAlbumId() {
            return albumId;
        }

        public void setAlbumId(String albumId) {
            this.albumId = albumId;
        }

        public int getImageSize() {
            return imageSize;
        }

        public void setImageSize(int imageSize) {
            this.imageSize = imageSize;
        }
    }

    static byte[] imageBytes = new byte[3457];

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 5) {
            System.out.println("Usage: SimpleClient <threadGroupSize> <numThreadGroups> <delay> <IPAddr> <reviewAddr>");
            return;
        }
        cm.setMaxTotal(5000);
        cm.setDefaultMaxPerRoute(5000);

        CloseableHttpClient httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .build();
        SecureRandom secureRandom = new SecureRandom();
        secureRandom.nextBytes(imageBytes);

        int threadGroupSize = Integer.parseInt(args[0]);
        int numThreadGroups = Integer.parseInt(args[1]);
        int delay = Integer.parseInt(args[2]);
        String IPAddr = args[3];
        String reviewAddr = args[4];

        // sendRequests(IPAddr, 1);

        // Initialization phase with 10 threads
//        ExecutorService initializationExecutor = Executors.newFixedThreadPool(10);
//        for (int i = 0; i < 10; i++) {
//            initializationExecutor.submit(() -> sendRequests(IPAddr, 100));
//        }
//        initializationExecutor.shutdown();
//        initializationExecutor.awaitTermination(1, TimeUnit.HOURS);

        long startTime = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(numThreadGroups * threadGroupSize);
        for (int i = 0; i < numThreadGroups; i++) {
            for (int j = 0; j < threadGroupSize; j++) {
                executor.submit(() -> sendPairRequest(httpClient, IPAddr, reviewAddr,100));
            }
            Thread.sleep(delay * 1000);
        }

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        long endTime = System.currentTimeMillis();

        System.out.println("====================================================");
        System.out.println("Execution started at: " + startTime);
        System.out.println("Execution ended at: " + endTime);
        System.out.println("====================================================");
        System.out.println("Total execution time (sec): " + (endTime - startTime) / 1000);
        System.out.println("Total throughput (reqs/sec): " + 1000 * (SUCCESSFUL_REQUESTS.get()) / (endTime - startTime));
        System.out.println("====================================================");
        System.out.println("Total successful total: " + SUCCESSFUL_REQUESTS.get());
        System.out.println("Total failed non-200 total: " + FAILED_NONE_OK.get());
        System.out.println("Total failed with exception total: " + FAILED_NONE_OK.get());
        System.out.println("====================================================");
        Metrics analyzer = new Metrics(METRICS_LIST);
        analyzer.computeAndPrintStats("POST");
        // analyzer.computeAndPrintStats("GET");
        // analyzer.saveMetricsToFile("metrics.csv");
    }

//    private static void sendRequests(CloseableHttpClient httpClient, String baseUri, int pairs) {
//        String albumID = sendPostRequest(httpClient, baseUri);
//        for (int i = 0; i < pairs; i++) {
//            sendGetRequest(httpClient, baseUri + "/" + albumID);
//        }
//    }

    private static void sendPairRequest(CloseableHttpClient httpClient, String baseUri, String reviewAddr, int pairs) {
        for (int i = 0; i < pairs; i++) {
            String albumID = sendPostRequest(httpClient, baseUri);
//            sendPostReviewRequest(httpClient, reviewAddr, albumID, true);
//            sendPostReviewRequest(httpClient, reviewAddr, albumID, true);
//            sendPostReviewRequest(httpClient, reviewAddr, albumID, false);
        }
    }

    private static String sendPostReviewRequest(CloseableHttpClient httpClient, String targetUrl, String albumID, boolean like) {
        try {
            long start = System.currentTimeMillis();
            // Construct the URL with path parameters
            String finalUrl = targetUrl + "/" + (like ? "like" : "dislike") + "/" + albumID;
            HttpPost httpPost = new HttpPost(finalUrl);

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int responseCode = response.getStatusLine().getStatusCode();

                if (responseCode == HttpURLConnection.HTTP_OK) {
                    SUCCESSFUL_REQUESTS.incrementAndGet();
                    String responseString = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

                    long end = System.currentTimeMillis();
                    METRICS_LIST.add(new RequestMetrics(start, "POST", end - start, responseCode));

                    // Log or return the response
                    debugPrintln(Thread.currentThread().getName() + " - POST request to " + finalUrl + " succeeded!");
                    return responseString;
                } else {
                    FAILED_NONE_OK.incrementAndGet();
                    System.out.println(Thread.currentThread().getName() + " - POST request to " + finalUrl + " failed! Response code: " + responseCode);
                    return "";
                }
            }
        } catch (IOException e) {
            FAILED_WITH_EXCEPTION.incrementAndGet();
            System.out.println(Thread.currentThread().getName() + " - POST request to " + targetUrl + " Error: " + e.getMessage());
            return "";
        }
    }


    private static String sendPostRequest(CloseableHttpClient httpClient, String targetUrl) {
        String jsonProfile = "{\"artist\": \"ximing1\", \"title\": \"dora\",\"year\": \"2014\"}";

        String boundary = Long.toHexString(System.currentTimeMillis()); // Just generate some unique random value.
        String CRLF = "\r\n"; // Line separator required by multipart/form-data.

        try {
            long start = System.currentTimeMillis();

            HttpPost httpPost = new HttpPost(targetUrl);

            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.addBinaryBody("image", imageBytes, ContentType.create("image/jpeg"), "image.jpg");
            builder.addTextBody("profile", jsonProfile, ContentType.APPLICATION_JSON);

            HttpEntity multipart = builder.build();
            httpPost.setEntity(multipart);

            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                int responseCode = response.getStatusLine().getStatusCode();

                if (responseCode == HttpURLConnection.HTTP_OK) {
                    SUCCESSFUL_REQUESTS.incrementAndGet();
                    String responseString = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);

                    AlbumResponse albumResponse = objectMapper.readValue(responseString, AlbumResponse.class);

                    long end = System.currentTimeMillis();
                    METRICS_LIST.add(new RequestMetrics(start, "POST", end - start, responseCode));

                    debugPrintln(Thread.currentThread().getName() + " - POST request to " + targetUrl + " succeeded! AlbumId: " + albumResponse.getAlbumId());
                    return albumResponse.getAlbumId();
                } else {
                    FAILED_NONE_OK.incrementAndGet();
                    System.out.println(Thread.currentThread().getName() + " - POST request to " + targetUrl + " failed! Response code: " + responseCode);
                    return "";
                }
            }
        } catch (Exception e) {
            FAILED_WITH_EXCEPTION.incrementAndGet();
            System.out.println(Thread.currentThread().getName() + " - POST request to " + targetUrl + " Error: " + e.getMessage());
            return "";
        }
    }
}

//    private static void sendGetRequest(String targetUrl) {
//        try {
//            long start = System.currentTimeMillis();
//            URL url = new URL(targetUrl);
//            HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
//            httpURLConnection.setRequestMethod("GET");
//            httpURLConnection.setRequestProperty("User-Agent", "Mozilla/5.0");
//
//            int responseCode = httpURLConnection.getResponseCode();
//            if (responseCode == HttpURLConnection.HTTP_OK) {
//                SUCCESSFUL_REQUESTS.incrementAndGet();
//                BufferedReader in = new BufferedReader(new InputStreamReader(httpURLConnection.getInputStream()));
//                StringBuffer response = new StringBuffer();
//                String inputLine;
//                while ((inputLine = in.readLine()) != null) {
//                    response.append(inputLine);
//                }
//                in.close();
////                debugPrintln(Thread.currentThread().getName() + " - GET request to " + targetUrl + " succeeded! Response: " + response.toString());
////                long end = System.currentTimeMillis();
////                METRICS_LIST.add(new RequestMetrics(start, "GET", end - start, responseCode));
//            } else {
//                FAILED_NONE_OK.incrementAndGet();
//                System.out.println(Thread.currentThread().getName() + " - GET request to " + targetUrl + " failed! Response code: " + responseCode);
//            }
//        } catch (Exception e) {
//            FAILED_WITH_EXCEPTION.incrementAndGet();
//            System.out.println(Thread.currentThread().getName() + " - GET request to " + targetUrl + " Error: " + e.getMessage());
//        }
//    }
//}