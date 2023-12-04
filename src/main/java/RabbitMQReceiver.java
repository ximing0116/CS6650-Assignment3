import com.rabbitmq.client.*;
import com.zaxxer.hikari.*;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class RabbitMQReceiver {

    private static final String QUEUE_NAME = "review";
    private static final String RABBITMQ_HOST = "b-46e2e8f5-31a4-4539-8ff2-8f0f2d5df381.mq.us-east-2.amazonaws.com"; // Set your RabbitMQ Host
    private static final int RABBITMQ_PORT = 5671;
    private static final String RABBITMQ_USERNAME = "root"; // RabbitMQ username
    private static final String RABBITMQ_PASSWORD = "qwer12345678"; // RabbitMQ password

    private static final String JDBC_URL = "jdbc:postgresql://database-1.ctk0zbunxh4u.us-east-2.rds.amazonaws.com:5432/postgres";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "qwer12345678";
    private static final int CHANNEL_POOL_SIZE = 10;

    public static void main(String[] args) throws Exception {
        // Set up the connection factory
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setPort(RABBITMQ_PORT);
        factory.setUsername(RABBITMQ_USERNAME);
        factory.setPassword(RABBITMQ_PASSWORD);
        factory.useSslProtocol();

        // Set up HikariCP DataSource
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(JDBC_URL);
        config.setUsername(DB_USERNAME);
        config.setPassword(DB_PASSWORD);
        config.setMaximumPoolSize(50);
        HikariDataSource dataSource = new HikariDataSource(config);

        Runnable consumerTask = () -> {
            try {
                final com.rabbitmq.client.Connection connection = factory.newConnection();
                Channel channel = connection.createChannel();
                channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                channel.basicQos(1);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    String message = new String(delivery.getBody(), "UTF-8");
                    processMessage(dataSource, message);
                };

                channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        // Start threads
        for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
            new Thread(consumerTask).start();
        }
    }

    private static void processMessage(HikariDataSource dataSource, String message) {
        System.out.println("processing message:" + message);
        String[] parts = message.split(" ");
        boolean like = "Like".equalsIgnoreCase(parts[0]);
        int albumID = Integer.parseInt(parts[parts.length - 1]); // Assuming the last part is the album ID

        String likeQuery = "INSERT INTO album_likes (albumID, likes, dislikes) VALUES (?, ?, ?) " +
                "ON CONFLICT (albumID) DO UPDATE SET likes = album_likes.likes + ?, dislikes = album_likes.dislikes + ?";

        try (Connection dbConnection = dataSource.getConnection();
             PreparedStatement statement = dbConnection.prepareStatement(likeQuery)) {

            statement.setInt(1, albumID);
            statement.setInt(2, like ? 1 : 0);
            statement.setInt(3, like ? 0 : 1);
            statement.setInt(4, like ? 1 : 0);
            statement.setInt(5, like ? 0 : 1);

            statement.executeUpdate();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        System.out.println("successfully processed message:" + message);
    }
}
