package Main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class LogService
{
    public static void main (String[] args)
    {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

        while (true)
        {
            var records = consumer.poll(Duration.ofMillis(100));

            if(!records.isEmpty())
            {
                System.out.println("Encontrei " + records.count() + " registros");

                for (var record : records)
                {
                    System.out.println("-------------------------------------------");
                    System.out.println("Log.........");
                    System.out.println("key: " + record.key());
                    System.out.println("topic: " + record.topic());
                    System.out.println("value: " + record.value());
                    System.out.println("partition: " + record.partition());
                    System.out.println("offset: " + record.offset());
                    try
                    {
                        Thread.sleep(50000);
                    } catch (InterruptedException e)
                    {
                        e.printStackTrace();
                        //throw new RuntimeException(e);
                    }
                    System.out.println("Order Processada");
                }
            }

        }
    }

    private static Properties properties()
    {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return properties;
    }
}
