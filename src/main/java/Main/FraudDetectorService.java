package Main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectorService
{
    public static void main (String[] args)
    {
        var fraudDetectorService = new FraudDetectorService();
        var service = new KafkaService(FraudDetectorService.class.getSimpleName() ,"ECOMMERCE_NEW_ORDER", fraudDetectorService::parse);
        service.run();
    }

    private void parse(ConsumerRecord<String, String> record)
    {
        System.out.println("-------------------------------------------");
        System.out.println("Processando send email, analisando fraudes");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("partition: " + record.partition());
        System.out.println("offset: " + record.offset());
        try
        {
            Thread.sleep(5000);
        } catch (InterruptedException e)
        {
            e.printStackTrace();
            //throw new RuntimeException(e);
        }
        System.out.println("Order Processada");
    }
}
