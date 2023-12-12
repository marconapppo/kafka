package Main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmailService
{
    public static void main (String[] args)
    {
        var emailService = new EmailService();
        var service = new KafkaService(EmailService.class.getSimpleName() ,"ECOMMERCE_SEND_EMAIL", emailService::parse);
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
