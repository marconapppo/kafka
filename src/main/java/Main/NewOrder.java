package Main;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrder
{
    public static void main(String[] args) throws ExecutionException, InterruptedException
    {
        var producer = new KafkaProducer<String, String>(properties());

        var key = UUID.randomUUID().toString();
        var value = key + "123,234,345";
        var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", key, value);

        var callback = getCallback();

        var email = "Welcome! Processing email...... ";
        var emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", key, email);

        producer.send(record, callback).get();
        producer.send(emailRecord, callback).get();
    }

    private static Callback getCallback()
    {
        return (data, ex) ->
        {
            if (ex != null)
            {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso " + data.topic() + " ::: partition " + data.partition() +
                    "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
    }

    private static Properties properties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
