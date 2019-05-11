import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

public class KafkaSSL {
    public static void main(String[] args) {

        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation().as(DataflowPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        List<KV<Long, String>> kvs = new ArrayList<>();
        kvs.add(KV.of(1L, "test message"));

        pipeline.apply(Create.of(kvs))
                .apply(KafkaIO.<Long, String>write()
                        .withBootstrapServers("broker0.kafka.ryancm.net:31090,broker1.kafka.ryancm.net:31090,broker2.kafka.ryancm.net:31090")
                        .withTopic("secured-topic")
                        .withKeySerializer(LongSerializer.class)
                        .withValueSerializer(StringSerializer.class)
                        .withProducerFactoryFn(props -> {

                            try {
                                Files.copy(KafkaSSL.class.getClassLoader()
                                        .getResourceAsStream("kafka.client1.keystore.jks"),
                                        Paths.get("/tmp/kafka.client1.keystore.jks"),
                                        StandardCopyOption.REPLACE_EXISTING);
                                Files.copy(KafkaSSL.class.getClassLoader()
                                        .getResourceAsStream("kafka.client1.truststore.jks"),
                                        Paths.get("/tmp/kafka.client1.truststore.jks"),
                                        StandardCopyOption.REPLACE_EXISTING);
                            } catch(IOException e) {
                                throw new UncheckedIOException(e);
                            }

                            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
                            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/tmp/kafka.client1.truststore.jks");
                            props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/tmp/kafka.client1.keystore.jks");
                            props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,  "password");
                            props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,  "password");
                            props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG,  "password");
                            return new KafkaProducer<>(props);
                        })
                );

        pipeline.run();
    }
}
