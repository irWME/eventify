package io.github.alikelleci.eventify;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.commandhandling.gateway.CommandGateway;
import io.github.alikelleci.eventify.messaging.commandhandling.gateway.DefaultCommandGateway;
import io.github.alikelleci.eventify.messaging.eventhandling.gateway.DefaultEventGateway;
import io.github.alikelleci.eventify.messaging.eventhandling.gateway.EventGateway;
import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class GatewayBuilder {

  private final String applicationId;
  private final String bootstrapServers;
  private final String securityProtocol;
  private final Properties producerConfig;

  public GatewayBuilder(String applicationId, String bootstrapServers, String securityProtocol) {
    this.applicationId = applicationId;
    this.bootstrapServers = bootstrapServers;
    this.securityProtocol = securityProtocol;

    this.producerConfig = new Properties();
    this.producerConfig.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    this.producerConfig.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
    this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
  }

  public GatewayBuilder(String applicationId, String bootstrapServers, String securityProtocol, Properties producerConfig) {
    this(applicationId, bootstrapServers, securityProtocol);
    producerConfig.forEach(this.producerConfig::putIfAbsent);
  }

  public CommandGateway commandGateway() {
    return new DefaultCommandGateway(producer(), consumer(), applicationId);
  }

  public EventGateway eventGateway() {
    return new DefaultEventGateway(producer());
  }

  private Producer<String, Message> producer() {
    return new KafkaProducer<>(this.producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
  }

  private Consumer<String, Message> consumer() {
    return new KafkaConsumer<>(consumerConfig(),
        new StringDeserializer(),
        new JsonDeserializer<>(Message.class));
  }

  private Properties consumerConfig() {
    String groupId = applicationId.concat("-command-gateway-").concat(UlidCreator.getMonotonicUlid().toString());

    Properties properties = new Properties();
    properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
    properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    properties.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    properties.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    return properties;
  }
}
