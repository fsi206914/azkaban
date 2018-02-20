package azkaban.logging;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A log4j appender that produces log messages to Kafka
 */
public class KafkaAppender extends AppenderSkeleton {

  private String brokerList;
  private String topic;
  private String compressionType;
  private String sslKafkaPropFilePath;

  private int retries;
  private int requiredNumAcks = Integer.MAX_VALUE;
  private boolean syncSend;
  private Producer<byte[], byte[]> producer;

  public Producer<byte[], byte[]> getProducer() {
    return this.producer;
  }

  public String getBrokerList() {
    return this.brokerList;
  }

  public void setBrokerList(final String brokerList) {
    this.brokerList = brokerList;
  }

  public int getRequiredNumAcks() {
    return this.requiredNumAcks;
  }

  public void setRequiredNumAcks(final int requiredNumAcks) {
    this.requiredNumAcks = requiredNumAcks;
  }

  public int getRetries() {
    return this.retries;
  }

  public void setRetries(final int retries) {
    this.retries = retries;
  }

  public String getCompressionType() {
    return this.compressionType;
  }

  public void setCompressionType(final String compressionType) {
    this.compressionType = compressionType;
  }

  public String getSslKafkaPropFilePath() {
    System.out.println("get Kafka producer ssl configs: ");
    return this.sslKafkaPropFilePath;
  }

  public void setSslKafkaPropFilePath(final String sslKafkaPropFilePath) {
    System.out.println("set ssl properties");
    this.sslKafkaPropFilePath = sslKafkaPropFilePath;
  }

  public String getTopic() {
    return this.topic;
  }

  public void setTopic(final String topic) {
    this.topic = topic;
  }

  public boolean getSyncSend() {
    return this.syncSend;
  }

  public void setSyncSend(final boolean syncSend) {
    this.syncSend = syncSend;
  }

  @Override
  public void activateOptions() {
    // check for config parameter validity
    final Properties props = new Properties();
    if (this.brokerList != null) {
      props.put(BOOTSTRAP_SERVERS_CONFIG, this.brokerList);
    }
    if (props.isEmpty()) {
      throw new ConfigException("The bootstrap servers property should be specified");
    }
    if (this.topic == null) {
      throw new ConfigException("Topic must be specified by the Kafka log4j appender");
    }
    if (this.compressionType != null) {
      props.put(COMPRESSION_TYPE_CONFIG, this.compressionType);
    }
    if (this.requiredNumAcks != Integer.MAX_VALUE) {
      props.put(ACKS_CONFIG, Integer.toString(this.requiredNumAcks));
    }
    if (this.retries > 0) {
      props.put(RETRIES_CONFIG, this.retries);
    }

    Properties sslProperties = null;
    System.out.println("Fetching Kafka producer ssl configs: ");
    try {
      if (this.sslKafkaPropFilePath != null && this.sslKafkaPropFilePath.length() > 0) {
        sslProperties = getPropertiesFromFile(this.sslKafkaPropFilePath);
        System.out.println("Kafka producer ssl configs: " + sslProperties);
      }
    } catch (final IOException ex) {

    }

    props.put(KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    Properties merged = new Properties();
    merged.putAll(props);
    merged.putAll(sslProperties);

    this.producer = getKafkaProducer(merged);
    LogLog.debug("Kafka producer connected to " + this.brokerList);
    LogLog.debug("Logging for topic: " + this.topic);
  }

  public Properties getPropertiesFromFile(final String path) throws IOException {
    final Properties properties = new Properties();
    InputStream input = null;
    try {
      input = new FileInputStream(path);
      properties.load(input);
    } catch (final IOException e) {
      System.out.println(
          "KafkaLog4JAppender: IOException reading properties: path=" + path + " : message=" + e
              .getMessage());
      e.printStackTrace();
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (final IOException e) {
          e.printStackTrace();
        }
      }
    }
    return properties;
  }

  protected Producer<byte[], byte[]> getKafkaProducer(final Properties props) {
    return new KafkaProducer<>(props);
  }

  @Override
  protected void append(final LoggingEvent event) {
    final String message = subAppend(event);
    LogLog.debug("[" + new Date(event.getTimeStamp()) + "]" + message);
    final Future<RecordMetadata> response = this.producer.send(
        new ProducerRecord<>(this.topic, message.getBytes(StandardCharsets.UTF_8)));
    if (this.syncSend) {
      try {
        response.get();
      } catch (InterruptedException | ExecutionException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private String subAppend(final LoggingEvent event) {
    return (this.layout == null) ? event.getRenderedMessage() : this.layout.format(event);
  }

  @Override
  public void close() {
    if (!this.closed) {
      this.closed = true;
      this.producer.close();
    }
  }

  @Override
  public boolean requiresLayout() {
    return true;
  }
}
