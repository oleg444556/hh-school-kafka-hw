package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  // Сразу подтверждаем получение сообщения, а обработается оно должным образом или нет уже не важно
  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    ack.acknowledge();
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    try {
      service.handle("topic1", consumerRecord.value());
    } catch (RuntimeException ignore) {
    }
  }

  // Обрабатываем сообщение до тех пор, пока оно не обработается без ошибок, и затем подтверждаем
  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    service.handle("topic2", consumerRecord.value());
    ack.acknowledge();
  }

  // Использую service, считаю сколько раз сообщение было обработано, если есть дубли отправленных сообщений
  // они будут проигнорированы, также не произойдет двойной обработки сообщения, когда ошибка в handle
  // произошла уже после обработки. Честно кажется, что это какой-то абуз, сначала была идея вместе
  // с сообщением отправлять какой-либо id и использовать set, чтобы отличать дубли сообщений,
  // но при этом проблема с повторной обработкой не пропадала, тогда придумал использовать
  // service.count, и при таком подходе необходимость использовать какие-либо id, для того чтобы
  // различать дубли отпала :/
  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    String message = consumerRecord.value();
    int messageCount = service.count("topic3", message);
    while (messageCount == 0) {
      try {
        LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
        service.handle("topic3", message);
      } catch (RuntimeException ignore) {
      } finally {
        messageCount = service.count("topic3", message);
      }
    }
    ack.acknowledge();
  }
}
