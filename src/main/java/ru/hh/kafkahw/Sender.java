package ru.hh.kafkahw;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class Sender {
  private final KafkaProducer producer;
  private final static int MAX_ATTEMPTS = 5;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
  }
 // Создал 3 отдельных метода для семантик, тк LeastOnce семантика ломает логику MostOnce и наоборот.
 // А ExactlyOnce сочетает в себе их

  // Ничего дополнительно не надо писать, отправилось сообщение и хорошо, не отправилось ну и ничего страшного
  public void doSomethingMostOnce(String topic, String message) {
    try {
      producer.send(topic, message);
    } catch (Exception ignore) {
    }
  }

  // Пробуем отправить сообщение 5 раз, может произойти дублирование сообщений
  // если сообщение уже отправилось, но в процессе отправки метода send возникла
  // еще ошибка, и мы снова пытаемся отправить сообщение, семантика позволяет это
  public void doSomethingLeastOnce(String topic, String message) {
    for (int i = 0; i < MAX_ATTEMPTS; i++) {
      try {
        producer.send(topic, message);
        break;
      } catch (Exception ignore) {
      }
    }
  }

  public void doSomethingExactlyOnce(String topic, String message) {
    doSomethingLeastOnce(topic, message);
  }
}
