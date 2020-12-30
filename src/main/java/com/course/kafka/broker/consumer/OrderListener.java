package com.course.kafka.broker.consumer;

import com.course.kafka.broker.message.OrderMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderListener {
    private static final Logger LOG = LoggerFactory.getLogger(OrderListener.class);

    @KafkaListener(topics = "t.commodity.order")
    public void listen(ConsumerRecord<String, OrderMessage> consumerRecord) {
          var headers22 = consumerRecord.headers();
          var orderMessage = consumerRecord.value();

LOG.info("Processing order {} ,item {}, credit card {}", orderMessage.getOrderNumber(),
                            orderMessage.getItemName(), orderMessage.getCreditCardNumber());

LOG.info("Headers are: ");
     headers22.forEach(x -> LOG.info(" key : {}, value: {}", x.key(), new String(x.value())) );

var bonusPercentage22 = Double.parseDouble(new String(headers22.lastHeader("surpriseBonus").value()));
var bonusAmount22 = (bonusPercentage22 /100) * orderMessage.getPrice() * orderMessage.getQuantity();

LOG.info("Surprise bonus is {}" , bonusAmount22);

    }

}
