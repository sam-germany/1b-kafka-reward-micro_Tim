package com.course.kafka.broker.consumer;

import com.course.kafka.broker.message.OrderMessage;
import com.course.kafka.broker.message.OrderReplyMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

@Service
public class OrderListenerTwo {
    private static final Logger LOG = LoggerFactory.getLogger(OrderListenerTwo.class);

    @KafkaListener(topics = "t.commodity.order")
    @SendTo("t.commodity.order-reply")
    public OrderReplyMessage listen(ConsumerRecord<String, OrderMessage> consumerRecord) {
          var headers22 = consumerRecord.headers();
          var orderMessage = consumerRecord.value();

        System.out.println(new String(headers22.lastHeader("surpriseBonus").value()+" value of the header we sended"));


LOG.info("Processing order {} ,item {}, credit card {}", orderMessage.getOrderNumber(),
                            orderMessage.getItemName(), orderMessage.getCreditCardNumber());

LOG.info("Headers are: ");
     headers22.forEach(x -> LOG.info(" key : {}, value: {}", x.key(), new String(x.value())) ); // here we are fetching
                                                                               // the key "surpriseBonus" and its value

var bonusPercentage22 = Double.parseDouble(new String(headers22.lastHeader("surpriseBonus").value()));
var bonusAmount22 = (bonusPercentage22 /100) * orderMessage.getPrice() * orderMessage.getQuantity();

LOG.info("Surprise bonus is {}" , bonusAmount22);


     var replyMessage22 = new OrderReplyMessage();
     replyMessage22.setReplyMessage(
               "Order " + orderMessage.getOrderNumber() + " item " + orderMessage.getItemName() + " processed " );

     return replyMessage22;
    }
}
/*
Note: in the OrderProducer class we have a method buildProducerRecord22()  their we are adding a extra message
as List<Header>   and inside it we put RecordHeader()   as  key-value pair so for reading that we need to
 use a forEach() method as we are using at line 26 to fetch all the List<> data one by one out ay key-value
 pairs

 */
