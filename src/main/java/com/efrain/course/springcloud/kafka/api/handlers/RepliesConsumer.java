package com.efrain.course.springcloud.kafka.api.handlers;

import com.efrain.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.efrain.course.springcloud.kafka.api.models.dto.Reply;
import org.springframework.cloud.stream.binding.MessageSourceBindingTargetFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;

@Configuration
public class RepliesConsumer {

    private final ReplyInbox replyInbox;

    public RepliesConsumer(ReplyInbox replyInbox) {
        this.replyInbox = replyInbox;
    }

    @Bean
    public Consumer<Message<Reply<?>>> handleReplies(MessageSourceBindingTargetFactory messageSourceFactory){
        return message->{
        String correlationId = message.getHeaders().get("CORRELACION-ID", String.class);
        System.out.println("Recibiendo respuesta para el ID: " + correlationId);
        replyInbox.complete(correlationId, message.getPayload());
        };
    }
}
