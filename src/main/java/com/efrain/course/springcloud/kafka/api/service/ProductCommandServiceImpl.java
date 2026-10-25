package com.efrain.course.springcloud.kafka.api.service;

import com.efrain.course.springcloud.kafka.api.messaging.ReplyInbox;
import com.efrain.course.springcloud.kafka.api.models.Command;
import com.efrain.course.springcloud.kafka.api.models.dto.ProductDto;
import com.efrain.course.springcloud.kafka.api.models.dto.Reply;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException; // <-- Importación corregida

@Service
public class ProductCommandServiceImpl implements ProductCommandService {

    private final StreamBridge streamBridge;
    private final ReplyInbox replyInbox;

    public ProductCommandServiceImpl(StreamBridge streamBridge, ReplyInbox replyInbox) {
        this.streamBridge = streamBridge;
        this.replyInbox = replyInbox;
    }

    @Override
    public Reply<?> sendCreateAndAwait(ProductDto dto, Duration timeout) { // <-- Se eliminó el ';'

        Command<ProductDto> cmd = new Command<>("CREATE", null, dto);
        String correlationId = UUID.randomUUID().toString();

        CompletableFuture<Reply<?>> future = replyInbox.register(correlationId);

        Message<Command<ProductDto>> message = MessageBuilder.withPayload(cmd)
                .setHeader("CORRELACION-ID", correlationId)
                .build();

        boolean send = this.streamBridge.send("commands-out-0", message);
        if (!send) {
            throw new IllegalStateException("No se pudo enviar a KAFKA");
        }

        try {
            // <-- Se cambió MICROSECONDS por MILLISECONDS
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            //Thread.currentThread().interrupt(); // <-- Buena práctica de concurrencia
            throw new RuntimeException("esperando respuesta de Kafka", e);
        }
    }
}