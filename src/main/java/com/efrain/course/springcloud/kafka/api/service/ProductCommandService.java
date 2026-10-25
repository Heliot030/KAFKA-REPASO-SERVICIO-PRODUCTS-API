package com.efrain.course.springcloud.kafka.api.service;

import com.efrain.course.springcloud.kafka.api.models.dto.ProductDto;
import com.efrain.course.springcloud.kafka.api.models.dto.Reply;

import java.time.Duration;

public interface ProductCommandService {

    Reply<?> sendCreateAndAwait(ProductDto dto, Duration timeout);
}

