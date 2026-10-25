package com.efrain.course.springcloud.kafka.api.models.dto;

public record Reply<T>(String status, String message, T body) {
}
