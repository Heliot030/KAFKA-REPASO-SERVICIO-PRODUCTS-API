package com.efrain.course.springcloud.kafka.api.controllers;

import com.efrain.course.springcloud.kafka.api.models.dto.ProductDto;
import com.efrain.course.springcloud.kafka.api.models.dto.Reply;
import com.efrain.course.springcloud.kafka.api.service.ProductCommandService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.util.Map;


@RestController
@RequestMapping("/products")
public class ProductController {

    private final ProductCommandService commandService;

    public ProductController(ProductCommandService commandService) {
        this.commandService = commandService;
    }


    @PostMapping()
    public ResponseEntity<?> create(@Valid @RequestBody ProductDto dto){
       Reply<?> reply= commandService.sendCreateAndAwait(dto, Duration.ofSeconds(5));
       if ("SUCCESS".equalsIgnoreCase(reply.status())){
           return ResponseEntity.ok(reply.body());
       }
        return ResponseEntity.badRequest().body(Map.of("error",reply.body()));
    }
}
