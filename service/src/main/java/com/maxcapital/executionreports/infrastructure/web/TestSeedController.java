package com.maxcapital.executionreports.infrastructure.web;

import com.maxcapital.executionreports.application.SeedStreamUseCase;
import com.maxcapital.executionreports.application.dto.SeedResultDto;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/test")
@RequiredArgsConstructor
public class TestSeedController {

    private final SeedStreamUseCase seedStreamUseCase;

    @PostMapping("/seed")
    public ResponseEntity<SeedResultDto> seed(
            @RequestParam(name = "ordersCount", defaultValue = "5") int ordersCount,
            @RequestParam(name = "injectDuplicates", defaultValue = "true") boolean injectDuplicates
    ) {
        SeedResultDto result = seedStreamUseCase.seed(ordersCount, injectDuplicates);
        return ResponseEntity.ok(result);
    }
}
