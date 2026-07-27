package com.maxcapital.executionreports.infrastructure.web;

import com.maxcapital.executionreports.application.SeedStreamUseCase;
import com.maxcapital.executionreports.application.dto.SeedResultDto;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(TestSeedController.class)
class TestSeedControllerUnitTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SeedStreamUseCase seedStreamUseCase;

    @Test
    @DisplayName("POST /test/seed invoca el caso de uso y retorna status 200 OK con el resumen")
    void seed_returns200OK_withSeedResult() throws Exception {
        SeedResultDto dto = new SeedResultDto(5, 12, 1, List.of(90001L, 90002L, 90003L, 90004L, 90005L));
        when(seedStreamUseCase.seed(5, true)).thenReturn(dto);

        mockMvc.perform(post("/test/seed")
                        .param("ordersCount", "5")
                        .param("injectDuplicates", "true")
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.seededOrdersCount").value(5))
                .andExpect(jsonPath("$.totalEventsPublished").value(12))
                .andExpect(jsonPath("$.injectedDuplicatesCount").value(1))
                .andExpect(jsonPath("$.generatedOrderIds").isArray());
    }
}
