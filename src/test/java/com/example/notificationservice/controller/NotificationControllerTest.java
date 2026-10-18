package com.example.notificationservice.controller;

import com.example.notificationservice.dto.NotificationRequest;
import com.example.notificationservice.exception.NotificationPublishException;
import com.example.notificationservice.service.NotificationProducerService;
import com.example.notificationservice.store.NotificationStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = {NotificationController.class, NotificationQueryController.class})
class NotificationControllerTest {

    @Autowired
    private MockMvc mvc;

    @MockBean
    private NotificationProducerService producerService;

    @MockBean
    private NotificationStore store;

    @Test
    void validRequestReturns202AndDelegatesToProducer() throws Exception {
        mvc.perform(post("/api/notifications/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"u1\",\"message\":\"Hi\",\"notificationType\":\"EMAIL\"}"))
                .andExpect(status().isAccepted());

        verify(producerService).publishNotificationEvent(any(NotificationRequest.class));
    }

    @Test
    void blankFieldsReturn400WithDetails() throws Exception {
        mvc.perform(post("/api/notifications/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"\",\"message\":\"\",\"notificationType\":\"EMAIL\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.details.userId").exists())
                .andExpect(jsonPath("$.details.message").exists());

        verify(producerService, never()).publishNotificationEvent(any());
    }

    @Test
    void unknownNotificationTypeReturns400() throws Exception {
        mvc.perform(post("/api/notifications/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"u1\",\"message\":\"Hi\",\"notificationType\":\"FAX\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.details.notificationType").exists());
    }

    @Test
    void malformedJsonReturns400() throws Exception {
        mvc.perform(post("/api/notifications/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{not json"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void brokerUnavailableReturns503() throws Exception {
        doThrow(new NotificationPublishException("broker down", new RuntimeException()))
                .when(producerService).publishNotificationEvent(any());

        mvc.perform(post("/api/notifications/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"userId\":\"u1\",\"message\":\"Hi\",\"notificationType\":\"SMS\"}"))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void recordsEndpointReturnsStoredRecords() throws Exception {
        when(store.findAll()).thenReturn(List.of());

        mvc.perform(get("/api/notifications/records"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray());
    }
}
