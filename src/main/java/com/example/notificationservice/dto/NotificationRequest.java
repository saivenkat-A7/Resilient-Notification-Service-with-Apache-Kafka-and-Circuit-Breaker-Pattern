package com.example.notificationservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NotificationRequest {

    @NotBlank(message = "userId must not be blank")
    private String userId; // Unique identifier for the user

    @NotBlank(message = "message must not be blank")
    private String message;

    @NotBlank(message = "notificationType must not be blank")
    @Pattern(regexp = "EMAIL|SMS|PUSH", message = "notificationType must be one of EMAIL, SMS, PUSH")
    private String notificationType; // "EMAIL", "SMS", "PUSH"
}
