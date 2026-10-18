package com.example.notificationservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserProfile {
    private String userId;
    private String email;
    private String phoneNumber;

    /**
     * The fallback profile is "empty": it carries the userId but no contact details.
     * (Not a bean getter on purpose, so it is never serialised to JSON.)
     */
    public boolean hasContactDetails() {
        return (email != null && !email.isBlank()) || (phoneNumber != null && !phoneNumber.isBlank());
    }
}
