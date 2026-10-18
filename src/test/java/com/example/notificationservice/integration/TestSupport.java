package com.example.notificationservice.integration;

import java.io.IOException;
import java.net.ServerSocket;

final class TestSupport {

    private TestSupport() {
    }

    static int freePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("No free port available", e);
        }
    }
}
