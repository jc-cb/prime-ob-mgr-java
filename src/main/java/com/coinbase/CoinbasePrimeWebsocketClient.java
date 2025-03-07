// Copyright 2025-present Coinbase Global, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.coinbase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.net.http.WebSocket.Listener;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class CoinbasePrimeWebsocketClient implements Listener {

    private static final String URI_STRING = "wss://ws-feed.prime.coinbase.com";

    private static final String ACCESS_KEY    = System.getenv("ACCESS_KEY");
    private static final String SECRET_KEY    = System.getenv("SIGNING_KEY");
    private static final String PASSPHRASE    = System.getenv("PASSPHRASE");
    private static final String SVC_ACCOUNTID = System.getenv("SVC_ACCOUNTID");

    private static final String CHANNEL    = "l2_data";
    private static final String PRODUCT_ID = "ETH-USD";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private WebSocket webSocket;
    private OrderBookProcessor processor;  

    private StringBuilder messageBuffer = new StringBuilder();

    public void start() {
        while (true) {
            try {
                connectAndListen();
            } catch (Exception e) {
                System.err.println("WebSocket connection lost. Reconnecting... " + e.getMessage());
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void connectAndListen() throws InterruptedException, ExecutionException {
        HttpClient client = HttpClient.newHttpClient();

        CompletableFuture<WebSocket> wsFuture = client.newWebSocketBuilder()
                .buildAsync(URI.create(URI_STRING), this);

        this.webSocket = wsFuture.get(); 

        String authMessage = AuthUtils.createAuthMessage(
            CHANNEL, PRODUCT_ID, PASSPHRASE, ACCESS_KEY, SECRET_KEY, SVC_ACCOUNTID
        );
        webSocket.sendText(authMessage, true);

        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(10000L);
        }
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        try {
            messageBuffer.append(data);

            if (last) {
                String fullMessage = messageBuffer.toString();
                messageBuffer.setLength(0); 
                
                JsonNode node = MAPPER.readTree(fullMessage);

                if ("l2_data".equals(node.path("channel").asText()) && node.has("events")) {
                    JsonNode eventsNode = node.get("events");
                    
                    if (eventsNode.size() > 0) {
                        String eventType = eventsNode.get(0).path("type").asText();
                        
                        if ("snapshot".equals(eventType)) {
                            this.processor = new OrderBookProcessor(fullMessage);
                            System.out.println("Snapshot received.");
                        } else {
                            if (this.processor != null) {
                                this.processor.applyUpdate(fullMessage);
                            }
                        }
                    }
                }
            }

        } catch (JsonProcessingException e) {
            System.err.println("Failed to parse JSON from WebSocket: " + e.getMessage());
        }

        webSocket.request(1);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        System.err.println("WebSocket closed: " + statusCode + " / " + reason);
        return Listener.super.onClose(webSocket, statusCode, reason);
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        System.err.println("WebSocket error: " + error.getMessage());
        Listener.super.onError(webSocket, error);
    }

    public OrderBookProcessor getProcessor() {
        return processor;
    }

    public void onMessage(String message) {
        messageBuffer.append(message);
    }
}
