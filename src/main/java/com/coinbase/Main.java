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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import com.coinbase.prime.orders.OrdersService;
import com.coinbase.prime.model.orders.CreateOrderRequest;
import com.coinbase.prime.model.orders.CreateOrderResponse;
import com.coinbase.prime.client.CoinbasePrimeClient;
import com.coinbase.prime.factory.PrimeServiceFactory;
import com.coinbase.prime.credentials.CoinbasePrimeCredentials;
import com.coinbase.prime.model.enums.OrderSide;
import com.coinbase.prime.model.enums.OrderType;

/**
 * Main entry point for capturing order book data into a CSV whenever we place an order.
 */
public class Main {

    private static final String CSV_FILE_PATH = "order_book_capture.csv";

    public static void main(String[] args) throws InterruptedException, IOException {

        // Load creds from environment
        String credsStringBlob = System.getenv("COINBASE_PRIME_CREDENTIALS");

        // Start the WebSocket in a background thread to receive L2 updates
        CoinbasePrimeWebsocketClient wsClient = new CoinbasePrimeWebsocketClient();
        new Thread(wsClient::start).start();

        // Create REST client
        CoinbasePrimeCredentials credentials = new CoinbasePrimeCredentials(credsStringBlob);
        CoinbasePrimeClient client = new CoinbasePrimeClient(credentials);
        OrdersService ordersService = PrimeServiceFactory.createOrdersService(client);

        // Ensure CSV file has a header row
        OrderBookCsvExporter.ensureCsvHasHeaderIfNeeded(CSV_FILE_PATH);

        // Main loop
        while (true) {
            // Access the real-time order book
            OrderBookProcessor processor = wsClient.getProcessor();

            if (processor == null) {
                System.out.println("\nNo snapshot has been received yet... waiting.");
            } else {
                // Print current mid price for debugging
                BigDecimal mid = processor.getMidPrice();
                System.out.println("\n----- Current Book -----");
                System.out.println("Mid Price: " + (mid != null ? mid : "N/A"));

                // Top 10 Bids & Asks (copy for concurrency safety)
                List<OrderBookProcessor.Level> topBids = processor.getTopBids(10);
                List<OrderBookProcessor.Level> topAsks = processor.getTopAsks(10);

                System.out.println("Top Bids:");
                for (OrderBookProcessor.Level lvl : topBids) {
                    System.out.println("  " + lvl);
                }

                System.out.println("Top Asks:");
                for (OrderBookProcessor.Level lvl : topAsks) {
                    System.out.println("  " + lvl);
                }

                // Place an order via REST
                CreateOrderResponse orderResponse = ordersService.createOrder(
                    new CreateOrderRequest.Builder()
                            .portfolioId("314dbd76-4459-41cd-ba9a-dccdd86b44e2")
                            .productId("ETH-USD")
                            .side(OrderSide.BUY)
                            .type(OrderType.LIMIT)
                            .baseQuantity("0.001")
                            .limitPrice("1000.0")
                            .clientOrderId(UUID.randomUUID().toString())
                            .build()
                );

                String orderId = orderResponse.getOrderId();
                System.out.println("Order ID: " + orderId);

                // Immediately capture the top-of-book data again (for concurrency safety)
                BigDecimal midSnapshot = processor.getMidPrice();
                List<OrderBookProcessor.Level> safeTopBids = processor.getTopBids(10);
                List<OrderBookProcessor.Level> safeTopAsks = processor.getTopAsks(10);

                System.out.println("\nCaptured Book for Order " + orderId);
                System.out.println("Mid Price: " + midSnapshot);

                // Print them again for debugging
                System.out.println("Top Bids:");
                for (OrderBookProcessor.Level lvl : safeTopBids) {
                    System.out.println("  " + lvl);
                }
                System.out.println("Top Asks:");
                for (OrderBookProcessor.Level lvl : safeTopAsks) {
                    System.out.println("  " + lvl);
                }

                // Finally, write to CSV
                OrderBookCsvExporter.captureOrderBookToCsv(
                        CSV_FILE_PATH,
                        orderId,
                        safeTopAsks,
                        safeTopBids
                );
            }

            // Sleep 3s before placing the next order in this demo
            Thread.sleep(3000);
        }
    }
}
