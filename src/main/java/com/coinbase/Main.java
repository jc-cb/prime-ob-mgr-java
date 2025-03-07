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

import java.math.BigDecimal;
import java.util.List;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        CoinbasePrimeWebsocketClient wsClient = new CoinbasePrimeWebsocketClient();

        new Thread(wsClient::start).start();

        while (true) {
            OrderBookProcessor processor = wsClient.getProcessor();

            if (processor == null) {
                System.out.println("\nNo snapshot has been received yet... waiting.");
            } else {
                BigDecimal mid = processor.getMidPrice();
                System.out.println("\n----- Current Book -----");
                System.out.println("Mid Price: " + (mid != null ? mid : "N/A"));

                List<OrderBookProcessor.Level> topBids = processor.getTopBids(10);
                System.out.println("Top Bids:");
                for (OrderBookProcessor.Level lvl : topBids) {
                    System.out.println("  " + lvl);
                }

                List<OrderBookProcessor.Level> topAsks = processor.getTopAsks(10);
                System.out.println("Top Asks:");
                for (OrderBookProcessor.Level lvl : topAsks) {
                    System.out.println("  " + lvl);
                }
            }

            Thread.sleep(3000);
        }
    }
}
