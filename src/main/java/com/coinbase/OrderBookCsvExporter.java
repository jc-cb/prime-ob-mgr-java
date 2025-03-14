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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

public class OrderBookCsvExporter {
    public static void ensureCsvHasHeaderIfNeeded(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            try (FileWriter fw = new FileWriter(filePath)) {
                fw.write(
                      "ORDER_ID," +
                      "SELL_QT_TOTAL10," +
                      "SELL_QT_01,SELL_QT_02,SELL_QT_03,SELL_QT_04,SELL_QT_05,SELL_QT_06,SELL_QT_07,SELL_QT_08,SELL_QT_09,SELL_QT_10," +
                      "SELL_PRC_01,SELL_PRC_02,SELL_PRC_03,SELL_PRC_04,SELL_PRC_05,SELL_PRC_06,SELL_PRC_07,SELL_PRC_08,SELL_PRC_09,SELL_PRC_10," +
                      "BUY_QT_01,BUY_QT_02,BUY_QT_03,BUY_QT_04,BUY_QT_05,BUY_QT_06,BUY_QT_07,BUY_QT_08,BUY_QT_09,BUY_QT_10," +
                      "BUY_PRC_01,BUY_PRC_02,BUY_PRC_03,BUY_PRC_04,BUY_PRC_05,BUY_PRC_06,BUY_PRC_07,BUY_PRC_08,BUY_PRC_09,BUY_PRC_10," +
                      "BUY_QT_TOTAL10\n"
                );
            }
        }
    }


    public static void captureOrderBookToCsv(
        String filePath,
        String orderId,
        List<OrderBookProcessor.Level> asks,  // top 10 ask levels
        List<OrderBookProcessor.Level> bids   // top 10 bid levels
    ) {
        // Sum the top 10 ask qty
        BigDecimal askTotal = BigDecimal.ZERO;
        for (OrderBookProcessor.Level ask : asks) {
            askTotal = askTotal.add(ask.qty);
        }

        // Sum the top 10 bid qty
        BigDecimal bidTotal = BigDecimal.ZERO;
        for (OrderBookProcessor.Level bid : bids) {
            bidTotal = bidTotal.add(bid.qty);
        }

        // 1) ORDER_ID
        StringBuilder sb = new StringBuilder(orderId).append(",");

        // 2) SELL_QT_TOTAL10
        sb.append(askTotal).append(",");

        // 3) SELL_QT_01..SELL_QT_10
        for (int i = 0; i < 10; i++) {
            if (i < asks.size()) {
                sb.append(asks.get(i).qty);
            }
            sb.append(",");
        }

        // 4) SELL_PRC_01..SELL_PRC_10
        for (int i = 0; i < 10; i++) {
            if (i < asks.size()) {
                sb.append(asks.get(i).px);
            }
            sb.append(",");
        }

        // 5) BUY_QT_01..BUY_QT_10
        for (int i = 0; i < 10; i++) {
            if (i < bids.size()) {
                sb.append(bids.get(i).qty);
            }
            sb.append(",");
        }

        // 6) BUY_PRC_01..BUY_PRC_10
        for (int i = 0; i < 10; i++) {
            if (i < bids.size()) {
                sb.append(bids.get(i).px);
            }
            sb.append(",");
        }

        // 7) BUY_QT_TOTAL10
        sb.append(bidTotal);

        String line = sb.toString();
        try (FileWriter fw = new FileWriter(filePath, true)) {
            fw.write(line + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
