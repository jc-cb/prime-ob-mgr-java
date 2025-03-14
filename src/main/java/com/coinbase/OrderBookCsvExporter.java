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

    private static final String CSV_HEADER = String.join(",", new String[] {
        "EXCH_NM",
        "FINES_CD",
        "ASSET_CD",
        "TICKER_NM",
        "BASE_ASSET_CD",
        "BASE_TICKER",
        "ORDER_DATE",
        "ORDER_NO",
        "ORG_ORDER_NO",
        "ORDER_TIME",
        "ORDER_SIDE",
        "ORDER_CAT",
        "ORDER_TYPE",
        "RSV_ORDER_DATE",
        "TRIGGER_PRICE",
        "USER_ID",
        "ACC_NO",
        "ORDER_ID",
        "ORG_ORDER_ID",
        "USER_CAT",
        "UNIX_TIMESTAMP",
        "ORDER_PRICE",
        "ORDER_PRICE_KRW",
        "ORDER_QUANTITY",
        "ORDER_AMOUNT",
        "ORDER_COND",
        "PREV_TRADE_PRICE",
        "DIFF_PREV_TRADE_PRICE",
        "DIFF_COUNTER_PRICE",
        "ACCUM_TRADE_QUANTITY",
        "SELL_QT_TOTAL",
        "SELL_QT_TOTAL10",
        "SELL_QT_01",
        "SELL_QT_02",
        "SELL_QT_03",
        "SELL_QT_04",
        "SELL_QT_05",
        "SELL_QT_06",
        "SELL_QT_07",
        "SELL_QT_08",
        "SELL_QT_09",
        "SELL_QT_10",
        "SELL_PRC_01",
        "SELL_PRC_02",
        "SELL_PRC_03",
        "SELL_PRC_04",
        "SELL_PRC_05",
        "SELL_PRC_06",
        "SELL_PRC_07",
        "SELL_PRC_08",
        "SELL_PRC_09",
        "SELL_PRC_10",
        "BUY_QT_01",
        "BUY_QT_02",
        "BUY_QT_03",
        "BUY_QT_04",
        "BUY_QT_05",
        "BUY_QT_06",
        "BUY_QT_07",
        "BUY_QT_08",
        "BUY_QT_09",
        "BUY_QT_10",
        "BUY_PRC_01",
        "BUY_PRC_02",
        "BUY_PRC_03",
        "BUY_PRC_04",
        "BUY_PRC_05",
        "BUY_PRC_06",
        "BUY_PRC_07",
        "BUY_PRC_08",
        "BUY_PRC_09",
        "BUY_PRC_10",
        "BUY_QT_TOTAL10",
        "BUY_QT_TOTAL",
        "ORDER_CHANNEL",
        "PROG_ORDER",
        "DEVICE_ID",
        "IP_ADDR",
        "CONN_OS",
        "CONN_BROWS",
        "HTTP_USER_AGENT"
    });

    public static void ensureCsvHasHeaderIfNeeded(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            try (FileWriter fw = new FileWriter(file)) {
                fw.write(CSV_HEADER + "\n");
            }
        }
    }


    public static void captureOrderBookToCsv(
            String filePath,
            String orderId,
            List<OrderBookProcessor.Level> asksTop10,
            List<OrderBookProcessor.Level> bidsTop10,
            BigDecimal fullAsksTotal, 
            BigDecimal fullBidsTotal 
    ) {
     

        BigDecimal askTotal10 = BigDecimal.ZERO;
        for (OrderBookProcessor.Level ask : asksTop10) {
            askTotal10 = askTotal10.add(ask.qty);
        }

        BigDecimal bidTotal10 = BigDecimal.ZERO;
        for (OrderBookProcessor.Level bid : bidsTop10) {
            bidTotal10 = bidTotal10.add(bid.qty);
        }

        String[] columns = new String[81];

        columns[0]  = "";                      // EXCH_NM
        columns[1]  = "";                          // FINES_CD
        columns[2]  = "";                          // ASSET_CD
        columns[3]  = "";                          // TICKER_NM
        columns[4]  = "";                          // BASE_ASSET_CD
        columns[5]  = "";                    // BASE_TICKER
        columns[6]  = "";                          // ORDER_DATE
        columns[7]  = "";                          // ORDER_NO
        columns[8]  = "";                          // ORG_ORDER_NO
        columns[9]  = "";                          // ORDER_TIME
        columns[10] = "";                          // ORDER_SIDE
        columns[11] = "";                          // ORDER_CAT
        columns[12] = "";                          // ORDER_TYPE
        columns[13] = "";                          // RSV_ORDER_DATE
        columns[14] = "";                          // TRIGGER_PRICE
        columns[15] = "";                          // USER_ID
        columns[16] = "";                          // ACC_NO
        columns[17] = orderId;                     // ORDER_ID
        columns[18] = "";                          // ORG_ORDER_ID
        columns[19] = "";                          // USER_CAT
        columns[20] = "";                          // UNIX_TIMESTAMP
        columns[21] = "";                          // ORDER_PRICE
        columns[22] = "";                          // ORDER_PRICE_KRW
        columns[23] = "";                          // ORDER_QUANTITY
        columns[24] = "";                          // ORDER_AMOUNT
        columns[25] = "";                          // ORDER_COND
        columns[26] = "";                          // PREV_TRADE_PRICE
        columns[27] = "";                          // DIFF_PREV_TRADE_PRICE
        columns[28] = "";                          // DIFF_COUNTER_PRICE
        columns[29] = "";                          // ACCUM_TRADE_QUANTITY
   
        columns[30] = fullAsksTotal.toPlainString();
        columns[31] = askTotal10.toPlainString();


        // SELL_QT_01..SELL_QT_10
        for (int i = 0; i < 10; i++) {
            BigDecimal qty = (i < asksTop10.size()) ? asksTop10.get(i).qty : BigDecimal.ZERO;
            columns[32 + i] = qty.toPlainString();
        }
        // SELL_PRC_01..SELL_PRC_10
        for (int i = 0; i < 10; i++) {
            BigDecimal px = (i < asksTop10.size()) ? asksTop10.get(i).px : BigDecimal.ZERO;
            columns[42 + i] = px.toPlainString();
        }
        // BUY_QT_01..BUY_QT_10
        for (int i = 0; i < 10; i++) {
            BigDecimal qty = (i < bidsTop10.size()) ? bidsTop10.get(i).qty : BigDecimal.ZERO;
            columns[52 + i] = qty.toPlainString();
        }
        // BUY_PRC_01..BUY_PRC_10
        for (int i = 0; i < 10; i++) {
            BigDecimal px = (i < bidsTop10.size()) ? bidsTop10.get(i).px : BigDecimal.ZERO;
            columns[62 + i] = px.toPlainString();
        }

        // BUY_QT_TOTAL10
        columns[72] = bidTotal10.toPlainString();
        // BUY_QT_TOTAL (entire book)
        columns[73] = fullBidsTotal.toPlainString();

        columns[74] = "";   // ORDER_CHANNEL
        columns[75] = "";   // PROG_ORDER
        columns[76] = "";   // DEVICE_ID
        columns[77] = "";   // IP_ADDR
        columns[78] = "";   // CONN_OS
        columns[79] = "";   // CONN_BROWS
        columns[80] = "";   // HTTP_USER_AGENT

        // Build CSV line
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            sb.append(columns[i]);
            if (i < columns.length - 1) {
                sb.append(",");
            }
        }

        // Append to CSV
        try (FileWriter fw = new FileWriter(filePath, true)) {
            fw.write(sb.toString());
            fw.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
