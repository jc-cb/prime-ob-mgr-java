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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class OrderBookProcessor {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final List<Level> bids = new ArrayList<>();
    private final List<Level> asks = new ArrayList<>();

    public OrderBookProcessor(String snapshotJson) {
        processSnapshot(snapshotJson);
    }

    public synchronized void processSnapshot(String snapshotJson) {
        try {
            JsonNode root = MAPPER.readTree(snapshotJson);
            JsonNode events = root.path("events");

            if (!events.isArray() || events.isEmpty()) {
                return;
            }

            JsonNode firstEvent = events.get(0);
            JsonNode updates = firstEvent.path("updates");
            if (!updates.isArray()) {
                return;
            }

            bids.clear();
            asks.clear();

            for (JsonNode levelNode : updates) {
                Level lvl = parseLevel(levelNode);
                if ("bid".equalsIgnoreCase(lvl.side)) {
                    bids.add(lvl);
                } else if ("offer".equalsIgnoreCase(lvl.side)) {
                    asks.add(lvl);
                }
            }

            sortAll();

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    public synchronized void applyUpdate(String updateJson) {
        try {
            JsonNode root = MAPPER.readTree(updateJson);
            String channel = root.path("channel").asText();
            if (!"l2_data".equals(channel)) {
                return;
            }

            JsonNode events = root.path("events");
            if (!events.isArray()) {
                return;
            }

            for (JsonNode event : events) {
                JsonNode updates = event.path("updates");
                if (!updates.isArray()) {
                    continue;
                }
                for (JsonNode upd : updates) {
                    Level lvl = parseLevel(upd);
                    applySingleLevel(lvl);
                }
            }
            filterClosed();
            sortAll();

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
    }

    private void applySingleLevel(Level lvl) {
        List<Level> list = "bid".equalsIgnoreCase(lvl.side) ? bids : asks;

        list.removeIf(existing -> existing.px.compareTo(lvl.px) == 0);

        if (lvl.qty.compareTo(BigDecimal.ZERO) > 0) {
            list.add(lvl);
        }
    }

    private void filterClosed() {
        bids.removeIf(lvl -> lvl.qty.compareTo(BigDecimal.ZERO) <= 0);
        asks.removeIf(lvl -> lvl.qty.compareTo(BigDecimal.ZERO) <= 0);
    }

    private void sortAll() {
        bids.sort(Comparator.comparing((Level l) -> l.px).reversed());
        asks.sort(Comparator.comparing((Level l) -> l.px));
    }

    private Level parseLevel(JsonNode node) {
        return new Level(
                new BigDecimal(node.path("px").asText()),
                new BigDecimal(node.path("qty").asText()),
                node.path("side").asText()
        );
    }

    public synchronized List<Level> getTopBids(int n) {
        int size = Math.min(n, bids.size());
        return new ArrayList<>(bids.subList(0, size));
    }

    public synchronized List<Level> getTopAsks(int n) {
        int size = Math.min(n, asks.size());
        return new ArrayList<>(asks.subList(0, size));
    }

    public synchronized BigDecimal getMidPrice() {
        if (bids.isEmpty() || asks.isEmpty()) {
            return null;
        }
        BigDecimal highestBid = bids.get(0).px;
        BigDecimal lowestAsk  = asks.get(0).px;
        return highestBid.add(lowestAsk).divide(BigDecimal.valueOf(2), RoundingMode.HALF_UP);
    }

    public static class Level {
        public BigDecimal px;
        public BigDecimal qty;
        public String side;

        public Level(BigDecimal px, BigDecimal qty, String side) {
            this.px = px;
            this.qty = qty;
            this.side = side;
        }

        @Override
        public String toString() {
            return side + ": px=" + px + ", qty=" + qty;
        }
    }
}
