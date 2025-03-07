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

import java.time.Instant;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class AuthUtils {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String createAuthMessage(String channel,
                                           String productId,
                                           String passphrase,
                                           String accessKey,
                                           String secretKey,
                                           String svcAccountId) {

        String timestamp = String.valueOf(Instant.now().getEpochSecond());

        String signature = sign(channel, accessKey, secretKey, svcAccountId, productId, timestamp);

        ObjectNode root = MAPPER.createObjectNode();
        root.put("type", "subscribe");
        root.put("channel", channel);
        root.put("access_key", accessKey);
        root.put("api_key_id", svcAccountId);
        root.put("timestamp", timestamp);
        root.put("passphrase", passphrase);
        root.put("signature", signature);
        root.putArray("product_ids").add(productId);

        try {
            return MAPPER.writeValueAsString(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create auth message JSON", e);
        }
    }

    private static String sign(String channel,
                               String accessKey,
                               String secretKey,
                               String svcAccountId,
                               String productId,
                               String timestamp) {

        try {

            String message = channel + accessKey + svcAccountId + timestamp + productId;

            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secret_key = new SecretKeySpec(secretKey.getBytes("UTF-8"), "HmacSHA256");
            sha256_HMAC.init(secret_key);

            byte[] rawHmac = sha256_HMAC.doFinal(message.getBytes("UTF-8"));
            return Base64.getEncoder().encodeToString(rawHmac);
        } catch (Exception e) {
            throw new RuntimeException("Failed to sign message", e);
        }
    }
}
