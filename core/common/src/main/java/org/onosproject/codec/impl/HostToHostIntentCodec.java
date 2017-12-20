/*
 * Copyright 2015-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.codec.impl;

import org.onosproject.codec.CodecContext;
import org.onosproject.codec.JsonCodec;
import org.onosproject.core.CoreService;
import org.onosproject.net.HostId;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.intent.ConnectivityIntent;
import org.onosproject.net.intent.HostToHostIntent;

import com.fasterxml.jackson.databind.node.ObjectNode;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.onlab.util.Tools.nullIsIllegal;

/**
 * Host to host intent codec.
 */
public final class HostToHostIntentCodec extends JsonCodec<HostToHostIntent> {

    private static final String ONE = "one";
    private static final String TWO = "two";

    @Override
    public ObjectNode encode(HostToHostIntent intent, CodecContext context) {
        checkNotNull(intent, "Host to host intent cannot be null");

        final JsonCodec<ConnectivityIntent> connectivityIntentCodec =
                context.codec(ConnectivityIntent.class);
        final ObjectNode result = connectivityIntentCodec.encode(intent, context);

        final String one = intent.one().toString();
        final String two = intent.two().toString();
        result.put(ONE, one);
        result.put(TWO, two);

        return result;
    }

    @Override
    public HostToHostIntent decode(ObjectNode json, CodecContext context) {
        HostToHostIntent.Builder builder = HostToHostIntent.builder();

        IntentCodec.intentAttributes(json, context, builder);
        ConnectivityIntentCodec.intentAttributes(json, context, builder);

        String one = nullIsIllegal(json.get(ONE),
                ONE + IntentCodec.MISSING_MEMBER_MESSAGE).asText();
        HostId oneId = HostId.hostId(one);
        builder.one(oneId);

        String two = nullIsIllegal(json.get(TWO),
                TWO + IntentCodec.MISSING_MEMBER_MESSAGE).asText();
        HostId twoId = HostId.hostId(two);
        builder.two(twoId);

        String appId = nullIsIllegal(json.get(IntentCodec.APP_ID),
                IntentCodec.APP_ID + IntentCodec.MISSING_MEMBER_MESSAGE).asText();

        // create the HostToHost selector specific key
        CoreService service = context.getService(CoreService.class);

        TrafficSelector selector = DefaultTrafficSelector.emptySelector();
        ObjectNode selectorJson = get(json, "selector");
        if (selectorJson != null) {
            JsonCodec<TrafficSelector> selectorCodec = context.codec(TrafficSelector.class);
            selector = selectorCodec.decode(selectorJson, context);
        }

        builder.key(HostToHostIntent.createSelectorKey(oneId, twoId, selector, service.getAppId(appId)));

        return builder.build();
    }
}
