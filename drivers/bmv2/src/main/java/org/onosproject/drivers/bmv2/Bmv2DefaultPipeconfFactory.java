/*
 * Copyright 2017-present Open Networking Laboratory
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

package org.onosproject.drivers.bmv2;

import org.onosproject.bmv2.model.Bmv2PipelineModelParser;
import org.onosproject.net.pi.model.DefaultPiPipeconf;
import org.onosproject.net.pi.model.PiPipeconf;
import org.onosproject.net.pi.model.PiPipeconfId;
import org.onosproject.net.pi.model.PiPipelineInterpreter;

import java.io.InputStream;

import static org.onosproject.net.pi.model.PiPipeconf.ExtensionType.BMV2_JSON;
import static org.onosproject.net.pi.model.PiPipeconf.ExtensionType.P4_INFO_TEXT;

/**
 * Factory of pipeconf implementation for the default.p4 program on BMv2.
 */
final class Bmv2DefaultPipeconfFactory {

    private static final String PIPECONF_ID = "bmv2-default-pipeconf";
    private static final String JSON_PATH = "/default.json";
    private static final String P4INFO_PATH = "/default.p4info";

    private Bmv2DefaultPipeconfFactory() {
        // Hides constructor.
    }

    static PiPipeconf get() {

        final InputStream jsonConfigStream = Bmv2DefaultPipeconfFactory.class.getResourceAsStream(JSON_PATH);
        final InputStream p4InfoStream = Bmv2DefaultPipeconfFactory.class.getResourceAsStream(P4INFO_PATH);

        return DefaultPiPipeconf.builder()
                .withId(new PiPipeconfId(PIPECONF_ID))
                .withPipelineModel(Bmv2PipelineModelParser.parse(jsonConfigStream))
                // TODO: reuse default single table pipeliner.
                .addBehaviour(PiPipelineInterpreter.class, Bmv2DefaultInterpreter.class)
                .addExtension(P4_INFO_TEXT, p4InfoStream)
                .addExtension(BMV2_JSON, jsonConfigStream)
                .build();
    }
}