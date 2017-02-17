/*
 * Copyright 2015-present Open Networking Laboratory
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
package org.onosproject.net.intent.impl.compiler;

import com.google.common.collect.ImmutableList;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onosproject.net.DefaultLink;
import org.onosproject.net.DefaultPath;
import org.onosproject.net.Host;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.TcpPortCriterion;
import org.onosproject.net.flow.criteria.UdpPortCriterion;
import org.onosproject.net.host.HostService;
import org.onosproject.net.intent.HostToHostIntent;
import org.onosproject.net.intent.Intent;
import org.onosproject.net.intent.PathIntent;
import org.onosproject.net.intent.constraint.AsymmetricPathConstraint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.onosproject.net.flow.DefaultTrafficSelector.builder;

/**
 * A intent compiler for {@link HostToHostIntent}.
 */
@Component(immediate = true)
public class HostToHostIntentCompiler
        extends ConnectivityIntentCompiler<HostToHostIntent> {

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected HostService hostService;

    @Activate
    public void activate() {
        intentManager.registerCompiler(HostToHostIntent.class, this);
    }

    @Deactivate
    public void deactivate() {
        intentManager.unregisterCompiler(HostToHostIntent.class);
    }

    @Override
    public List<Intent> compile(HostToHostIntent intent, List<Intent> installable) {
        // If source and destination are the same, there are never any installables.
        if (Objects.equals(intent.one(), intent.two())) {
            return ImmutableList.of();
        }

        boolean isAsymmetric = intent.constraints().contains(new AsymmetricPathConstraint());
        Path pathOne = getPath(intent, intent.one(), intent.two());
        Path pathTwo = isAsymmetric ?
                getPath(intent, intent.two(), intent.one()) : invertPath(pathOne);

        Host one = hostService.getHost(intent.one());
        Host two = hostService.getHost(intent.two());

        return Arrays.asList(createPathIntent(pathOne, one, two, intent),
                             createPathIntent(pathTwo, two, one, intent));
    }

    // Inverts the specified path. This makes an assumption that each link in
    // the path has a reverse link available. Under most circumstances, this
    // assumption will hold.
    private Path invertPath(Path path) {
        List<Link> reverseLinks = new ArrayList<>(path.links().size());
        for (Link link : path.links()) {
            reverseLinks.add(0, reverseLink(link));
        }
        return new DefaultPath(path.providerId(), reverseLinks, path.cost());
    }

    // Produces a reverse variant of the specified link.
    private Link reverseLink(Link link) {
        return DefaultLink.builder().providerId(link.providerId())
                .src(link.dst())
                .dst(link.src())
                .type(link.type())
                .state(link.state())
                .isExpected(link.isExpected())
                .build();
    }

    // Creates a path intent from the specified path and original connectivity intent.
    private Intent createPathIntent(Path path, Host src, Host dst,
                                    HostToHostIntent intent) {

        TrafficSelector.Builder selectorBuilder = builder(intent.selector())
                .matchEthSrc(src.mac()).matchEthDst(dst.mac());

        // if source and destination are inverted, invert the traffic selector
        if (intent.one().equals(dst.id()) && intent.two().equals(src.id())) {
            invertSelector(selectorBuilder, intent);
        }

        return PathIntent.builder()
                .appId(intent.appId())
                .selector(selectorBuilder.build())
                .treatment(intent.treatment())
                .path(path)
                .constraints(intent.constraints())
                .priority(intent.priority())
                .build();
    }

    private void invertSelector(TrafficSelector.Builder selectorBuilder, HostToHostIntent intent) {

        // all criterions types defined for the intent selector
        Set<Criterion.Type> criterionTypes = intent.selector().criteria().stream()
                .map(c -> c.type())
                .collect(Collectors.toSet());

        // if the intent selector has an ip protocol criterion
        if (criterionTypes.stream().anyMatch(type -> type.equals(Criterion.Type.IP_PROTO))) {

            // switch tcp/udp tpPort source and destination if present
            if (criterionTypes.stream().anyMatch(type -> type.equals(Criterion.Type.TCP_SRC))) {
                selectorBuilder.matchTcpDst(((TcpPortCriterion) intent.selector().getCriterion(Criterion.Type.TCP_SRC))
                                .tcpPort());
            }

            if (criterionTypes.stream().anyMatch(type -> type.equals(Criterion.Type.TCP_DST))) {
                selectorBuilder.matchTcpSrc(((TcpPortCriterion) intent.selector().getCriterion(Criterion.Type.TCP_DST))
                                .tcpPort());
            }

            if (criterionTypes.stream().anyMatch(type -> type.equals(Criterion.Type.UDP_SRC))) {
                selectorBuilder.matchUdpDst(((UdpPortCriterion) intent.selector().getCriterion(Criterion.Type.UDP_SRC))
                                .udpPort());
            }

            if (criterionTypes.stream().anyMatch(type -> type.equals(Criterion.Type.UDP_SRC))) {
                selectorBuilder.matchUdpSrc(((UdpPortCriterion) intent.selector().getCriterion(Criterion.Type.UDP_DST))
                                .udpPort());
            }
        }
    }

}
