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
package org.onosproject.net.intent.impl.compiler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableSet;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onosproject.net.DefaultAnnotations;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DefaultLink;
import org.onosproject.net.DefaultPath;
import org.onosproject.net.DeviceId;
import org.onosproject.net.FilteredConnectPoint;
import org.onosproject.net.Host;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.TcpPortCriterion;
import org.onosproject.net.flow.criteria.UdpPortCriterion;
import org.onosproject.net.host.HostService;
import org.onosproject.net.intent.Constraint;
import org.onosproject.net.intent.HostToHostIntent;
import org.onosproject.net.intent.Intent;
import org.onosproject.net.intent.PathIntent;
import org.onosproject.net.intent.constraint.AdvancedAnnotationConstraint;
import org.onosproject.net.intent.constraint.AnnotationConstraint;
import org.onosproject.net.intent.IntentCompilationException;
import org.onosproject.net.intent.LinkCollectionIntent;
import org.onosproject.net.intent.constraint.AsymmetricPathConstraint;
import org.onosproject.net.link.DefaultLinkDescription;
import org.onosproject.net.link.LinkDescription;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.link.LinkStore;
import org.onosproject.net.provider.ProviderId;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.onosproject.net.Link.Type.EDGE;
import static org.onosproject.net.flow.DefaultTrafficSelector.builder;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * A intent compiler for {@link HostToHostIntent}.
 */
@Component(immediate = true)
public class HostToHostIntentCompiler
        extends ConnectivityIntentCompiler<HostToHostIntent> {

    private final Logger log = getLogger(getClass());

    private static final String DEVICE_ID_NOT_FOUND = "Didn't find device id in the link";

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected HostService hostService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected LinkStore linkStore;

    @Activate
    public void activate() {
        intentManager.registerCompiler(HostToHostIntent.class, this);
    }

    @Deactivate
    public void deactivate() {
        intentManager.unregisterCompiler(HostToHostIntent.class);
    }

    @Override
    public synchronized List<Intent> compile(HostToHostIntent intent, List<Intent> installable) {
        // If source and destination are the same, there are never any installables.
        if (Objects.equals(intent.one(), intent.two())) {
            return ImmutableList.of();
        }

        boolean updatedOldPaths = true;
        if (intent.getPaths() == null) {
            updatedOldPaths = false;
        } else {
            // remove old link annotation values from the intents path
            removeOldAnnotationValues(intent);
        }

        boolean isAsymmetric = intent.constraints().contains(new AsymmetricPathConstraint());
        Path pathOne = getPathOrException(intent, intent.one(), intent.two());
        Path pathTwo = isAsymmetric ?
                getPathOrException(intent, intent.two(), intent.one()) : invertPath(pathOne);

        Host one = hostService.getHost(intent.one());
        Host two = hostService.getHost(intent.two());

        // update paths of intent
        intent.setPaths(Lists.newArrayList(pathOne, pathTwo));

        // only update new paths if the annotations where removed from the old ones
        // otherwise the network management system becomes unstable
        if (updatedOldPaths) {
            // add intents constraint values as link annotation values to new path
            addNewAnnotationValues(intent);
        }

        return Arrays.asList(createLinkCollectionIntent(pathOne, one, two, intent),
                             createLinkCollectionIntent(pathTwo, two, one, intent));
    }

    /**
     * Add constraint values of the intent to the link annotations.
     *
     * @param intent the intent to update values for
     */
    private void addNewAnnotationValues(HostToHostIntent intent) {
        List<Link> links = getLinks(intent);
        // update annotation values of links
        updateAnnotationValues(intent.constraints(), links, true);
    }

    /**
     * Remove constraint values of the intent from the old paths annotations.
     *
     * @param intent the intent to update values for
     */
    private void removeOldAnnotationValues(HostToHostIntent intent) {
        List<Link> links = getLinks(intent);
        // update annotation values of links
        updateAnnotationValues(intent.constraints(), links, false);
    }

    /**
     * Compute a list of all links in the paths of the HostToHostIntent.
     *
     * @param intent the intent with the path
     * @return List of links if paths are set, empty list otherwise
     */
    private List<Link> getLinks(HostToHostIntent intent) {
        if (intent.getPaths() != null) {
            // map path to list of links, map list of links to link,
            // filter only non edge links and collect it to a list
            // contains all links the intent is routed on
            return intent.getPaths().stream()
                    .map(Path::links)
                    .flatMap(ls -> ls.stream())
                    .filter(l -> l.type() != Link.Type.EDGE)
                    .collect(Collectors.toList());
        } else {
            return Lists.newArrayList();
        }
    }

    /**
     * Update link information in LinkManager.
     *
     * @param constraints the constraints the new link information is based on
     * @param links the links to update
     * @param add boolean determining whether values are added to the links or removed
     */
    private void updateAnnotationValues(List<Constraint> constraints, List<Link> links, boolean add) {

        // update each link
        for (Link intentLink : links) {
            // create new annotation for the link
            DefaultAnnotations.Builder newAnnotations = DefaultAnnotations.builder();
            Link storeLink = linkStore.getLink(intentLink.src(), intentLink.dst());

            // if theres no link in store, do nothing
            if (storeLink != null) {
                // iterrate through all existing annotations of the links
                for (String annotationKey : storeLink.annotations().keys()) {

                    String value = storeLink.annotations().value(annotationKey);

                    // check if the link annotation is updated by the intent constraints
                    for (Constraint constraint : constraints) {
                        String newValue = "";
                        if (add) {
                            newValue = addConstraintValues(storeLink.annotations().value(annotationKey),
                                    annotationKey, constraint);
                        } else {
                            newValue = removeConstraintValues(storeLink.annotations().value(annotationKey),
                                    annotationKey, constraint);
                        }
                        // only update value if constraint key corresponds to annotation key
                        if (!newValue.isEmpty()) {
                            value = newValue;
                        }
                    }

                    newAnnotations.set(annotationKey, value);

                }
                LinkDescription ld = new DefaultLinkDescription(
                        storeLink.src(),
                        storeLink.dst(),
                        storeLink.type(),
                        storeLink.isExpected(),
                        newAnnotations.build());
                // Do not trigger a TopologyUpdated event as no intent recompile is desired
                // TODO: Is link realy updated here? Look for "link" map in "ECLinkStore"
                // TODO: Look in "refreshLinkCache" method and there how the link and its annotations are composed.
                if (!storeLink.annotations().equals(ld.annotations())) {

                    boolean linkUpdated = false;

                    // try 10 times to add the link description
                    // FIXME: why is the link description not accepted by linkStore?
                    for (int i = 1; i <= 10 && !linkUpdated; i++) {
                        LinkEvent linkEvent = linkStore.createOrUpdateLink(
                                new ProviderId("h2h", "intentCompiler"), ld);
                        log.debug("H2HIntentCompiler: linkEvent={}", linkEvent);

                        Link newLink = linkStore.getLink(ld.src(), ld.dst());
                        if (newLink.annotations().equals(ld.annotations())) {
                            log.debug("H2HIntentCompiler: Updated link annotations for link=(src={}, dst={})",
                                    ld.src(), ld.dst());
                            break;
                        } else {
                            log.warn("H2HIntentCompiler: Link annotations NOT updated! Link=(src={}, dst={})." +
                                    " Trying again.", ld.src(), ld.dst());
                        }

                    }
                }
            }
        }
    }

    /**
     * Calculate the new value of the link annotation based on the intent constraint.
     *
     * @param linkAnnotationValue the annotation value of the link
     * @param annotationKey the annotation key to check
     * @param constraint the intent constraint
     * @return new value of the link annotation
     */
    private String addConstraintValues(String linkAnnotationValue, String annotationKey, Constraint constraint) {
        // only annotationConstraint holds a key
        if (constraint instanceof AnnotationConstraint) {
            AnnotationConstraint annotationConstraint = (AnnotationConstraint) constraint;
            // check if the constraint and the link annotation match
            if (annotationConstraint.key().equals(annotationKey)) {

                if (annotationConstraint instanceof AdvancedAnnotationConstraint) {
                    AdvancedAnnotationConstraint advConstraint = (AdvancedAnnotationConstraint) annotationConstraint;

                    if (advConstraint.isUpperLimit()) {
                        // is upper limit -> increase annotation value
                        return String.valueOf(((Double) (Double.valueOf(linkAnnotationValue)
                                + advConstraint.threshold())).intValue());
                    } else {
                        double result = ((Double) (Double.valueOf(linkAnnotationValue)
                                - advConstraint.threshold())).intValue();
                        // return only positive values
                        return result > 0 ? String.valueOf(result) : "0";
                    }
                } else {
                    // AnnotationConstraint has always an upper limit -> increase annotation value
                    return String.valueOf(((Double) (Double.valueOf(linkAnnotationValue)
                            + annotationConstraint.threshold())).intValue());
                }

            }
        }
        // boolean constraint has no key, or keys do not match
        return "";
    }


    /**
     * Calculate the new value of the link annotation based on the intent constraint.
     *
     * @param linkAnnotationValue the annotation value of the link
     * @param annotationKey the annotation key to check
     * @param constraint the intent constraint
     * @return new value of the link annotation
     */
    private String removeConstraintValues(String linkAnnotationValue, String annotationKey, Constraint constraint) {
        // only annotationConstraint holds a key
        if (constraint instanceof AnnotationConstraint) {
            AnnotationConstraint annotationConstraint = (AnnotationConstraint) constraint;

            // check if the constraint and the link annotation match
            if (annotationConstraint.key().equals(annotationKey)) {

                if (annotationConstraint instanceof AdvancedAnnotationConstraint) {
                    AdvancedAnnotationConstraint advConstraint = (AdvancedAnnotationConstraint) annotationConstraint;

                    if (advConstraint.isUpperLimit()) {
                        // is upper limit -> decrease annotation value
                        double result = Double.valueOf(linkAnnotationValue) - advConstraint.threshold();
                        // return only positive values
                        return result > 0 ? String.valueOf(result) : "0";
                    } else {
                        return String.valueOf(Double.valueOf(linkAnnotationValue) + advConstraint.threshold());
                    }

                } else {
                    // AnnotationConstraint has always an upper limit -> decrease annotation value
                    double result = Double.valueOf(linkAnnotationValue) - annotationConstraint.threshold();
                    // return only positive values
                    return result > 0 ? String.valueOf(result) : "0";
                }
            }
        }
        // boolean constraint has no key, or keys do not match
        return "";
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

    private FilteredConnectPoint getFilteredPointFromLink(Link link) {
        FilteredConnectPoint filteredConnectPoint;
        if (link.src().elementId() instanceof DeviceId) {
            filteredConnectPoint = new FilteredConnectPoint(link.src());
        } else if (link.dst().elementId() instanceof DeviceId) {
            filteredConnectPoint = new FilteredConnectPoint(link.dst());
        } else {
            throw new IntentCompilationException(DEVICE_ID_NOT_FOUND);
        }
        return filteredConnectPoint;
    }

    private Intent createLinkCollectionIntent(Path path,
                                             Host src,
                                             Host dst,
                                             HostToHostIntent intent) {
        // Try to allocate bandwidth
        List<ConnectPoint> pathCPs =
                path.links().stream()
                            .flatMap(l -> Stream.of(l.src(), l.dst()))
                            .collect(Collectors.toList());

        allocateBandwidth(intent, pathCPs);

        Link ingressLink = path.links().get(0);
        Link egressLink = path.links().get(path.links().size() - 1);

        FilteredConnectPoint ingressPoint = getFilteredPointFromLink(ingressLink);
        FilteredConnectPoint egressPoint = getFilteredPointFromLink(egressLink);

        TrafficSelector.Builder selectorBuilder = builder(intent.selector())
                .matchEthSrc(src.mac());

        // if source and destination are inverted, invert the traffic selector
        if (intent.one().equals(dst.id()) && intent.two().equals(src.id())) {
            invertSelector(selectorBuilder, intent);
        }

        /*
         * The path contains also the edge links, these are not necessary
         * for the LinkCollectionIntent.
         */
        Set<Link> coreLinks = path.links()
                .stream()
                .filter(link -> !link.type().equals(EDGE))
                .collect(Collectors.toSet());

        return LinkCollectionIntent.builder()
                .key(intent.key())
                .appId(intent.appId())
                .selector(selectorBuilder.build())
                .treatment(intent.treatment())
                .links(coreLinks)
                .filteredIngressPoints(ImmutableSet.of(
                        ingressPoint
                ))
                .filteredEgressPoints(ImmutableSet.of(
                        egressPoint
                ))
                .applyTreatmentOnEgress(true)
                .constraints(intent.constraints())
                .priority(intent.priority())
                .resourceGroup(intent.resourceGroup())
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
