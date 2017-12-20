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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onlab.graph.DefaultEdgeWeigher;
import org.onlab.graph.ScalarWeight;
import org.onlab.graph.Weight;
import org.onlab.osgi.DefaultServiceDirectory;
import org.onlab.util.Bandwidth;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.DisjointPath;
import org.onosproject.net.ElementId;
import org.onosproject.net.Host;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.IPProtocolCriterion;
import org.onosproject.net.host.HostService;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.intent.ConnectivityIntent;
import org.onosproject.net.intent.Constraint;
import org.onosproject.net.intent.HostToHostIntent;
import org.onosproject.net.intent.IntentCompiler;
import org.onosproject.net.intent.IntentExtensionService;
import org.onosproject.net.intent.constraint.BandwidthConstraint;
import org.onosproject.net.intent.constraint.MarkerConstraint;
import org.onosproject.net.intent.constraint.PathViabilityConstraint;
import org.onosproject.net.intent.impl.PathNotFoundException;
import org.onosproject.net.link.LinkStore;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.net.resource.Resource;
import org.onosproject.net.resource.ResourceAllocation;
import org.onosproject.net.resource.ResourceConsumer;
import org.onosproject.net.resource.ResourceId;
import org.onosproject.net.resource.ResourceService;
import org.onosproject.net.resource.Resources;
import org.onosproject.net.topology.LinkWeigher;
import org.onosproject.net.topology.PathService;
import org.onosproject.net.topology.TopologyEdge;
import org.onosproject.net.topology.TopologyVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base class for compilers of various
 * {@link org.onosproject.net.intent.ConnectivityIntent connectivity intents}.
 */
@Component(immediate = true)
public abstract class ConnectivityIntentCompiler<T extends ConnectivityIntent>
        implements IntentCompiler<T> {

    private static final ProviderId PID = new ProviderId("core", "org.onosproject.core", true);

    private static final Logger log = LoggerFactory.getLogger(ConnectivityIntentCompiler.class);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected IntentExtensionService intentManager;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PathService pathService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ResourceService resourceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected HostService hostService;

    /**
     * Returns an edge-weight capable of evaluating links on the basis of the
     * specified constraints.
     *
     * @param constraints path constraints
     * @return edge-weight function
     */
    protected LinkWeigher weigher(List<Constraint> constraints) {
        return new ConstraintBasedLinkWeigher(constraints);
    }

    /**
     * Validates the specified path against the given constraints.
     *
     * @param path        path to be checked
     * @param constraints path constraints
     * @return true if the path passes all constraints
     */
    protected boolean checkPath(Path path, List<Constraint> constraints) {
        if (path == null) {
            return false;
        }
        for (Constraint constraint : constraints) {
            if (!constraint.validate(path, resourceService::isAvailable)) {
                return false;
            }
        }
        return true;
    }

    protected int getFiveTupleHash(ConnectivityIntent intent) {

        Map<Short, Criterion.Type> ipProtoSrcMap = new HashMap<Short, Criterion.Type>();
        ipProtoSrcMap.put((short) 6, Criterion.Type.TCP_SRC);
        ipProtoSrcMap.put((short) 17, Criterion.Type.UDP_SRC);
        ipProtoSrcMap.put((short) 132, Criterion.Type.SCTP_SRC);
        Map<Short, Criterion.Type> ipProtoDstMap = new HashMap<Short, Criterion.Type>();
        ipProtoDstMap.put((short) 6, Criterion.Type.TCP_DST);
        ipProtoDstMap.put((short) 17, Criterion.Type.UDP_DST);
        ipProtoDstMap.put((short) 132, Criterion.Type.SCTP_DST);

        TrafficSelector selector = intent.selector();

        int result = 0;

        // create hash code based on ip addresses
        if (intent.getClass().equals(HostToHostIntent.class)) {
            HostToHostIntent h2hIntent = (HostToHostIntent) intent;
            HostService hs = ((HostToHostIntentCompiler) this).hostService;
            Host[] hosts = {hs.getHost(h2hIntent.one()), hs.getHost(h2hIntent.two())};

            for (Host host : hosts) {
                if (host != null) {
                    result = 31 * result + (host.ipAddresses() != null ? host.ipAddresses().hashCode() : 0);
                }
            }
        }

        // ip protocol
        result = 31 * result + (selector.getCriterion(Criterion.Type.IP_PROTO) != null ?
                selector.getCriterion(Criterion.Type.IP_PROTO).hashCode() : 0);

        if (selector.getCriterion(Criterion.Type.IP_PROTO) != null) {
            IPProtocolCriterion ipProtoCrit = (IPProtocolCriterion) selector.getCriterion(Criterion.Type.IP_PROTO);
            // protocol src port
            if (ipProtoSrcMap.containsKey(ipProtoCrit.protocol())) {
                result = 31 * result + (selector.getCriterion(ipProtoSrcMap.get(ipProtoCrit.protocol())) != null ?
                        selector.getCriterion(ipProtoSrcMap.get(ipProtoCrit.protocol())).hashCode() : 0);
            }
            // protocol dst port
            if (ipProtoDstMap.containsKey(ipProtoCrit.protocol())) {
                result = 31 * result + (selector.getCriterion(ipProtoDstMap.get(ipProtoCrit.protocol())) != null ?
                        selector.getCriterion(ipProtoDstMap.get(ipProtoCrit.protocol())).hashCode() : 0);
            }
        }

        return result;
    }

    /**
     * Computes a path between two ConnectPoints.
     *
     * @param intent intent on which behalf path is being computed
     * @param one    start of the path
     * @param two    end of the path
     * @return Path between the two
     * @throws PathNotFoundException if a path cannot be found
     */
    @Deprecated
    protected Path getPathOrException(ConnectivityIntent intent,
                                      ElementId one, ElementId two) {
        Path path = getPath(intent, one, two);
        if (path == null) {
            throw new PathNotFoundException(one, two);
        }
        // TODO: let's be more intelligent about this eventually
        return path;
    }

    /**
     * Computes a path between two ConnectPoints.
     *
     * @param intent intent on which behalf path is being computed
     * @param one    start of the path
     * @param two    end of the path
     * @return Path between the two, or null if no path can be found
     */
    protected Path getPath(ConnectivityIntent intent,
                           ElementId one, ElementId two) {
        Set<Path> paths = pathService.getPaths(one, two, weigher(intent.constraints()));
        final List<Constraint> constraints = intent.constraints();
        ImmutableList<Path> filtered = FluentIterable.from(paths)
                .filter(path -> checkPath(path, constraints))
                .toList();
        if (filtered.isEmpty()) {
            return null;
        }

        // return the path based on the 5-tuple hash
        return filtered.get(Math.floorMod(this.getFiveTupleHash(intent), filtered.size()));
    }

    /**
     * Computes a disjoint path between two ConnectPoints.
     *
     * @param intent intent on which behalf path is being computed
     * @param one    start of the path
     * @param two    end of the path
     * @return DisjointPath         between the two
     * @throws PathNotFoundException if two paths cannot be found
     */
    protected DisjointPath getDisjointPath(ConnectivityIntent intent,
                           ElementId one, ElementId two) {
        Set<DisjointPath> paths = pathService.getDisjointPaths(one, two, weigher(intent.constraints()));
        final List<Constraint> constraints = intent.constraints();
        ImmutableList<DisjointPath> filtered = FluentIterable.from(paths)
                .filter(path -> checkPath(path, constraints))
                .filter(path -> checkPath(path.backup(), constraints))
                .toList();
        if (filtered.isEmpty()) {
            throw new PathNotFoundException(one, two);
        }

        // return the path based on the 5-tuple hash
        return filtered.get(Math.floorMod(this.getFiveTupleHash(intent), filtered.size()));
    }

    /**
     * Allocates the bandwidth specified as intent constraint on each link
     * composing the intent, if a bandwidth constraint is specified.
     *
     * @param intent the intent requesting bandwidth allocation
     * @param connectPoints the connect points composing the intent path computed
     */
    protected void allocateBandwidth(ConnectivityIntent intent,
                                     List<ConnectPoint> connectPoints) {
        // Retrieve bandwidth constraint if exists
        List<Constraint> constraints = intent.constraints();

        if (constraints == null) {
            return;
        }

        Optional<Constraint> constraint =
                constraints.stream()
                           .filter(c -> c instanceof BandwidthConstraint)
                           .findAny();

        // If there is no bandwidth constraint continue
        if (!constraint.isPresent()) {
            return;
        }

        BandwidthConstraint bwConstraint = (BandwidthConstraint) constraint.get();

        double bw = bwConstraint.bandwidth().bps();

        // If a resource group is set on the intent, the resource consumer is
        // set equal to it. Otherwise it's set to the intent key
        ResourceConsumer newResourceConsumer =
                intent.resourceGroup() != null ? intent.resourceGroup() : intent.key();

        // Get the list of current resource allocations
        Collection<ResourceAllocation> resourceAllocations =
                resourceService.getResourceAllocations(newResourceConsumer);

        // Get the list of resources already allocated from resource allocations
        List<Resource> resourcesAllocated =
                resourcesFromAllocations(resourceAllocations);

        // Get the list of resource ids for resources already allocated
        List<ResourceId> idsResourcesAllocated = resourceIds(resourcesAllocated);

        // Create the list of incoming resources requested. Exclude resources
        // already allocated.
        List<Resource> incomingResources =
                resources(connectPoints, bw).stream()
                                            .filter(r -> !resourcesAllocated.contains(r))
                                            .collect(Collectors.toList());

        if (incomingResources.isEmpty()) {
            return;
        }

        // Create the list of resources to be added, meaning their key is not
        // present in the resources already allocated
        List<Resource> resourcesToAdd =
                incomingResources.stream()
                                 .filter(r -> !idsResourcesAllocated.contains(r.id()))
                                 .collect(Collectors.toList());

        // Resources to updated are all the new valid resources except the
        // resources to be added
        List<Resource> resourcesToUpdate = Lists.newArrayList(incomingResources);
        resourcesToUpdate.removeAll(resourcesToAdd);

        // If there are no resources to update skip update procedures
        if (!resourcesToUpdate.isEmpty()) {
            // Remove old resources that need to be updated
            // TODO: use transaction updates when available in the resource service
            List<ResourceAllocation> resourceAllocationsToUpdate =
                    resourceAllocations.stream()
                            .filter(rA -> resourceIds(resourcesToUpdate).contains(rA.resource().id()))
                            .collect(Collectors.toList());
            log.debug("Releasing bandwidth for intent {}: {} bps", newResourceConsumer, resourcesToUpdate);
            resourceService.release(resourceAllocationsToUpdate);

            // Update resourcesToAdd with the list of both the new resources and
            // the resources to update
            resourcesToAdd.addAll(resourcesToUpdate);
        }

        // Look also for resources allocated using the intent key and -if any-
        // remove them
        if (intent.resourceGroup() != null) {
            // Get the list of current resource allocations made by intent key
            Collection<ResourceAllocation> resourceAllocationsByKey =
                    resourceService.getResourceAllocations(intent.key());

            resourceService.release(Lists.newArrayList(resourceAllocationsByKey));
        }

        // Allocate resources
        log.debug("Allocating bandwidth for intent {}: {} bps", newResourceConsumer, resourcesToAdd);
        List<ResourceAllocation> allocations =
                resourceService.allocate(newResourceConsumer, resourcesToAdd);

        if (allocations.isEmpty()) {
            log.debug("No resources allocated for intent {}", newResourceConsumer);
        }

        log.debug("Done allocating bandwidth for intent {}", newResourceConsumer);
    }

    /**
     * Produces a list of resources from a list of resource allocations.
     *
     * @param rAs the list of resource allocations
     * @return a list of resources retrieved from the resource allocations given
     */
    private static List<Resource> resourcesFromAllocations(Collection<ResourceAllocation> rAs) {
        return rAs.stream()
                  .map(ResourceAllocation::resource)
                  .collect(Collectors.toList());
    }

    /**
     * Creates a list of continuous bandwidth resources given a list of connect
     * points and a bandwidth.
     *
     * @param cps the list of connect points
     * @param bw the bandwidth expressed as a double
     * @return the list of resources
     */
    private static List<Resource> resources(List<ConnectPoint> cps, double bw) {
        return cps.stream()
                  // Make sure the element id is a valid device id
                  .filter(cp -> cp.elementId() instanceof DeviceId)
                  // Create a continuous resource for each CP we're going through
                  .map(cp -> Resources.continuous(cp.deviceId(), cp.port(),
                                                  Bandwidth.class).resource(bw))
                  .collect(Collectors.toList());
    }

    /**
     * Returns a list of resource ids given a list of resources.
     *
     * @param resources the list of resources
     * @return the list of resource ids retrieved from the resources given
     */
    private static List<ResourceId> resourceIds(List<Resource> resources) {
        return resources.stream()
                        .map(Resource::id)
                        .collect(Collectors.toList());
    }

    /**
     * Edge-weight capable of evaluating link cost using a set of constraints.
     */
    protected class ConstraintBasedLinkWeigher extends DefaultEdgeWeigher<TopologyVertex, TopologyEdge>
            implements LinkWeigher {

        protected final List<Constraint> constraints;

        // link store for up-to-date link information
        private final LinkStore linkStore = new DefaultServiceDirectory().get(LinkStore.class);

        /**
         * Creates a new edge-weight function capable of evaluating links
         * on the basis of the specified constraints.
         *
         * @param constraints path constraints
         */
        ConstraintBasedLinkWeigher(List<Constraint> constraints) {
            if (constraints == null) {
                this.constraints = Collections.emptyList();
            } else {
                this.constraints = ImmutableList.copyOf(constraints);
            }
        }

        @Override
        public Weight weight(TopologyEdge edge) {

            // iterate over all constraints in order and return the weight of
            // the first one with fast fail over the first failure
            Iterator<Constraint> it = constraints.stream()
                    .filter(c -> !(c instanceof MarkerConstraint))
                    .filter(c -> !(c instanceof PathViabilityConstraint))
                    .iterator();

            if (!it.hasNext()) {
                return DEFAULT_HOP_WEIGHT;
            }

            // FIXME: only returns the costs of the last constraint!
            double cost = it.next().cost(edge.link(), resourceService::isAvailable);
            while (it.hasNext() && cost > 0) {

                if (edge.link().type().equals(Link.Type.EDGE) || linkStore == null) {
                    // use provided link info for edge link
                    cost = it.next().cost(edge.link(), resourceService::isAvailable);
                } else {
                    // use updated link info from store for direct link
                    Link link = linkStore.getLink(edge.link().src(), edge.link().dst());
                    if (link != null) {
                        cost = it.next().cost(link, resourceService::isAvailable);
                    } else {
                        cost = it.next().cost(edge.link(), resourceService::isAvailable);
                    }
                }
                if (cost < 0) {
                    cost = -1;
                }
            }
            return ScalarWeight.toWeight(cost);

        }
    }

}
