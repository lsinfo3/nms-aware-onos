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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onlab.osgi.DefaultServiceDirectory;
import org.onosproject.net.DisjointPath;
import org.onosproject.net.ElementId;
import org.onosproject.net.Host;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.IPProtocolCriterion;
import org.onosproject.net.host.HostService;
import org.onosproject.net.intent.ConnectivityIntent;
import org.onosproject.net.intent.Constraint;
import org.onosproject.net.intent.HostToHostIntent;
import org.onosproject.net.intent.IntentCompiler;
import org.onosproject.net.intent.IntentExtensionService;
import org.onosproject.net.intent.impl.PathNotFoundException;
import org.onosproject.net.link.LinkStore;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.net.resource.ResourceQueryService;
import org.onosproject.net.topology.LinkWeight;
import org.onosproject.net.topology.PathService;
import org.onosproject.net.topology.TopologyEdge;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for compilers of various
 * {@link org.onosproject.net.intent.ConnectivityIntent connectivity intents}.
 */
@Component(immediate = true)
public abstract class ConnectivityIntentCompiler<T extends ConnectivityIntent>
        implements IntentCompiler<T> {

    private static final ProviderId PID = new ProviderId("core", "org.onosproject.core", true);

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected IntentExtensionService intentManager;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PathService pathService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected ResourceQueryService resourceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected HostService hostService;

    /**
     * Returns an edge-weight capable of evaluating links on the basis of the
     * specified constraints.
     *
     * @param constraints path constraints
     * @return edge-weight function
     */
    protected LinkWeight weight(List<Constraint> constraints) {
        return new ConstraintBasedLinkWeight(constraints);
    }

    /**
     * Validates the specified path against the given constraints.
     *
     * @param path        path to be checked
     * @param constraints path constraints
     * @return true if the path passes all constraints
     */
    protected boolean checkPath(Path path, List<Constraint> constraints) {
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
    protected Path getPath(ConnectivityIntent intent,
                           ElementId one, ElementId two) {
        Set<Path> paths = pathService.getPaths(one, two, weight(intent.constraints()));
        final List<Constraint> constraints = intent.constraints();
        ImmutableList<Path> filtered = FluentIterable.from(paths)
                .filter(path -> checkPath(path, constraints))
                .toList();
        if (filtered.isEmpty()) {
            throw new PathNotFoundException(one, two);
        }
        // TODO: let's be more intelligent about this eventually
        // return the path based on the 5-tuple hash
        return filtered.get(Math.floorMod(getFiveTupleHash(intent), filtered.size()));
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
        Set<DisjointPath> paths = pathService.getDisjointPaths(one, two, weight(intent.constraints()));
        final List<Constraint> constraints = intent.constraints();
        ImmutableList<DisjointPath> filtered = FluentIterable.from(paths)
                .filter(path -> checkPath(path, constraints))
                .toList();
        if (filtered.isEmpty()) {
            throw new PathNotFoundException(one, two);
        }
        // TODO: let's be more intelligent about this eventually
        // return the path based on the 5-tuple hash
        return filtered.get(Math.floorMod(getFiveTupleHash(intent), filtered.size()));
    }

    /**
     * Edge-weight capable of evaluating link cost using a set of constraints.
     */
    protected class ConstraintBasedLinkWeight implements LinkWeight {

        protected final List<Constraint> constraints;
        // link store for up-to-date link information
        private final LinkStore linkStore = new DefaultServiceDirectory().get(LinkStore.class);

        /**
         * Creates a new edge-weight function capable of evaluating links
         * on the basis of the specified constraints.
         *
         * @param constraints path constraints
         */
        ConstraintBasedLinkWeight(List<Constraint> constraints) {
            if (constraints == null) {
                this.constraints = Collections.emptyList();
            } else {
                this.constraints = ImmutableList.copyOf(constraints);
            }
        }

        @Override
        public double weight(TopologyEdge edge) {
            if (!constraints.iterator().hasNext()) {
                return 1.0;
            }

            // iterate over all constraints in order and return the weight of
            // the first one with fast fail over the first failure
            Iterator<Constraint> it = constraints.iterator();
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
                    return -1;
                }
            }
            return cost;

        }
    }

}
