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
import org.onlab.osgi.ServiceDirectory;
import org.onosproject.net.DisjointPath;
import org.onosproject.net.ElementId;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.intent.ConnectivityIntent;
import org.onosproject.net.intent.Constraint;
import org.onosproject.net.intent.IntentCompiler;
import org.onosproject.net.intent.IntentExtensionService;
import org.onosproject.net.intent.impl.PathNotFoundException;
import org.onosproject.net.link.LinkStore;
import org.onosproject.net.resource.ResourceQueryService;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.net.topology.LinkWeight;
import org.onosproject.net.topology.PathService;
import org.onosproject.net.topology.TopologyEdge;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Iterator;

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
        // FIXME: does the path service use the updated links of HostToHostIntentCompiler
        Set<Path> paths = pathService.getPaths(one, two, weight(intent.constraints()));
        final List<Constraint> constraints = intent.constraints();
        ImmutableList<Path> filtered = FluentIterable.from(paths)
                .filter(path -> checkPath(path, constraints))
                .toList();
        if (filtered.isEmpty()) {
            throw new PathNotFoundException(one, two);
        }
        // TODO: let's be more intelligent about this eventually
        // shuffling filtered list for a better distribution of the packet flows
        Random rand = new Random(System.nanoTime());
        return filtered.get(rand.nextInt(filtered.size()));
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
        // shuffling filtered list for a better distribution of the packet flows
        Random rand = new Random(System.nanoTime());
        return filtered.get(rand.nextInt(filtered.size()));
    }

    /**
     * Edge-weight capable of evaluating link cost using a set of constraints.
     */
    protected class ConstraintBasedLinkWeight implements LinkWeight {

        protected final List<Constraint> constraints;

        private ServiceDirectory services = new DefaultServiceDirectory();

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

                // use provided link info for edge link
                if (edge.link().type().equals(Link.Type.EDGE)) {
                    cost = it.next().cost(edge.link(), resourceService::isAvailable);
                } else {
                    // get the link store TODO: move out of the while loop
                    LinkStore linkStore = services.get(LinkStore.class);
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
