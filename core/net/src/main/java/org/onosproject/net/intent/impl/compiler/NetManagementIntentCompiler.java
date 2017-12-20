package org.onosproject.net.intent.impl.compiler;

import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Component;
import org.onlab.graph.ScalarWeight;
import org.onosproject.net.DefaultPath;
import org.onosproject.net.Path;
import org.onosproject.net.intent.ConnectivityIntent;
import org.onosproject.net.intent.Constraint;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.net.topology.TopologyEdge;
import org.onosproject.net.intent.constraint.MarkerConstraint;
import org.onosproject.net.intent.constraint.PathViabilityConstraint;
import org.onlab.graph.Weight;

import java.util.Iterator;
import java.util.List;

/**
 * Network Management System base class for compilers of various
 * {@link org.onosproject.net.intent.ConnectivityIntent connectivity intents}.
 */
@Component(immediate = false)
public abstract class NetManagementIntentCompiler<T extends ConnectivityIntent>
        extends ConnectivityIntentCompiler<T> {

    protected class NetManConstraintBasedLinkWeight extends ConnectivityIntentCompiler<T>.ConstraintBasedLinkWeigher {

        /**
         * Creates a new edge-weight function capable of evaluating links
         * on the basis of the specified constraints.
         *
         * @param constraints path constraints
         */
        NetManConstraintBasedLinkWeight(List<Constraint> constraints) {
            super(constraints);
        }

        @Override
        public Weight weight(TopologyEdge edge) {

            // iterate over all constraints in order and return the weight of
            // the first one with fast fail over the first failure
            Iterator<Constraint> it = constraints.stream()
                    .filter(c -> !(c instanceof MarkerConstraint))
                    .filter(c -> !(c instanceof PathViabilityConstraint))
                    .iterator();

            if (!super.constraints.iterator().hasNext()) {
                return DEFAULT_HOP_WEIGHT;
            }

            double cost = 0.0;
            while (it.hasNext() && cost > 0) {
                Constraint constraint = it.next();
                // check if constraint is fullfiled
                Path path = new DefaultPath(ProviderId.NONE, Lists.newArrayList(edge.link()), 0);
                try {
                    if (!constraint.validate(path, resourceService::isAvailable)) {
                        // constraint not fullfilled -> set edge weight != 0 (shortest path)
                        cost += 1.0;
                    }
                } catch (Exception e) {
                    // exception means no knowledge over constraint -> set edge weight != 0 (shortest path)
                    cost += 1.0;
                    throw e;
                }
            }
            return ScalarWeight.toWeight(cost);
        }
    }
}
