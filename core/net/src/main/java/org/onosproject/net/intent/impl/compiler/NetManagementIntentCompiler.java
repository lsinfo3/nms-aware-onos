package org.onosproject.net.intent.impl.compiler;

import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Component;
import org.onosproject.net.DefaultPath;
import org.onosproject.net.Path;
import org.onosproject.net.intent.ConnectivityIntent;
import org.onosproject.net.intent.Constraint;
import org.onosproject.net.provider.ProviderId;
import org.onosproject.net.topology.TopologyEdge;

import java.util.Iterator;
import java.util.List;

/**
 * Network Management System base class for compilers of various
 * {@link org.onosproject.net.intent.ConnectivityIntent connectivity intents}.
 */
@Component(immediate = false)
public abstract class NetManagementIntentCompiler<T extends ConnectivityIntent>
        extends ConnectivityIntentCompiler<T> {

    protected class NetManConstraintBasedLinkWeight extends ConnectivityIntentCompiler<T>.ConstraintBasedLinkWeight {

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
        public double weight(TopologyEdge edge) {
            if (!constraints.iterator().hasNext()) {
                return 0.0;
            }

            // iterate over all constraints in order and return the weight
            Iterator<Constraint> it = constraints.iterator();

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
            return cost;
        }
    }
}
