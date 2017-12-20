package org.onosproject.net.intent.constraint;

import com.google.common.annotations.Beta;
import com.google.common.base.MoreObjects;
import org.onlab.osgi.DefaultServiceDirectory;
import org.onosproject.net.Link;
import org.onosproject.net.intent.ResourceContext;
import org.onosproject.net.link.LinkStore;

import java.util.Objects;

import static org.onosproject.net.AnnotationKeys.getAnnotatedValue;

/**
 * Created by lorry on 02.12.16.
 */
@Beta
public class AdvancedAnnotationConstraint extends AnnotationConstraint {

    // link store for up-to-date link information
    private final LinkStore linkStore = new DefaultServiceDirectory().get(LinkStore.class);

    private final boolean isUpperLimit;

    /**
     * Creates a new constraint to keep the value for the specified key
     * of link annotation under or over the threshold.
     *
     * @param key       key of link annotation
     * @param threshold threshold value of the specified link annotation
     * @param isUpperLimit true if threshold is the ceiling
     */
    public AdvancedAnnotationConstraint(String key, double threshold, boolean isUpperLimit) {
        super(key, threshold);
        this.isUpperLimit = isUpperLimit;
    }

    /**
     * Returns whether the threshold is an upper or lower limit.
     *
     * @return isUpperLimit as boolean
     */
    public boolean isUpperLimit() {
        return isUpperLimit;
    }

    @Override
    public boolean isValid(Link link, ResourceContext context) {
        // explicitly call a method not depending on LinkResourceService
        Link updatedLink = linkStore.getLink(link.src(), link.dst());
        if (updatedLink == null) {
            return isValid(link);
        } else {
            return isValid(updatedLink);
        }

    }

    private boolean isValid(Link link) {
        if (link.type() != Link.Type.EDGE) {
            double value = getAnnotatedValue(link, super.key());

            return isUpperLimit ? value <= super.threshold() : value >= super.threshold();
        } else {
            return true;
        }
    }

    // doesn't use LinkResourceService
    @Override
    public double cost(Link link, ResourceContext context) {
        // explicitly call a method not depending on LinkResourceService
        return cost(link);
    }

    private double cost(Link link) {
        if (isValid(link)) {
            // edge links do not have an annotation
            if (!link.type().equals(Link.Type.EDGE)) {
                // define reciprocal costs if isUpperLimit is false
                return isUpperLimit ? getAnnotatedValue(link, super.key()) :
                        (1.0 / getAnnotatedValue(link, super.key()));
            } else {
                return 1;
            }
        } else {
            return -1;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isUpperLimit);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        AdvancedAnnotationConstraint that = (AdvancedAnnotationConstraint) o;

        return isUpperLimit == that.isUpperLimit;

    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("key", super.key())
                .add("threshold", super.threshold())
                .add("isUpperLimit", isUpperLimit)
                .toString();
    }
}
