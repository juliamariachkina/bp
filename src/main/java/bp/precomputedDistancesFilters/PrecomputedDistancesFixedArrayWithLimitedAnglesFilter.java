package bp.precomputedDistancesFilters;

import messif.objects.LocalAbstractObject;
import messif.objects.PrecomputedDistancesFilter;
import messif.objects.PrecomputedDistancesFixedArrayFilter;
import messif.objects.nio.BinaryInput;
import messif.objects.nio.BinaryOutput;
import messif.objects.nio.BinarySerializator;
import messif.objects.util.AbstractObjectIterator;
import messif.objects.util.AbstractObjectList;

import java.io.IOException;
import java.util.Map;

public class PrecomputedDistancesFixedArrayWithLimitedAnglesFilter extends PrecomputedDistancesFixedArrayFilter {
    private final Map<String, Float> pivotCoefs;

    public PrecomputedDistancesFixedArrayWithLimitedAnglesFilter(Map<String, Float> pivotCoefs) {
        super();
        this.pivotCoefs = pivotCoefs;
    }

    public PrecomputedDistancesFixedArrayWithLimitedAnglesFilter(int initialSize, Map<String, Float> pivotCoefs) {
        super(initialSize);
        this.pivotCoefs = pivotCoefs;
    }

    public PrecomputedDistancesFixedArrayWithLimitedAnglesFilter(LocalAbstractObject object, Map<String, Float> pivotCoefs) {
        super(object);
        this.pivotCoefs = pivotCoefs;
    }

    public PrecomputedDistancesFixedArrayWithLimitedAnglesFilter(LocalAbstractObject object, int initialSize,
                                                                 Map<String, Float> pivotCoefs) {
        super(object, initialSize);
        this.pivotCoefs = pivotCoefs;
    }

    public PrecomputedDistancesFixedArrayWithLimitedAnglesFilter(BinaryInput input, BinarySerializator serializator,
                                                                 Map<String, Float> pivotCoefs) throws IOException {
        super(input, serializator);
        this.pivotCoefs = pivotCoefs;
    }

    public synchronized float addPrecompDist(LocalAbstractObject p, LocalAbstractObject o) {
        if (p != null && o != null) {
            float distance = p.getDistance(o) * (
                    pivotCoefs.containsKey(p.getLocatorURI()) ?
                            pivotCoefs.get(p.getLocatorURI()) :
                            pivotCoefs.containsKey(o.getLocatorURI()) ?
                                    pivotCoefs.get(o.getLocatorURI()) :
                                    1);
            this.addPrecompDist(distance);
            return distance;
        } else
            return (float) (-1.0F / 0.0);
    }

    public synchronized int addPrecompDist(AbstractObjectList<LocalAbstractObject> pivots, LocalAbstractObject obj) {
        if (pivots != null && obj != null) {
            resizePrecompDistArray(pivots.size() + actualSize);

            for (LocalAbstractObject pivot : pivots) {
                precompDist[actualSize++] = pivot.getDistance(obj) * (pivotCoefs.containsKey(pivot.getLocatorURI()) ?
                        pivotCoefs.get(pivot.getLocatorURI()) :
                        1);
            }
        }
        return actualSize;
    }

    public synchronized int addPrecompDist(LocalAbstractObject[] pivots, LocalAbstractObject obj) {
        if (pivots != null && obj != null) {
            this.resizePrecompDistArray(pivots.length + actualSize);
            for (LocalAbstractObject pivot : pivots) {
                precompDist[actualSize++] = pivot.getDistance(obj) * (pivotCoefs.containsKey(pivot.getLocatorURI()) ?
                        pivotCoefs.get(pivot.getLocatorURI()) :
                        1);
            }
        }
        return this.actualSize;
    }

    public synchronized float insertPrecompDist(int pos, LocalAbstractObject p, LocalAbstractObject o) throws IndexOutOfBoundsException {
        if (p != null && o != null) {
            float d = p.getDistance(o) * (
                    pivotCoefs.containsKey(p.getLocatorURI()) ?
                            pivotCoefs.get(p.getLocatorURI()) :
                            pivotCoefs.containsKey(o.getLocatorURI()) ?
                                    pivotCoefs.get(o.getLocatorURI()) :
                                    1);
            ;
            this.insertPrecompDist(pos, d);
            return d;
        } else {
            return (float) (-1.0F / 0.0);
        }
    }

    public float setPrecompDist(int pos, LocalAbstractObject p, LocalAbstractObject o) throws IndexOutOfBoundsException {
        if (p != null && o != null) {
            float d = p.getDistance(o) * (
                    pivotCoefs.containsKey(p.getLocatorURI()) ?
                            pivotCoefs.get(p.getLocatorURI()) :
                            pivotCoefs.containsKey(o.getLocatorURI()) ?
                                    pivotCoefs.get(o.getLocatorURI()) :
                                    1);
            ;
            this.setPrecompDist(pos, d);
            return d;
        } else
            return (float) (-1.0F / 0.0);
    }

    public Object clone() throws CloneNotSupportedException {
        PrecomputedDistancesFixedArrayWithLimitedAnglesFilter rtv = (PrecomputedDistancesFixedArrayWithLimitedAnglesFilter) super.clone();
        if (rtv.precompDist != null) {
            float[] origArray = rtv.precompDist;
            rtv.precompDist = new float[origArray.length];
            System.arraycopy(origArray, 0, rtv.precompDist, 0, origArray.length);
        }
        return rtv;
    }

    public int binarySerialize(BinaryOutput output, BinarySerializator serializator) throws IOException {
        return super.binarySerialize(output, serializator) + serializator.write(output, actualSize) + serializator.write(output, precompDist);
    }

    public int getBinarySize(BinarySerializator serializator) {
        return super.getBinarySize(serializator) + 4 + serializator.getBinarySize(precompDist);
    }
}
