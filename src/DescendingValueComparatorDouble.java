import java.util.Comparator;
import java.util.HashMap;

/**
 * A comparator for sorting objects of type T where T is a key in a HashMap. The 
 * objects are sorted in descending order of their corresponding values from the map.
 * For example, for two keys a and b, where map.get(a) = valA and map.get(b) = valB,
 * if valA.compareTo(valB)is 1, this comparator will return -1.
 * The map that will be used must be passed in the constructor. If the map is null, no
 * comparing will be done, we return 0 by default.
 * @author tanoojp
 *
 * @param <T>
 */
public class DescendingValueComparatorDouble<T> implements Comparator<T> {

	HashMap<T, Double> counts;
	public DescendingValueComparatorDouble(HashMap<T, Double> counts) {
		this.counts = counts;
	}

	@Override
	public int compare(T a, T b) {
		if(counts==null)
			return 0;
		if(counts.get(a)==null && counts.get(b)==null)
		{
			//Both are not present in map 
			return 0;
		}
		else if(counts.get(a)==null)
		{
			//Only a is not present in map
			return -1;
		}
		else if(counts.get(b)==null)
		{
			//Only b is not present in map
			return 1;
		}
		return -1*counts.get(a).compareTo(counts.get(b));
	}

}
