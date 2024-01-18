package simpledb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, ArrayList<String>> map = new HashMap<>();


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	if(what != Op.COUNT) {
    		throw new IllegalArgumentException("StringAggregator only supports COUNT operation");
    	}
    	if(afield >= 0 && (gbfield >= 0 || (gbfieldtype == null && gbfield == Aggregator.NO_GROUPING))) {
    		this.gbfield = gbfield;
    		this.gbfieldtype = gbfieldtype;
    		this.afield = afield;
    		this.what = what;
    	}
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field;
    	
    	if(this.gbfield == Aggregator.NO_GROUPING) {
    		field = new IntField(42);
    	} else {
    		field = tup.getField(gbfield);
    	}
    	
    	if(!this.map.containsKey(field)) {
    		this.map.put(field, new ArrayList<String>());
    	}
    	
    	if(tup.getField(afield).getType() == Type.STRING_TYPE) {
			StringField f =(StringField) tup.getField(afield);
    		this.map.get(field).add(f.getValue());
    	} else {
    		// TODO: should be handled with exceptions
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
    	if(this.gbfield == Aggregator.NO_GROUPING) {
        	ArrayList<Tuple> iterable = new ArrayList<>();
        	Type[] types = new Type[] { Type.INT_TYPE };
        	String[] fields = new String[] { "aggregateVal" };
        	TupleDesc td = new TupleDesc(types, fields);
        	ArrayList<String> list = this.map.get(new IntField(42));
        	
            Integer agg = 0;
            
            switch(what) {
                case COUNT:
                	agg = list.size();
                    break;
                default:
                	break;
            }
            
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(agg));
            iterable.add(tuple);
            
            return new TupleIterator(td, iterable);
            
        } else {
        	ArrayList<Tuple> iterable = new ArrayList<>();
        	Type[] types = new Type[] { gbfieldtype, Type.INT_TYPE };
        	String[] fields = new String[] { "groupVal", "aggregateVal" };
        	TupleDesc td = new TupleDesc(types, fields);
        	
        	for(Map.Entry<Field, ArrayList<String>> entry : this.map.entrySet()) {
        		Integer agg = 0;
        		ArrayList<String> list = entry.getValue();
                
                switch(what) {
                    case COUNT:
                    	agg = list.size();
                        break;
                    default:
                    	break;
                }
                
                Tuple tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(agg));
                iterable.add(tuple);
                
                
        	}
        	
        	return new TupleIterator(td, iterable);
        }
    }

}
