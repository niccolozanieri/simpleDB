package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private static final long serialVersionUID = 1L;
    private ArrayList<TDItem> items;
    
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
        
        public Type getType() {
        	return this.fieldType;
        }
        
        public String getName() {
        	return this.fieldName;        
        }
       
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return items.iterator();
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	this.items=new ArrayList<TDItem>();
    	for(int i=0; i<typeAr.length; i++) {
    		TDItem n;
    		if (fieldAr[i]==null){
    			n=new TDItem(typeAr[i], null);
    		}
    		else {
    			n=new TDItem(typeAr[i], fieldAr[i]);
    		}
    		this.items.add(n);
    	}
    		
    		
    	}
    

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	this.items=new ArrayList<TDItem>();
    	for(int i=0; i<typeAr.length; i++) {
    		TDItem n;
    		
    		n=new TDItem(typeAr[i], null);
    		
    		this.items.add(n);
    	}
    		
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if (i>this.numFields()) {
    		throw new NoSuchElementException();
    	}
    	
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if (i>this.numFields()) {
    		throw new NoSuchElementException();
    	
    	}
    	
        return items.get(i).fieldType;
        
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	
    	for (int i = 0; i < this.items.size(); i++) {
    		if (this.items.get(i).fieldName != null) {
	    		if ( this.items.get(i).fieldName.compareTo(name) == 0) {
	    			return i;
	    		}
    		}
    	}
    	
    		
    	throw new NoSuchElementException("" + this.items.size());
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int tot=0;
    	for(int i=0; i<this.numFields(); i++) {
    		tot=tot+this.items.get(i).getType().getLen();  	}
        return tot;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	List<Type> t1=td1.items.stream().map(v->v.getType()).collect(Collectors.toList());
    	List<Type> t2=td2.items.stream().map(v->v.getType()).collect(Collectors.toList());
    	
    	List<String> s1=td1.items.stream().map(v->v.getName()).collect(Collectors.toList());
    	List<String> s2=td2.items.stream().map(v->v.getName()).collect(Collectors.toList());
    	
    	List<Type> concat_type=new ArrayList<Type>();
    	concat_type.addAll(t1);
    	concat_type.addAll(t2);
    	List<String> concat_name=new ArrayList<String>();
    	concat_name.addAll(s1);
    	concat_name.addAll(s2);
    	
    	TupleDesc n=new TupleDesc(concat_type.toArray(new Type[concat_type.size()]), concat_name.toArray(new String[concat_name.size()]));
    	
        return n;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	if (o instanceof TupleDesc) {
    		TupleDesc t=(TupleDesc) o;
    		if (t.numFields()!=this.numFields())
    			return false;
    		
    	    for(int i=0; i<this.numFields(); i++) {
    	    	if (this.getFieldName(i)!=t.getFieldName(i))
    	    		return false;
    	    	if (this.getFieldType(i)!=t.getFieldType(i))
    	    		return false;
    	    		
    	    }
    	}
    	
    	else 
    		return false;
    	return true;
    	
        
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	String res="";
    	for(int i=0; i<this.numFields(); i++) {
    		res=res+this.items.get(i).getType().toString()+ "(" + this.items.get(i).getName()+ ")";
    		if (i!=this.numFields()-1)
    			res=res+",";
    	}
        return res;
    }
}
