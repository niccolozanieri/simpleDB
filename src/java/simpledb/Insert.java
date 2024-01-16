package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private boolean finished;
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
    	this.t=t;
    	this.child=child;
    	this.tableId=tableId;
    	this.finished=false;
    	
    	if (!child.getTupleDesc().equals(Database.getCatalog().getDatabaseFile(this.tableId).getTupleDesc()))
    		throw new DbException("The tuple desc don't match");
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] types = new Type[] {Type.INT_TYPE }; // TODO: is int ok for returning avg?
    	String[] fields = new String[] { null };
    	TupleDesc t=new TupleDesc(types, fields);
        return t;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    	super.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    	this.finished=false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(this.finished)
    		return null;
    	Type[] types = new Type[] {Type.INT_TYPE };
    	String[] fields = new String[] {null};
    	TupleDesc td = new TupleDesc(types, fields);
    	Tuple tup = new Tuple(td);
    	int count=0;
        while(child.hasNext()) {
        	
        	try {
        		Database.getBufferPool().insertTuple(this.t, this.tableId, child.next());
        		
        	}
        	catch (TransactionAbortedException e) {
        		throw new TransactionAbortedException();
        	}
        	catch(DbException d) {
        		throw new DbException ("insertion of tuple failed");
        		}
        	catch (IOException i) {
        		//TODO
        	}
        	count=count+1;       	
        }
        IntField f= new IntField(count);
        tup.setField(0, f);
        this.finished=true;
        return tup;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    	if (children.length==1) {
    		this.child=children[0];
    	}
    	else {
    		//TODO
    	}
    }
}
