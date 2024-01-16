package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId t;
    private OpIterator child;
    private boolean finished;
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    	this.t=t;
    	this.child=child;
    	this.finished=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type[] types = new Type[] {Type.INT_TYPE };
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (this.finished)
    		return null;
    	Type[] types = new Type[] {Type.INT_TYPE }; 
    	String[] fields = new String[] {null };
    	TupleDesc td = new TupleDesc(types, fields);
    	Tuple tup = new Tuple(td);
    	int count=0;
        while(child.hasNext()) {
        	try {
        		Database.getBufferPool().deleteTuple(this.t, child.next());;
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
