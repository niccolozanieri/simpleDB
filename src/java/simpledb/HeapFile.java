package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	
	private File file;
	private TupleDesc tupledesc;
    private RandomAccessFile randFile;
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.file=f;
    	this.tupledesc=td;
    	try {
    		this.randFile = new RandomAccessFile(f, "rw");
    	} catch(FileNotFoundException e) {
    		e.printStackTrace();
    	}
    	
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupledesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	int pageNo = pid.getPageNumber();
    	int offset = BufferPool.getPageSize() * pageNo;
    	
    	try {
			this.randFile.seek(offset);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	byte[] data = new byte[BufferPool.getPageSize()];
    	
    	for(int i = 0; i < BufferPool.getPageSize(); i++) {
    		try {
				data[i] = this.randFile.readByte();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	Page result = null;
		try {
			result = new HeapPage((HeapPageId)pid, data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return result;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
    	
    	this.randFile.seek(page.getId().getPageNumber() * Database.getBufferPool().getPageSize());
    	this.randFile.write(page.getPageData());
      //TODO close file?
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	int result = (int)(file.length() / BufferPool.getPageSize());
        return result;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	
    	BufferPool bf= Database.getBufferPool();
    	for (int i=0; i<this.numPages(); i++) { //iterate through all the pages in the db
    		HeapPageId pid = new HeapPageId(this.getId(), i);
    		Page p = bf.getPage(tid, pid, Permissions.READ_ONLY);
    		
    		HeapPage hp = (HeapPage) p;
    		
    		if(hp.getNumEmptySlots()>0 && hp.getTupleDesc().equals(t.getTupleDesc())) { //if there are empty slots, insert tuple
    			
    			try {
    				hp.insertTuple(t); //TODO recordId
    			}
    			catch (DbException d) {
    				throw new DbException(d.getMessage()); //should not happen
    			}
    			return new ArrayList<Page> (Arrays.asList(p));
    			
    		}
    		
    		
    	}
    	//if there is no space, we create a new page
    	HeapPageId pid = new HeapPageId(this.getId(), this.numPages()); 
    	HeapPage newp = new HeapPage(pid, new byte[bf.getPageSize()]);
    	writePage(newp);
    	newp.insertTuple(t);
    	return new ArrayList<Page> (Arrays.asList(newp));   	
        
        
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
    	BufferPool bf= Database.getBufferPool();
    	HeapPage hp= (HeapPage)bf.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	try {
    		hp.deleteTuple(t);
    	}
    	catch(DbException d) {
    		throw new DbException(d.getMessage());
    	}
    	
        return null;
        
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	
    	return new HeapFileIterator(this, tid);
    }

}

