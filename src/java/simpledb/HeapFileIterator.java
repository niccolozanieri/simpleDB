package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
	
	public HeapFileIterator(HeapFile file, TransactionId tid) {
		this.file = file;
		this.open = false;
		this.currentPage = 0;
		this.currentIt = null;
		this.tid = tid;
		this.numPages = file.numPages();
	}

	@Override
	public void open() throws DbException, TransactionAbortedException {
		BufferPool pool = Database.getBufferPool();
		PageId pageId = new HeapPageId(this.file.getId(), this.currentPage);
		Page page =  pool.getPage(this.tid, pageId, Permissions.READ_ONLY);
		
		if(page instanceof HeapPage) {
			this.currentIt = ((HeapPage)page).iterator();
		} else {
			// TODO: should be handled with exceptions
		}
		
		this.open = true;
	}

	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		boolean result = false;
		if(open) {
			if(this.currentIt.hasNext()) {
				result = true;
			} else {
				if(this.currentPage < this.numPages - 1) {
					BufferPool pool = Database.getBufferPool();
					PageId pageId = new HeapPageId(this.file.getId(), this.currentPage + 1);
					Page page =  pool.getPage(this.tid, pageId, Permissions.READ_ONLY);
					
					if(page instanceof HeapPage) {
						result = ((HeapPage)page).iterator().hasNext();
					} else {
						// TODO: should be handled with exceptions
					}
				}
			}
		}
		return result;
	}

	@Override
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
		Tuple result = null;
		
		if(this.hasNext()) {
			if(this.currentIt.hasNext()) {
				result = this.currentIt.next();
			} else {
				if(this.currentPage < this.numPages - 1) {
					BufferPool pool = Database.getBufferPool();
					PageId pageId = new HeapPageId(this.file.getId(), this.currentPage + 1);
					Page page =  pool.getPage(this.tid, pageId, Permissions.READ_ONLY);
					
					if(page instanceof HeapPage) {
						this.currentIt = ((HeapPage)page).iterator();
						this.currentPage++;
						
						result = this.currentIt.next();
					} else {
						// TODO: should be handled with exceptions
					}
				}
			}
		} else {
			throw new NoSuchElementException("No existing next element could be found");
		}
		
		return result;
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		if(open) {
			this.currentPage = 0;
			
			BufferPool pool = Database.getBufferPool();
			PageId pageId = new HeapPageId(this.file.getId(), this.currentPage);
			Page page =  pool.getPage(this.tid, pageId, Permissions.READ_ONLY);
			
			if(page instanceof HeapPage) {
				this.currentIt = ((HeapPage)page).iterator();
			} else {
				// TODO: should be handled with exceptions
			}
		}
        
	}

	@Override
	public void close() {

        this.open = false;
	}
	
	private HeapFile file;
	private int currentPage;
	private int numPages;
	private Iterator<Tuple> currentIt;
	private boolean open;
	private TransactionId tid;

}
