package simpledb;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

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
    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            // some code goes here
            
            RandomAccessFile raf = new RandomAccessFile(file,"r");
            byte[] data = new byte[BufferPool.getPageSize()];
            
            raf.seek(BufferPool.getPageSize()*pid.getPageNumber());
            raf.readFully(data);
            raf.close();
            return new HeapPage((HeapPageId)pid,data);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(HeapFile.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException();
        } catch (IOException ex) {
            Logger.getLogger(HeapFile.class.getName()).log(Level.SEVERE, null, ex);
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }
    
    private class HeapFileIterator implements DbFileIterator {
        
        private HeapFile hfile;
        private TransactionId tid;
        private Iterator<Tuple> heapiter;
        private int pageNum = Integer.MIN_VALUE;
        private HeapPage currPage;
        
        private HeapFileIterator(HeapFile h, TransactionId t) {
            hfile = h;
            tid = t;
           
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            // set page to 0
            // iterater to something
            pageNum = 0;
            currPage = (HeapPage)Database.getBufferPool().getPage(tid,
                        new HeapPageId(hfile.getId(),pageNum), Permissions.READ_ONLY);
            heapiter = currPage.iterator();
            
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(heapiter ==  null) throw new IllegalStateException();
            if(heapiter.hasNext())
                return true;
       
            //check next page
            pageNum++;
            if(pageNum < hfile.numPages()) {
                currPage = (HeapPage)Database.getBufferPool().getPage(tid,
                   new HeapPageId(hfile.getId(),pageNum), Permissions.READ_ONLY);
                
          //  heapiter moves to next page if has next
             if(currPage.iterator().hasNext()) {
                 heapiter = currPage.iterator();
                 return true;
             }
            }
           //  else put pageNum back;
             pageNum--;
            
            return false;
            
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(heapiter == null) throw new IllegalStateException();
      
            return heapiter.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
           heapiter = null;
           pageNum = Integer.MAX_VALUE;
           
        }
    }

}

