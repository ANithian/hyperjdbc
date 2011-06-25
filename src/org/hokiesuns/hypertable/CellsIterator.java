package org.hokiesuns.hypertable;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.thrift.TException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.Cell;
import org.hypertable.thriftgen.ClientException;

public class CellsIterator implements Iterator<Cell>
{
    private ThriftClient mClient;
    private long mScanner;
    private Iterator<Cell> mIterator;
    
    public CellsIterator(ThriftClient pClient, long pScanner)
    throws ClientException, TException
    {
        mClient = pClient;
        mScanner = pScanner;
        
        List<Cell> lCurrentCells = pClient.next_cells(mScanner);
        mIterator = lCurrentCells.iterator();
    }
    
    @Override
    public boolean hasNext()
    {
        // TODO Auto-generated method stub
        if(mIterator == null)
            return false;
        else if(mIterator.hasNext())
            return true;
        else
        {
            fetchMore();
        }
        if(mIterator == null)
            return false;
        else
            return mIterator.hasNext();
    }

    private void fetchMore()
    {
        List<Cell> lCurrentCells;
        try
        {
            lCurrentCells = mClient.next_cells(mScanner);
            mIterator = lCurrentCells.iterator();
            if(!mIterator.hasNext())
            {
                mClient.close_scanner(mScanner);
                mIterator=null;
            }
            
        }
        catch (ClientException e)
        {
            e.printStackTrace();
        }
        catch (TException e)
        {
            e.printStackTrace();
        }        
    }
    @Override
    public Cell next()
    {
        // TODO Auto-generated method stub
        if(!hasNext())
            throw new NoSuchElementException();
        return mIterator.next();
    }

    @Override
    public void remove()
    {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

}
