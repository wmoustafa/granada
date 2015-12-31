package query.filter;

import java.util.Iterator;
import java.util.Set;

import evaluation.Cursor;
import evaluation.TableAlias;
import parser.Expression;
import schema.Database;
import schema.Tuple;
import utils.Log;

public abstract class Filter {
	
	Expression[] filterConditions;
	Iterator<Tuple> results;
	//Database inputDatabase;
	//Database outputDatabase;
	long executionTime=0;
	long openExecutionTime=0;
	long nextExecutionTime=0;
	long closeExecutionTime=0;

	Filter nextFilter=null;
	Cursor cursor=null;
	
	public abstract void open(Database inputDatabase, Database outputDatabase);
	public abstract void next();
	public abstract String toString();

	public Filter()
	{
	
	}
	
	public void setNextFilter(Filter f)
	{
		nextFilter = f;
		//nextFilter.inputDatabase = inputDatabase;
		//nextFilter.outputDatabase = outputDatabase;
	}
	
	//public void setDatabases(Database inputDatabase, Database outputDatabase)
	//{
		//this.inputDatabase = inputDatabase;
		//this.outputDatabase = outputDatabase;
	//}
		
	public Filter getNextFilter()
	{
		return nextFilter;
	}
	
	public void setInputCursor(Cursor cursor)
	{
		this.cursor = cursor;		
	}
	
	public void close()
	{
		if (nextFilter!=null) nextFilter.close();
		
	}
	public Database evaluate(Database inputDatabase)
	{
		Database outputDatabase = new Database();
		int cursorSize = 0;
		for (Filter f = this; f != null; f = f.nextFilter) cursorSize++;
		cursor = new Cursor(cursorSize - 1);
		open(inputDatabase, outputDatabase);		
		if (cursor!=null) next();
		close();
		return outputDatabase;
		//print();
	}
	
	public abstract Filter duplicate();

	public void print()
	{
		Log.DEBUG("***************************************************");
		Log.DEBUG(this.toString());
		if (nextFilter!=null) nextFilter.print();
	}

}
