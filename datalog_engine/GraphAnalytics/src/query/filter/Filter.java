package query.filter;

import java.util.Iterator;

import parser.Expression;
import schema.Database;
import schema.Metadata;
import schema.Tuple;
import evaluation.Cursor;

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
	
	public abstract void open(Database inputDatabase, Database outputDatabase, Metadata metadata);
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
	public Database evaluate(Database inputDatabase, Metadata metadata)
	{
		Database outputDatabase = new Database();
		int cursorSize = 0;
		for (Filter f = this; f != null; f = f.nextFilter) cursorSize++;
		cursor = new Cursor(cursorSize - 1);
		open(inputDatabase, outputDatabase, metadata);		
		if (cursor!=null) next();
		close();
//		System.out.println("Evaluate plan:");
		print();
		return outputDatabase;
		
	}
	
	public abstract Filter duplicate();

	public void print()
	{
		//System.out.println("***************************************************");
//		System.out.println(this.toString());
		if (nextFilter!=null) nextFilter.print();
	}

}
