package parser;

import evaluation.Cursor;

public abstract class Term  extends Expression {

	public Term()
	{
		super(null,null);
	}
	
	public abstract Object evaluate(Cursor m);
	public abstract String toString();

	
}
