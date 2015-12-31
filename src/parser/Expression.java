package parser;

import java.util.Map;
import java.util.Set;

import schema.Database;
import schema.Metadata;
import evaluation.Cursor;
import evaluation.TableAlias;
import evaluation.TableField;

public abstract class Expression {

	Expression left;
	Expression right;
	
	public Expression(Expression l, Expression r)
	{
		left = l;
		right = r;
	}

	public abstract Object evaluate(Cursor m);
	public abstract String toString();
	public abstract boolean equals(Object e);
	public abstract int hashCode();
	public abstract Set<TableAlias> getIncludedTables();
	public abstract Set<TableField> getIncludedFields();
	public abstract Set<DatalogVariable> getIncludedDatalogVariables();
	public abstract boolean isEquality();
	public abstract Expression substitute(Map<? extends Expression, ? extends Expression> m);
	
	public boolean isAggregateFunction()
	{
		return false;
	}
	
	public Expression getLHS()
	{
		return left;
	}

	public Expression getRHS()
	{
		return right;
	}
	
	public abstract Class getType(Database database);
	
	public abstract Class getType(Metadata metadata);
	
}
