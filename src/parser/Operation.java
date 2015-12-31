package parser;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import schema.Database;
import schema.Metadata;
import evaluation.Cursor;
import evaluation.TableAlias;
import evaluation.TableField;

public class Operation extends Expression {

	enum OPERATION_SIGN
	{
		EQUALS,
		LESS_THAN,
		GREATER_THAN,
		NOT_EQUALS,
		DIVIDE,
		PLUS,
		MINUS,
		TIMES,
		OR,
		AND
	};
	
	OPERATION_SIGN operationSign;
	String sign;
	
	public Operation(String sign, Expression l, Expression r)
	{
		super(l, r);
		switch (sign) {
		case "==":
			this.operationSign = OPERATION_SIGN.EQUALS;
			break;
		case "<":
			this.operationSign = OPERATION_SIGN.LESS_THAN;
			break;
		case ">":
			this.operationSign = OPERATION_SIGN.GREATER_THAN;
			break;
		case "!=":
			this.operationSign = OPERATION_SIGN.NOT_EQUALS;
			break;
		case "/":
			this.operationSign = OPERATION_SIGN.DIVIDE;
			break;
		case "+":
			this.operationSign = OPERATION_SIGN.PLUS;
			break;
		case "-":
			this.operationSign = OPERATION_SIGN.MINUS;
			break;
		case "*":
			this.operationSign = OPERATION_SIGN.TIMES;
			break;
		case "&&":
			this.operationSign = OPERATION_SIGN.AND;
			break;
		case "||":
			this.operationSign = OPERATION_SIGN.OR;
			break;

		default:
			break;
		}
		this.sign = sign;
	}
	
	public String toString()
	{
		return left.toString()+sign+right.toString();
	}
	
	public Object evaluate(Cursor m)
	{
		Object l = left.evaluate(m);
		Object r = right.evaluate(m);
		if (operationSign == OPERATION_SIGN.EQUALS) return new Boolean(l.equals(r));
		else if (operationSign == OPERATION_SIGN.LESS_THAN) return new Boolean(((Comparable)l).compareTo((Comparable)r) < 0);
		else if (operationSign == OPERATION_SIGN.PLUS) return new Integer((Integer)l + (Integer) r);
		else if (operationSign == OPERATION_SIGN.GREATER_THAN) return new Boolean(((Comparable)l).compareTo((Comparable)r) > 0);
		else if (operationSign == OPERATION_SIGN.NOT_EQUALS) return new Boolean(!l.equals(r));
		else if (operationSign == OPERATION_SIGN.DIVIDE) 
		{
			if (((Float)r).equals(0.0)) return new String("0");
			else return String.valueOf(Float.parseFloat(l.toString()) / Float.parseFloat(r.toString()));
		}
		else if (operationSign == OPERATION_SIGN.MINUS) return new Integer((Integer)l - (Integer) r);
		else if (operationSign == OPERATION_SIGN.TIMES) return new Integer((Integer)l * (Integer) r);
		else if (operationSign == OPERATION_SIGN.OR) return new Boolean((Boolean)l || (Boolean)r);
		else if (operationSign == OPERATION_SIGN.AND) return new Boolean((Boolean)l && (Boolean)r);
		else return null;
	}

	public Expression substitute(Map<? extends Expression, ? extends Expression> m)
	{
		Expression l=null, r=null;
		if (left!=null) l = left.substitute(m);
		if (right!=null) r = right.substitute(m);
		return new Operation(sign, l, r);	
	}
	
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		Operation other = (Operation)obj;
		if (!other.sign.equals(sign)) return false;
		if (  (sign.equals("=="))
			|| (sign.equals("!="))
			|| (sign.equals("+"))
			|| (sign.equals("*"))
			|| (sign.equals("&&"))
			|| (sign.equals("||")))
			return ((left.equals(other.left) && right.equals(other.right)) || (left.equals(other.right) && right.equals(other.left)));
		else return (left.equals(other.left) && right.equals(other.right));
	}
	
	public int hashCode()
	{
		return sign.hashCode() ^ left.hashCode() ^ right.hashCode();
	}
	
	public Set<TableAlias> getIncludedTables()
	{
		Set<TableAlias> result = left.getIncludedTables();
		result.addAll(right.getIncludedTables());
		return result;
	}

	public Set<TableField> getIncludedFields()
	{
		Set<TableField> result = new HashSet<TableField>();
		result.addAll(left.getIncludedFields());
		result.addAll(right.getIncludedFields());
		return result;
	}

	public Set<DatalogVariable> getIncludedDatalogVariables() {
		Set<DatalogVariable> result = new HashSet<DatalogVariable>();
		result.addAll(left.getIncludedDatalogVariables());
		result.addAll(right.getIncludedDatalogVariables());
		return result;
	}

	public boolean isEquality()
	{
		return sign.equals("==");
	}
	
	public boolean isAggregateFunction()
	{
		return false;
	}
	
	public Class getType(Database database)
	{
		String op = sign;
		if (op.equals("==") | op.equals("<") | op.equals(">") | op.equals("!=")) return Boolean.class;
		if (op.equals("/")) return String.class;
		
		Object l = left.getType(database);
		Object r = right.getType(database);
		if (l.equals(Integer.class) && r.equals(Integer.class))
		{
			if (op.equals("+") | op.equals("-") | op.equals("*")) return Integer.class;
			else return null;
		}
		else return null;
	}

	public Class getType(Metadata metadata)
	{
		String op = sign;
		if (op.equals("==") | op.equals("<") | op.equals(">") | op.equals("!=")) return Boolean.class;
		if (op.equals("/")) return String.class;
		
		Object l = left.getType(metadata);
		Object r = right.getType(metadata);
		if (l.equals(Integer.class) && r.equals(Integer.class))
		{
			if (op.equals("+") | op.equals("-") | op.equals("*")) return Integer.class;
			else return null;
		}
		else return null;
	}

	public String getSign()
	{
		return sign;
	}

}

