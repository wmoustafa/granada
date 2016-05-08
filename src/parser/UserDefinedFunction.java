package parser;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import evaluation.Cursor;
import evaluation.TableAlias;
import evaluation.TableField;
import schema.Database;
import schema.Metadata;

public class UserDefinedFunction extends Expression {

	String name;
	List<Expression> args = new ArrayList<Expression>();
	Method function;

	public UserDefinedFunction()
	{
		super(null, null);

	}
	public UserDefinedFunction(String name, List<Expression> args)
	{
		super(null, null);
		this.name = name;
		this.args = args;
		if (!isAggregateFunction())
		{
			Class[] paramTypes = new Class[args.size()];
			Arrays.fill(paramTypes, Object.class);
			try
			{
				function = udf.UserDefinedFunction.class.getMethod(name, paramTypes);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public int evaluate(Cursor m) {
		List<Object> argEvaluations = new ArrayList<Object>();
		for (Expression arg : args)
			argEvaluations.add(arg.evaluate(m));
		try {
			return (int)function.invoke(null, argEvaluations.toArray());
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	@Override
	public Set<TableAlias> getIncludedTables() {
		Set<TableAlias> result = new HashSet<TableAlias>(); 
		for (Expression arg : args)
			result.addAll(arg.getIncludedTables());
		return result;
	}

	public Set<TableField> getIncludedFields() {
		Set<TableField> result = new HashSet<TableField>(); 
		for (Expression arg : args)
			result.addAll(arg.getIncludedFields());
		return result;
	}

	public Set<DatalogVariable> getIncludedDatalogVariables() {
		Set<DatalogVariable> result = new HashSet<DatalogVariable>(); 
		for (Expression arg : args)
			result.addAll(arg.getIncludedDatalogVariables());
		return result;
	}

	@Override
	public Class getType(Database database) {
		if (name.equalsIgnoreCase("Count") || name.equalsIgnoreCase("Sum")) return Integer.class;
		return function.getReturnType();
	}

	@Override
	public Class getType(Metadata metadata) {
		if (name.equalsIgnoreCase("Count") || name.equalsIgnoreCase("Sum")) return Integer.class;
		return function.getReturnType();
	}

	@Override
	public boolean isEquality() {
		return false;
	}

	@Override
	public Expression substitute(Map<? extends Expression, ? extends Expression> m) {
		List<Expression> newArgs = new ArrayList<Expression>();
		for (Expression arg: args)
			newArgs.add(arg.substitute(m));
		return new UserDefinedFunction(name, newArgs);	
	}

	public boolean isAggregateFunction()
	{
		return name.equalsIgnoreCase("COUNT")
		| name.equalsIgnoreCase("SUM") | name.equalsIgnoreCase("FSUM")  | name.equalsIgnoreCase("MIN") | name.equalsIgnoreCase("MAX");
	}
	
	public String getName()
	{
		return name;
	}
	
	public Expression getLHS()
	{
		return args.get(0);
	}
	
	public String toString()
	{
		StringBuffer s = new StringBuffer();
		s.append(name+"(");
		for (int i=0; i<args.size();i++)
			s.append(args.get(i).toString()+", ");
		s.delete(s.length()-2, s.length()-1);
		s.append(")");
		return s.toString();
	}
	@Override
	public int hashCode() {
		int result = name.hashCode();
		for (Expression arg : args)
			result = 31 * result + arg.hashCode();
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (getClass() != obj.getClass())
			return false;
		final UserDefinedFunction other = (UserDefinedFunction) obj;
		if (args == null) {
			if (other.args != null)
				return false;
		} else if (!args.equals(other.args))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		int n = args.size();
		if (n != other.args.size()) return false;
		for (int i=0; i<n; i++)
			if (!args.get(i).equals(other.args.get(i))) return false;
		if (!name.equals(other.name)) return false;
		return true;
	}

}
