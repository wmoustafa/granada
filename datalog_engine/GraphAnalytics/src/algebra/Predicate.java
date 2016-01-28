package algebra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import parser.DatalogVariable;
import parser.Expression;

public class Predicate<T extends Expression> {
	
	String name;
	List<T> args;
	int[] keyFields;
	
	public Predicate(String name)
	{
		this.name = name;
		args = new ArrayList<T>();
	}
	
	public Predicate(Predicate p)
	{
		name = new String(p.name);
		args = new ArrayList<T>();
		for (Object arg : p.getArgs())
			args.add((T)arg);
		keyFields = p.keyFields;
	}
	
	
	public String getName()
	{
		return name;
	}
	
	public List<T> getArgs()
	{
		return args;
	}
	
	public void addArg(T arg)
	{
		args.add(arg);
	}
	
	public void removeLastArg()
	{
		args.remove(args.size() - 1);
	}

	public void setKeyFields(List<Integer> keyFields)
	{
		this.keyFields = new int[keyFields.size()];
		int i=0; for(Integer keyField : keyFields) this.keyFields[i++]=keyField;
	}
	
	public void setKeyFields(int[] keyFields)
	{
		this.keyFields = keyFields;
	}

	public int[] getKeyFields()
	{
		return keyFields;
	}

	public void rename(String newName)
	{
		name = newName;
	}
	
	public Predicate<Expression> substitute(Map<? extends Expression, ? extends Expression> m)
	{
		Predicate<Expression> p_prime = new Predicate<Expression>(this);
		p_prime.args.clear();
		for (T arg : getArgs()) p_prime.addArg(arg.substitute(m));
		return p_prime;

	}
	
	public String toString()
	{
		StringBuffer s = new StringBuffer();
		s.append(name+"(");
		for (T arg : args)
			if (arg.toString().startsWith("DontCare")) s.append("_, ");
			else s.append(arg+", ");
		if (!args.isEmpty()) s.delete(s.length()-2, s.length());
		s.append(")");
		return s.toString();
	}
	
	public Set<DatalogVariable> getIncludedDatalogVaraibles()
	{
		Set<DatalogVariable> includedDatalogVariables = new HashSet<DatalogVariable>();
		for (T arg : getArgs())
			includedDatalogVariables.addAll(arg.getIncludedDatalogVariables());
		return includedDatalogVariables;
	}
}
