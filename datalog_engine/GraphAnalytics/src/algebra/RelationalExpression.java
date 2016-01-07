package algebra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import evaluation.TableAlias;
import evaluation.TableField;
import parser.TableFieldVariable;
import parser.DatalogVariable;
import parser.Expression;
import parser.IntegerConst;
import parser.Operation;
import parser.StringConst;
import parser.Term;
import utils.AggregationFunctionType;

public class RelationalExpression {

	Rule rule;
	List<Expression> conditions = new ArrayList<Expression>();
	Map<String, List<TableField>> variableBindings = new Hashtable<String, List<TableField>>();
	Map<Predicate, TableAlias> subgoalAliases;
	List<Expression> outputFields = new ArrayList<Expression>();
	int[] keyFields;
	List<TableAlias> participatingTableAliases;
	String outputTableName;
	boolean isAggregate;
	RelationalType relationalType;
	AggregationFunctionType aggregationFunctionType;
	boolean isRecursive;
	boolean isSourceNodeVariableUnncessary;

	public RelationalExpression(Rule rule)
	{
		this.rule = rule;
		this.keyFields = rule.getHead().getKeyFields();
		this.outputTableName = rule.getHead().name;
		this.isAggregate = rule.isAggregate();
		this.relationalType = rule.getRelationalType();
		this.isRecursive = !rule.getRecursivePredicates().isEmpty();
		this.isSourceNodeVariableUnncessary = rule.isSourceNodeVariableUnncessary();
		setSubgoalAliases();
		setParticipatingAliases();
		setVariableBindings();
		setJoinConditions();
		setConstantTermConditions();
		setConditionSubgoalsConditions();
		setOutputFields();
	}
	
	public RelationalExpression(String outputTableName, Set<Expression> conditions)
	{
		this.outputTableName = outputTableName;
		participatingTableAliases = new ArrayList<TableAlias>();
		for (Expression e : conditions)
			for (TableAlias tableAlias : e.getIncludedTables())
				if (!participatingTableAliases.contains(tableAlias)) participatingTableAliases.add(tableAlias);
		this.conditions.addAll(conditions);
	}
		
	public String toString()
	{
		StringBuffer s = new StringBuffer("Expression\n");
		s.append(outputTableName+"\n");
		s.append("Base Tables\n");
		s.append(Arrays.toString(participatingTableAliases.toArray()));
		s.append("\n");
		s.append("Conditions\n");
		s.append(Arrays.toString(conditions.toArray()));
		s.append("\n");
		s.append("Output Fields\n");
		s.append(Arrays.toString(outputFields.toArray()));
		s.append("\n");
		return s.toString();
		
	}
	
	
	public List<Expression> getConditions()
	{
		return conditions;
	}
	
	public Collection<TableAlias> getParticipatingTableAliases()
	{
		return participatingTableAliases;
	}
	
	public List<Expression> getOutputFields()
	{
		return outputFields;
	}
	
	public String getOutputTableName()
	{
		return outputTableName;
	}
	
	public int[] getKeyFields()
	{
		return keyFields;
	}
			
	private void setSubgoalAliases()
	{
		int sequenceNumber=0;
		subgoalAliases = new Hashtable<Predicate, TableAlias>();
		for (Predicate subgoal: rule.getLitertalSubgoals())
			subgoalAliases.put(subgoal, new TableAlias(subgoal.getName(), sequenceNumber++));
	}
	
	public boolean isAggregate()
	{
		return isAggregate;
	}
	
	public boolean isRecursive()
	{
		return isRecursive;
	}
	
	public boolean isSourceNodeVariableUnncessary()
	{
		return isSourceNodeVariableUnncessary;
	}
		
	public RelationalType getRelationalType()
	{
		return relationalType;
	}

	private void setParticipatingAliases()
	{
		participatingTableAliases = new ArrayList<TableAlias>();
		for (Predicate subgoal : rule.getLitertalSubgoals())
			participatingTableAliases.add(subgoalAliases.get(subgoal));
	}
	
	public void setOutputFields(List<Expression> outputFields)
	{
		this.outputFields = outputFields;
	}

	private void setVariableBindings()
	{
		for (Predicate<Expression> subgoal: rule.getLitertalSubgoals())
		{
			TableAlias tableAliasOfThisVariable = subgoalAliases.get(subgoal);
			int fieldNumber = 0;
			for (Expression t: subgoal.getArgs())
			{
				if (t instanceof DatalogVariable)
				{
					TableField fieldOfThisVariable = new TableField(tableAliasOfThisVariable, fieldNumber);
					List<TableField> bindings = variableBindings.get(t.toString());
					if (bindings==null) bindings = new ArrayList<TableField>();
					bindings.add(fieldOfThisVariable);
					variableBindings.put(t.toString(), bindings);
				}
				fieldNumber++;
			}
		}
	}
	
	private void setJoinConditions()
	{
		for (String v : variableBindings.keySet())
		{
			List<TableField> bindings = variableBindings.get(v);
			for (int i=0; i<bindings.size()-1; i++)
				for (int j=i+1; j<bindings.size(); j++)
				{
					TableFieldVariable lhs = new TableFieldVariable(bindings.get(i));
					TableFieldVariable rhs = new TableFieldVariable(bindings.get(j));
					Operation join = new Operation("==",lhs, rhs);
					conditions.add(join);
				}
		}
			
	}

	private void setConstantTermConditions()
	{
		for (Predicate<Expression> subgoal: rule.getLitertalSubgoals())
		{
			TableAlias tableAliasOfThisVariable = subgoalAliases.get(subgoal);
			int fieldNumber = 0;
			for (Expression t: subgoal.getArgs())
			{
				if (t instanceof StringConst || t instanceof IntegerConst)
				{
					TableField fieldOfThisConst = new TableField(tableAliasOfThisVariable, fieldNumber);
					TableFieldVariable lhs = new TableFieldVariable(fieldOfThisConst);
					Expression rhs = t;
					Operation condition = new Operation("==",lhs, rhs);
					conditions.add(condition);
				}
				fieldNumber++;
			}
		}
	}
	
	private void setConditionSubgoalsConditions()
	{
		datalogExpressionsToDatabaseExpressions(rule.getConditionSubgoals(), conditions);
	}

	private void setOutputFields()
	{
		datalogExpressionsToDatabaseExpressions(rule.getHead().getArgs(), outputFields);
	}
	
	private void datalogExpressionsToDatabaseExpressions(List<Expression> datalogExpressions, List<Expression> databaseExpressions)
	{
		Map<DatalogVariable, TableFieldVariable> oneToOneBinding = new HashMap<DatalogVariable, TableFieldVariable>();
		for (String v : variableBindings.keySet())
			oneToOneBinding.put(new DatalogVariable(v), new TableFieldVariable(variableBindings.get(v).get(0)));
		for (Expression e : datalogExpressions)
		{
			Expression databaseFieldExpression = e.substitute(oneToOneBinding);
			databaseExpressions.add(databaseFieldExpression);
		}		
	}
	/*public String toSQL()
	{
		StringBuffer sql = new StringBuffer();
		//sql.append("DROP TABLE "+getOutputDbName()+";");
		sql.append("CREATE TABLE "+getOutputTableName()+ "(");
		int i=0;
		for (Expression outputField : outputFields)
			sql.append("F"+(i++)+" "+getSQLType(outputField.getType())+", ");
		sql.append("PRIMARY KEY (");
		for (int key : keyFields)
			sql.append("F"+key+", ");
		sql.delete(sql.length()-2, sql.length()-1);
		sql.append(")); ");
		sql.append("INSERT INTO "+getOutputTableName()+" SELECT ");
		for (Expression outputField : outputFields)
			sql.append(outputField + ", ");
		sql.delete(sql.length()-2, sql.length()-1);
		sql.append(" FROM ");
		String connector = ",";
		for (TableAlias tableAlias : getParticipatingTableAliases()) {
			sql.append(tableAlias.tableName + " AS "+tableAlias+connector+" ");
		}
		sql.delete(sql.length()-2, sql.length()-1);
		sql.append(" WHERE ");
		for (Expression e : getConditions()) {
			// GMN Added to convert == to =
			String estring = e.toString();
			estring = estring.replaceAll("==", "=");
			estring = estring.replaceAll("!=", "<>");
			sql.append(estring+ " AND ");
		}
		
		if (getConditions().size()==0) //Delete WHERE
			sql.delete(sql.length()-7, sql.length()-1);
		else  //Delete AND at the end
			sql.delete(sql.length()-5, sql.length()-1);
	
		if (outputFields.get(outputFields.size()-1).isAggregateFunction())
		{
			sql.append(" GROUP BY  ");
			for (Expression groupByField : outputFields)
			{
				if (!groupByField.isAggregateFunction())
					sql.append(groupByField+", ");
			}
			sql.delete(sql.length()-2, sql.length()-1);
		}
		sql.append(";");
		
		return sql.toString();

	}
	
	private String getSQLType(Class c)
	{
		if (c==Integer.class)
			return "INT";
		else return "VARCHAR(256)";
	}*/
	
}
