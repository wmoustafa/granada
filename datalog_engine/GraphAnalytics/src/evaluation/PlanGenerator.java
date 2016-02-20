package evaluation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import algebra.RelationalExpression;
import algebra.RelationalType;
import parser.Expression;
import parser.TableFieldVariable;
import parser.UserDefinedFunction;
import query.filter.Filter;
import query.filter.GroupByFilter;
import query.filter.HashJoinFilter;
import query.filter.ProjectFilter;
import query.filter.ScanFilter;
import schema.Metadata;
import utils.AggregationFunctionType;

public class PlanGenerator {

	RelationalExpression relationalExpression;
	PossibleOrder evaluationOrder;
	Map<TableAlias, Set<Expression>> expressionByTableAlias;
	List<Expression> expressions;
	List<Expression> outputFields;
	int[] keyFields;
	String outputTableName;
	//boolean isAggregate;
	RelationalType relationalType;
	AggregationFunctionType aggregationFunctionType;
	boolean isRecursive;
	boolean isSourceNodeVariableUnncessary;
	
	static Map<String, Filter> pipelineableFilters = new Hashtable<String, Filter>();

	public PlanGenerator(PossibleOrder evaluationOrder, RelationalExpression relationalExpression)
	{
		this.relationalExpression = relationalExpression;
		this.evaluationOrder = evaluationOrder;
		this.expressions = relationalExpression.getConditions();
		this.outputFields = relationalExpression.getOutputFields();
		this.outputTableName = relationalExpression.getOutputTableName();
		this.keyFields = relationalExpression.getKeyFields();
		//this.isAggregate = relationalExpression.isAggregate();
		this.relationalType = relationalExpression.getRelationalType();
		this.isRecursive = relationalExpression.isRecursive();
		this.isSourceNodeVariableUnncessary = relationalExpression.isSourceNodeVariableUnncessary();
	}

	private void createExpressionByTableAlias()
	{
		expressionByTableAlias = new Hashtable<TableAlias, Set<Expression>>();
		for (TableAlias tableAlias : evaluationOrder.getOrdering()) expressionByTableAlias.put(tableAlias, new HashSet<Expression>());
		Set<TableAlias> tableAliasesSeenSoFar = new HashSet<TableAlias>();
		for (TableAlias tableAlias : evaluationOrder.getOrdering())
		{
			tableAliasesSeenSoFar.add(tableAlias);
			for (Expression e : expressions)
				if (e.getIncludedTables().contains(tableAlias) && tableAliasesSeenSoFar.containsAll(e.getIncludedTables()))
				{
					Set<Expression> exprs = expressionByTableAlias.get(tableAlias);
					exprs.add(e);
					expressionByTableAlias.put(tableAlias, exprs);
				}
		}
	}

	public Filter generatePlan(Metadata metadata)
	{
			createExpressionByTableAlias();
			//Cursor inputCursor = new Cursor();
			Filter firstFilter = null, previousFilter = null;
			for (TableAlias tableAlias : evaluationOrder.getOrdering())
			{
				if (firstFilter == null)
				{
					Set<Expression> filterConditions = expressionByTableAlias.get(tableAlias);
					firstFilter = new ScanFilter(tableAlias, filterConditions.toArray(new Expression[filterConditions.size()]));
					//firstFilter.setDatabases(inputDatabase, outputDatabase);
					//firstFilter.setInputCursor(inputCursor);
					previousFilter = firstFilter;
				}
				else
				{
					HashJoinFilter hashJoin = createHashJoinFilter(metadata,tableAlias);
					previousFilter.setNextFilter(hashJoin);
					previousFilter = hashJoin;
				}
			}
			Expression lastArgument = outputFields.get(outputFields.size() - 1);
			AggregationFunctionType aggregationFunctionType;
			if (lastArgument.isAggregateFunction())
			{
				aggregationFunctionType = AggregationFunctionType.valueOf(((UserDefinedFunction)lastArgument).getName());
				List<Expression> groupingKeys= new ArrayList<Expression>();
				groupingKeys.addAll(outputFields);
				groupingKeys.remove(lastArgument);
				Filter groupByFilter = new GroupByFilter(outputTableName, keyFields, groupingKeys.toArray(new Expression[groupingKeys.size()]), aggregationFunctionType, lastArgument.getLHS(), isRecursive, isSourceNodeVariableUnncessary, relationalType, metadata);
				previousFilter.setNextFilter(groupByFilter);
			}
			else
			{
				aggregationFunctionType = AggregationFunctionType.NONE;
				Filter project = new ProjectFilter(outputTableName, keyFields, outputFields.toArray(new Expression[outputFields.size()]), aggregationFunctionType, isRecursive, isSourceNodeVariableUnncessary, relationalType, metadata);
				if (firstFilter == null) firstFilter = project; else previousFilter.setNextFilter(project);
			}
			return firstFilter;
		}
	


	private HashJoinFilter createHashJoinFilter(Metadata metadata,TableAlias rhsTableAlias)
	{
		int[] rhsTableKeyFields = metadata.getKeyFields(rhsTableAlias.tableName);
		List<Integer> rhsTableKeyFieldsList = new ArrayList<Integer>();
		for (int rhsTableKeyField : rhsTableKeyFields) rhsTableKeyFieldsList.add(rhsTableKeyField);
		List<Integer> rhsTableKeyFieldsListSorted = new ArrayList<>(rhsTableKeyFieldsList);
		Collections.sort(rhsTableKeyFieldsListSorted);
		Set<Expression> allConditions = expressionByTableAlias.get(rhsTableAlias);
		Set<Expression> joinConditions = new HashSet<>();
		boolean buildRightHashTable = true;
		List<Expression> lhs = new ArrayList<Expression>();
		List<Expression> rhs = new ArrayList<Expression>();
		List<Integer> rhsFieldNumbers = new ArrayList<Integer>();
		////System.out.println("******************************");
		////System.out.println("allConditions1: "+allConditions);
		for (Expression e : allConditions)
		{
			if (e.isEquality())
			{
				Expression expressionLhs = e.getLHS();
				Expression expressionRhs = e.getRHS();
				////System.out.println("e: "+e);
				////System.out.println("expressionLHS: "+expressionLhs);
				////System.out.println("expressionRHS: "+expressionRhs);
				if (expressionLhs instanceof TableFieldVariable)
				{
					TableField expressionLhsField = ((TableFieldVariable)expressionLhs).getField();
					TableAlias expressionLhsTableAlias = expressionLhsField.alias;
					if (expressionLhsTableAlias.equals(rhsTableAlias)) {
						int expressionLhsFieldNumber = expressionLhsField.fieldNumber;
						rhs.add(expressionLhs);
						lhs.add(expressionRhs);
						rhsFieldNumbers.add(expressionLhsFieldNumber);
						joinConditions.add(e);
						////System.out.println("rhs: "+rhs);
						////System.out.println("lhs: "+lhs);
					}
				}
				if (expressionRhs instanceof TableFieldVariable)
				{
					TableField expressionRhsField = ((TableFieldVariable)expressionRhs).getField();
					TableAlias expressionRhsTableAlias = expressionRhsField.alias;
					if (expressionRhsTableAlias.equals(rhsTableAlias)) {
						int expressionRhsFieldNumber = expressionRhsField.fieldNumber;
						lhs.add(expressionLhs);
						rhs.add(expressionRhs);
						rhsFieldNumbers.add(expressionRhsFieldNumber);
						joinConditions.add(e);
						////System.out.println("rhs: "+rhs);
						////System.out.println("lhs: "+lhs);
					}
				}
			}
		}
		
		allConditions.removeAll(joinConditions);
		////System.out.println("allConditions2: "+allConditions);
		////System.out.println("rhs: "+rhs);
		////System.out.println("lhs: "+lhs);

		List<Integer> rhsFieldNumbersSorted = new ArrayList<>(rhsFieldNumbers);
		Collections.sort(rhsFieldNumbersSorted);
		if (rhsFieldNumbersSorted.equals(rhsTableKeyFieldsListSorted))
		{
			buildRightHashTable = false;
			List<Expression> newLhs = new ArrayList<Expression>();
			List<Expression> newRhs = new ArrayList<Expression>();
			for (Integer keyField : rhsTableKeyFieldsList)
			{
				int index = rhsFieldNumbers.indexOf(keyField);
				newLhs.add(lhs.get(index));
				newRhs.add(rhs.get(index));
			}
			lhs = newLhs;
			rhs = newRhs;
		}
		////System.out.println("build: "+buildRightHashTable);
		////System.out.println("rhs: "+rhs);
		////System.out.println("lhs: "+lhs);
		
		
		//if (rhsFieldNumbers.equals(rhsTableKeyFieldsList)) buildRightHashTable = true;
		return new HashJoinFilter(rhsTableAlias, lhs.toArray(new Expression[lhs.size()]), rhs.toArray(new Expression[rhs.size()]), allConditions.toArray(new Expression[allConditions.size()]), buildRightHashTable);
	}


}
