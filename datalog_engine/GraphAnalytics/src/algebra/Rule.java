package algebra;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import org.apache.commons.collections.MultiMap;
import org.python.antlr.runtime.RuleReturnScope;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import evaluation.Optimizer;
import evaluation.PlanGenerator;
import evaluation.PossibleOrder;
import evaluation.PossibleOrderSpace;
import parser.DatalogVariable;
import parser.Expression;
import parser.IntegerConst;
import parser.Operation;
import parser.UserDefinedFunction;
import query.filter.Filter;
import schema.Database;
import schema.Metadata;
import utils.Log;

public class Rule {
	

	public static int m=0;
	Predicate<Expression> head;
	List<Predicate<Expression>> literalSubgoals;
	List<Expression> conditionSubgoals;
	Set<Predicate> recursivePredicates;
	Multimap<DatalogVariable,DatalogVariable> ruleGraph;
	Map<String,DatalogVariable> weightVariableNames;
	Map<DatalogVariable,DatalogVariable> substitutionMap;
	Multimap<DatalogVariable,DatalogVariable> dag;
	private boolean isAggregate;
	private RelationalType relationalType = RelationalType.NOT_RELATIONAL;
	private boolean isModularlyStratified;
	private Filter evaluationPlan;
	private boolean SourceNodeVariableUnncessaryUnnecessary = false;
	
	public Rule()
	{
		literalSubgoals = new ArrayList<Predicate<Expression>>();
		conditionSubgoals = new ArrayList<Expression>();
	}
	
	public Rule(Rule r)
	{
		head = new Predicate(r.head);
		literalSubgoals = new ArrayList<Predicate<Expression>>();
		for (Predicate<Expression> subgoal : r.literalSubgoals)
			literalSubgoals.add(new Predicate<Expression>(subgoal));
		conditionSubgoals = new ArrayList<Expression>();
		for (Expression e : r.conditionSubgoals)
			conditionSubgoals.add(e);
		isAggregate = r.isAggregate;
		relationalType = r.relationalType;
		SourceNodeVariableUnncessaryUnnecessary = r.SourceNodeVariableUnncessaryUnnecessary;
		isModularlyStratified = r.isModularlyStratified;
		if (r.recursivePredicates != null) {
			recursivePredicates = new HashSet<>();
			recursivePredicates.addAll(r.recursivePredicates);
		}
	}
	
	public List<Predicate<Expression>> getLitertalSubgoals()
	{
		return literalSubgoals;
	}

	public List<Expression> getConditionSubgoals()
	{
		return conditionSubgoals;
	}
	
	public boolean isAggregate()
	{
		return isAggregate;
	}
	
	public RelationalType getRelationalType()
	{
		return relationalType;
	}

	
	public boolean isModularlyStratified()
	{
		return isModularlyStratified;
	}
	
	public void setHead(Predicate<Expression> head)
	{
		this.head = head;
		for (Expression e : head.getArgs()) if (e.isAggregateFunction()) { isAggregate = true; break; }
	}
	
	public void setAggregate()
	{
		isAggregate = true;
	}
	
	public void setModularlyStratified(boolean isModularlyStratified)
	{
		this.isModularlyStratified = isModularlyStratified;
	}
		
	public void setRecursivePredicates(Set<Predicate> recursivePredicates)
	{
		this.recursivePredicates = recursivePredicates;
	}
	
	public Set<Predicate> getRecursivePredicates()
	{
		return recursivePredicates;
	}

	public void addLiteralSubgoal(Predicate<Expression> subgoal)
	{
		literalSubgoals.add(subgoal);
	}
	
	public void addConditionSubgoal(Expression e)
	{
		conditionSubgoals.add(e);
	}
	

	
	public Predicate getHead()
	{
		return head;
	}
		
	public Expression getLastHeadArg()
	{
		List<Expression> args = head.getArgs();
		return args.get(args.size()-1);
	}
	
	public void setLastHeadArg(Expression e)
	{
		List<Expression> args = head.getArgs();
		args.set(args.size()-1, e);
	}
	
	public void generateEvaluationPlan(Database inputDatabase, Metadata metadata)
	{
		RelationalExpression relExpression = new RelationalExpression(this);
		PossibleOrderSpace orderSpace = new PossibleOrderSpace(relExpression);
		Optimizer optimizer = new Optimizer(inputDatabase, orderSpace);
		PossibleOrder bestOrder = optimizer.getBestPlan();
		PlanGenerator planGenerator = new PlanGenerator(bestOrder, relExpression);
		evaluationPlan = planGenerator.generatePlan(metadata);
	}
	
	public Filter getEvaluationPlan()
	{
		return evaluationPlan;
	}

	
	public String toString()
	{
		StringBuffer s = new StringBuffer();
		s.append(head +":-"+Arrays.toString(literalSubgoals.toArray())+", "+Arrays.toString(conditionSubgoals.toArray()));
		return s.toString();
	}
	
	private void generateRuleGraph()
	{
		ruleGraph = HashMultimap.create();
		weightVariableNames = new HashMap<>();
		substitutionMap = new HashMap<>();
		for (Predicate<Expression> p : literalSubgoals)
			if (p.getName().equalsIgnoreCase("edges"))
			{
				List<Expression> edgeArgs = p.getArgs();
				Expression startVertexExpression = edgeArgs.get(0);
				Expression endVertexExpression = edgeArgs.get(1);
				Expression weightVarExpression = edgeArgs.get(2);
				if (startVertexExpression instanceof DatalogVariable && endVertexExpression instanceof DatalogVariable)
				{
					DatalogVariable startVertex = (DatalogVariable)startVertexExpression;
					DatalogVariable endVertex = (DatalogVariable)endVertexExpression;
					ruleGraph.put(startVertex, endVertex);
					weightVariableNames.put(startVertex + "->" + endVertex, (DatalogVariable)weightVarExpression);
				}
			}
	}
	
	private int getEccentricity(DatalogVariable v)
	{
		int diameter = 0;
		Queue<DatalogVariable> queue = new LinkedList<DatalogVariable>();
		Map<DatalogVariable,Integer> distances = new HashMap<DatalogVariable,Integer>();
		queue.add(v);
		distances.put(v, 0);
		while (!queue.isEmpty())
		{
			DatalogVariable w = queue.remove();
			for (DatalogVariable n : ruleGraph.get(w))
				if (!distances.containsKey(n)) { queue.add(n); diameter = distances.get(w) + 1; distances.put(n, diameter); }
		}
		return diameter;
	}
	
	public Rule rewriteOld(DatalogVariable startFrom, int nHops)
	{
		Rule rewrite = new Rule();
		Predicate<Expression> rewriteHead = new Predicate<Expression>(this.head.name + "_" + startFrom.toString() + nHops);
		Set<Predicate<Expression>> rewriteLiteralSubgoals = new HashSet<Predicate<Expression>>();
		Set<Expression> rewriteConditionSubgoals = new HashSet<Expression>();
		Set<Expression> projectionFields = new LinkedHashSet<Expression>();

		Set<DatalogVariable> neighborhood = new HashSet<DatalogVariable>();
		neighborhood.add(startFrom);

		if (nHops > 0)
		{
			for (DatalogVariable n : ruleGraph.get(startFrom))
				neighborhood.add(n);
			for (DatalogVariable n : neighborhood)
			{
				Predicate<Expression> lowerLevelRuleHead = rewriteOld(n, nHops - 1).getHead();
				rewrite.addLiteralSubgoal(lowerLevelRuleHead);
				for (Expression arg : lowerLevelRuleHead.getArgs()) projectionFields.addAll(arg.getIncludedDatalogVariables());
			}
		}
		else
		{			
			projectionFields.add(startFrom);
	
			for (Predicate<Expression> p : literalSubgoals)
			{
				Set<DatalogVariable> predicateIncludedDatalogVariables = new HashSet<DatalogVariable>();
				for (Expression arg : p.getArgs()) predicateIncludedDatalogVariables.addAll(arg.getIncludedDatalogVariables());
				if (!Collections.disjoint(predicateIncludedDatalogVariables, neighborhood))
				{
					rewriteLiteralSubgoals.add(p);
					projectionFields.addAll(predicateIncludedDatalogVariables);
				}
			}
		}

		for (Expression e : conditionSubgoals)
		{
			Set<DatalogVariable> expressionIncludedDatalogVariables = e.getIncludedDatalogVariables();
			if (projectionFields.containsAll(expressionIncludedDatalogVariables))
			{
				rewriteConditionSubgoals.add(e);
				//projectionFields.addAll(expressionIncludedDatalogVariables);
			}
		}
		
		for (Expression e : projectionFields) rewriteHead.addArg(e);
		
		rewrite.setHead(rewriteHead);
		for (Predicate p : rewriteLiteralSubgoals) rewrite.addLiteralSubgoal(p);
		for (Expression e : rewriteConditionSubgoals) rewrite.addConditionSubgoal(e);
		rewriteHead.setKeyFields(Arrays.asList(0));
		//rewrite.isRelational = true;
		
		return rewrite;		
	}
	
	public List<Rule> rewriteOld()
	{
		generateRuleGraph();
		Set<DatalogVariable> rewriteVariables = new HashSet<DatalogVariable>();
		rewriteVariables.addAll(ruleGraph.keySet());
		rewriteVariables.addAll(ruleGraph.values());
		
		List<Rule> rewrites = new ArrayList<Rule>();
		
		if (rewriteVariables.isEmpty()) rewrites.add(this);
		else
		{
			for (DatalogVariable v : rewriteVariables)
			{
				int eccentricity = getEccentricity(v);
				for (int i = 0; i <= eccentricity; i++)
				{
					Rule nHopVersionStartingFromV = rewriteOld(v, i);
					rewrites.add(nHopVersionStartingFromV);
				}
			}
			
			DatalogVariable primaryKeyVariable = (DatalogVariable)getHead().getArgs().get(getHead().keyFields[0]);
			Rule sinkRule = rewriteOld(primaryKeyVariable, getEccentricity(primaryKeyVariable));
			Rule finalRule = new Rule();
			finalRule.addLiteralSubgoal(sinkRule.getHead());
			finalRule.setHead(new Predicate(getHead()));		
			rewrites.add(finalRule);
		}
		return rewrites;
	}
	
	public void localizePrimaryKey()
	{
		//Rule r = new Rule(this);
		DatalogVariable primaryKeyVariable = (DatalogVariable)getHead().getArgs().get(getHead().keyFields[0]);
		Predicate<Expression> localizationPredicate = new Predicate<Expression>("vertices");
		localizationPredicate.addArg(primaryKeyVariable);
		localizationPredicate.addArg(new DatalogVariable("DontCare"));
		localizationPredicate.setKeyFields(new int[]{0});
		//Expression locationRestrictionCondition = new Operation("==", primaryKeyVariable, new IntegerConst(location));
		literalSubgoals.add(localizationPredicate);
	}
		
	private void createDagFromRuleGraph(DatalogVariable sink)
	{
		Multimap<DatalogVariable, DatalogVariable> undirectedRuleGraph = HashMultimap.create();
		for (Entry<DatalogVariable, DatalogVariable> entry : ruleGraph.entries())
		{
			DatalogVariable startVertex = entry.getKey();
			DatalogVariable endVertex = entry.getValue();
			undirectedRuleGraph.put(startVertex, endVertex);
			undirectedRuleGraph.put(endVertex, startVertex);
		}
		
		Queue<DatalogVariable> bfsQueue = new LinkedList<DatalogVariable>();
		Map<DatalogVariable,Integer> levels = new HashMap<DatalogVariable,Integer>();
		bfsQueue.add(sink);
		levels.put(sink, 0);
		while (!bfsQueue.isEmpty())
		{
			DatalogVariable head = bfsQueue.poll();
			for (DatalogVariable v : undirectedRuleGraph.get(head))
				if (!levels.containsKey(v))
				{
					bfsQueue.add(v);
					levels.put(v, levels.get(head) + 1);
				}
		}
		dag = HashMultimap.create();
		for (Entry<DatalogVariable,DatalogVariable> edge : undirectedRuleGraph.entries())
		{
			DatalogVariable v1 = edge.getKey();
			DatalogVariable v2 = edge.getValue();
			if (levels.get(v1) > levels.get(v2)) dag.put(v1, v2);
			else if (levels.get(v1) < levels.get(v2)) dag.put(v2, v1);
			else
			{
				if (v1.toString().compareTo(v2.toString()) > 0) dag.put(v1, v2);
				else dag.put(v2, v1);
			}
		}
	}
	
	public List<DatalogVariable> getTopologicalOrder(DatalogVariable sink)
	{
		List<DatalogVariable> topologicalOrder = new ArrayList<>();
		Multimap<DatalogVariable, DatalogVariable> dagCopy = HashMultimap.create();
		dagCopy.putAll(dag);
		Set<DatalogVariable> sourceNodes = new HashSet<DatalogVariable>();
		sourceNodes.addAll(dagCopy.keySet());
		sourceNodes.removeAll(dagCopy.values());			
		while (!sourceNodes.isEmpty())
		{
			DatalogVariable sourceNode = sourceNodes.iterator().next();
			sourceNodes.remove(sourceNode);
			topologicalOrder.add(sourceNode);
			dagCopy.removeAll(sourceNode);
			sourceNodes.addAll(dagCopy.keySet());
			sourceNodes.removeAll(dagCopy.values());			
		}
		topologicalOrder.add(sink);
		return topologicalOrder;
	}
	
	public Rule rewrite(DatalogVariable v, Map<DatalogVariable, Rule> previousRules, List<DatalogVariable> topologicalOrder)
	{
		/*DatalogVariable predecessor = null;
		for (int i = topologicalOrder.indexOf(v) - 1; i >= 0; i--)
		{
			DatalogVariable w = topologicalOrder.get(i);
			if (dag.containsEntry(w, v)) { predecessor = w; break; }
		}*/
		Set<DatalogVariable> predecessors = new HashSet<DatalogVariable>();
		Set<DatalogVariable> successors = new HashSet<DatalogVariable>();
		for (Entry<DatalogVariable,DatalogVariable> entry : dag.entries())
		{
			DatalogVariable v1 = entry.getKey();
			DatalogVariable v2 = entry.getValue();
			if (v1.equals(v)) successors.add(v2);
			if (v2.equals(v)) predecessors.add(v1);
		}
		
		Set<DatalogVariable> prohibitedDatalogVariables = new HashSet<DatalogVariable>();
		for (int i = topologicalOrder.indexOf(v) + 1; i < topologicalOrder.size(); i++)
			prohibitedDatalogVariables.add(topologicalOrder.get(i));
		Rule rewrite = new Rule();
		Predicate<Expression> rewriteHead = new Predicate<Expression>(this.head.name + "_" + v.toString() + toString().hashCode());
		Set<Predicate<Expression>> rewriteLiteralSubgoals = new HashSet<Predicate<Expression>>();
		Set<Expression> rewriteConditionSubgoals = new HashSet<Expression>();
		Set<Expression> projectionFields = new LinkedHashSet<Expression>();
		projectionFields.add(v);
		
		for (Predicate p : getLitertalSubgoals())
		{
			Set<DatalogVariable> predicateIncludedDatalogVariables = p.getIncludedDatalogVaraibles();
			if (predicateIncludedDatalogVariables.contains(v) && Collections.disjoint(predicateIncludedDatalogVariables, prohibitedDatalogVariables))
			{
				//if (p.getName().equals("edges")) p.name = "incomingNeighbors";
					rewriteLiteralSubgoals.add(p);
					projectionFields.addAll(predicateIncludedDatalogVariables);
			}
		}
		
		for (DatalogVariable predecessor : predecessors)
		{
			Predicate predecessorRuleHead = previousRules.get(predecessor).getHead();
			rewriteLiteralSubgoals.add(new Predicate(predecessorRuleHead));
			projectionFields.addAll(predecessorRuleHead.getIncludedDatalogVaraibles());
		}
		
		for (Expression e : getConditionSubgoals())
		{
			Set<DatalogVariable> conditionIncludedDatalogVariables = e.getIncludedDatalogVariables();
			if (projectionFields.containsAll(conditionIncludedDatalogVariables) && Collections.disjoint(conditionIncludedDatalogVariables, prohibitedDatalogVariables))
			{
					rewriteConditionSubgoals.add(e);
					projectionFields.addAll(conditionIncludedDatalogVariables);
			}
			
		}
		for (Expression e : projectionFields) rewriteHead.addArg(e);
		
		rewrite.setHead(rewriteHead);
		for (Predicate p : rewriteLiteralSubgoals) rewrite.addLiteralSubgoal(p);
		for (Expression e : rewriteConditionSubgoals) rewrite.addConditionSubgoal(e);
		rewriteHead.setKeyFields(Arrays.asList(0));
		
		boolean isOutgoingRelational = false;
		boolean isIncomingRelational = false;
		
		for (DatalogVariable successor : successors)
			if (ruleGraph.containsEntry(v, successor))
				isOutgoingRelational = true;
				
		for (DatalogVariable successor : successors)
			if (ruleGraph.containsEntry(successor, v))
				isIncomingRelational = true;

		if (isOutgoingRelational && isIncomingRelational)
			rewrite.relationalType = RelationalType.TWO_WAY_RELATIONAL;
		else if (isOutgoingRelational)
			rewrite.relationalType = RelationalType.OUTGOING_RELATIONAL;
		else if (isIncomingRelational)
			rewrite.relationalType = RelationalType.INCOMING_RELATIONAL;
		
		rewrite.localizePrimaryKey();
		
		return rewrite;

	}
	
	public List<Rule> rewriteEdgeBased(DatalogVariable v, Multimap<DatalogVariable,Rule> rewriteRuleDependency, List<DatalogVariable> topologicalOrder)
	{
		/*DatalogVariable predecessor = null;
		for (int i = topologicalOrder.indexOf(v) - 1; i >= 0; i--)
		{
			DatalogVariable w = topologicalOrder.get(i);
			if (dag.containsEntry(w, v)) { predecessor = w; break; }
		}*/
		List<Rule> rewrites = new ArrayList<Rule>();
		
		Set<DatalogVariable> predecessors = new HashSet<DatalogVariable>();
		Set<DatalogVariable> successors = new HashSet<DatalogVariable>();
		for (Entry<DatalogVariable,DatalogVariable> entry : dag.entries())
		{
			DatalogVariable v1 = entry.getKey();
			DatalogVariable v2 = entry.getValue();
			if (v1.equals(v)) successors.add(v2);
			if (v2.equals(v)) predecessors.add(v1);
		}
		
		Set<DatalogVariable> prohibitedDatalogVariables = new HashSet<DatalogVariable>();
		for (int i = topologicalOrder.indexOf(v) + 1; i < topologicalOrder.size(); i++)
			prohibitedDatalogVariables.add(topologicalOrder.get(i));
		Rule rewrite = new Rule();
		Predicate<Expression> rewriteHead = new Predicate<Expression>(this.head.name + "_" + v.toString() + toString().hashCode());
		Set<Predicate<Expression>> rewriteLiteralSubgoals = new HashSet<Predicate<Expression>>();
		Set<Expression> rewriteConditionSubgoals = new HashSet<Expression>();
		Set<Expression> projectionFields = new LinkedHashSet<Expression>();
		projectionFields.add(v);
		
		for (Predicate p : getLitertalSubgoals())
		{
			if (p.getName().equals("edges")) continue;
			Set<DatalogVariable> predicateIncludedDatalogVariables = p.getIncludedDatalogVaraibles();
			if (predicateIncludedDatalogVariables.contains(v) && Collections.disjoint(predicateIncludedDatalogVariables, prohibitedDatalogVariables))
			{
					rewriteLiteralSubgoals.add(p);
					projectionFields.addAll(predicateIncludedDatalogVariables);
			}
		}
		
		for (Rule predecessorRule : rewriteRuleDependency.get(v))
		{
			Predicate predecessorRuleHead = predecessorRule.getHead();
//			System.out.println("PredecessorRuleHead:" + predecessorRuleHead);
//			System.out.println("v:" + v);
			Predicate newPredicate = new Predicate(predecessorRuleHead);
			newPredicate.removeLastArg();
			newPredicate.addArg(v);
			rewriteLiteralSubgoals.add(newPredicate);
			projectionFields.addAll(newPredicate.getIncludedDatalogVaraibles());
		}
		
		for (Expression e : getConditionSubgoals())
		{
			Set<DatalogVariable> conditionIncludedDatalogVariables = e.getIncludedDatalogVariables();
			if (projectionFields.containsAll(conditionIncludedDatalogVariables) && Collections.disjoint(conditionIncludedDatalogVariables, prohibitedDatalogVariables))
			{
					rewriteConditionSubgoals.add(e);
					projectionFields.addAll(conditionIncludedDatalogVariables);
			}
			
		}
		for (Expression e : projectionFields) rewriteHead.addArg(e);
//		System.out.println(rewriteHead);
		rewriteHead = rewriteHead.substitute(substitutionMap);
//		System.out.println(rewriteHead);
//		System.out.println("SUBStiutionmap: " + substitutionMap);
		rewrite.setHead(rewriteHead);
		for (Predicate p : rewriteLiteralSubgoals) rewrite.addLiteralSubgoal(p);
		for (Expression e : rewriteConditionSubgoals) rewrite.addConditionSubgoal(e);
		rewriteHead.setKeyFields(Arrays.asList(0));
		
		boolean isOutgoingRelational = false;
		boolean isIncomingRelational = false;
		
		Set<DatalogVariable> outgoingVariables = new HashSet<>();
		Set<DatalogVariable> incomingVariables = new HashSet<>();
		for (DatalogVariable successor : successors)
			if (ruleGraph.containsEntry(v, successor))
			{
				isOutgoingRelational = true;
				outgoingVariables.add(successor);
			}
				
		for (DatalogVariable successor : successors)
			if (ruleGraph.containsEntry(successor, v))
			{
				isIncomingRelational = true;
				incomingVariables.add(successor);
			}

		if (isOutgoingRelational)
		{
			Rule outgoingRewrite = new Rule(rewrite);
			outgoingRewrite.getHead().rename(rewrite.getHead().getName() + "_OUTGOING");
			outgoingRewrite.relationalType = RelationalType.OUTGOING_RELATIONAL;
			Predicate<Expression> edgesPredicate = new Predicate<>("outgoingNeighbors");
			outgoingRewrite.addLiteralSubgoal(edgesPredicate);
			edgesPredicate.addArg(v);
			DatalogVariable randomVariable1 = outgoingRewrite.generateRandomVariableName();
			edgesPredicate.addArg(randomVariable1);
			DatalogVariable randomVariable2 = outgoingRewrite.generateRandomVariableName();
			edgesPredicate.addArg(randomVariable2);
			//There is a hard assumption that the cost variable is the second to last variable
			//The following must always be the second to last line.
			outgoingRewrite.getHead().addArg(randomVariable2);
			//There is a hard assumption that the destination node id is the last in the predicate
			//The following must always be the last line.
			outgoingRewrite.getHead().addArg(randomVariable1);
			outgoingRewrite.getHead().keyFields = new int[]{ outgoingRewrite.getHead().getArgs().size() - 1 };
			
			rewrites.add(outgoingRewrite);
			for (DatalogVariable successor : outgoingVariables) {
				rewriteRuleDependency.put(successor, outgoingRewrite);
				substitutionMap.put(weightVariableNames.get(v + "->" + successor), randomVariable2);
			}
		}
		
		if (isIncomingRelational)
		{
			Rule incomingRewrite = new Rule(rewrite);
			incomingRewrite.getHead().rename(rewrite.getHead().getName() + "_INCOMING");
			incomingRewrite.relationalType = RelationalType.INCOMING_RELATIONAL;
			Predicate<Expression> edgesPredicate = new Predicate<>("incomingNeighbors");
			incomingRewrite.addLiteralSubgoal(edgesPredicate);
			edgesPredicate.addArg(v);
			DatalogVariable randomVariable1 = incomingRewrite.generateRandomVariableName();
			edgesPredicate.addArg(randomVariable1);
			DatalogVariable randomVariable2 = incomingRewrite.generateRandomVariableName();
			edgesPredicate.addArg(randomVariable2);
			//There is a hard assumption that the cost variable is the second to last variable
			//The following must always be the second to last line.
			incomingRewrite.getHead().addArg(randomVariable2);
			//There is a hard assumption that the destination node id is the last in the predicate
			//The following must always be the last line.
			incomingRewrite.getHead().addArg(randomVariable1);
			incomingRewrite.getHead().keyFields = new int[]{ incomingRewrite.getHead().getArgs().size() - 1 };

			rewrites.add(incomingRewrite);
			for (DatalogVariable successor : incomingVariables) {
				rewriteRuleDependency.put(successor, incomingRewrite);
				substitutionMap.put(weightVariableNames.get(successor + "->" + v), randomVariable2);
			}
		}
		
		if (!isIncomingRelational && ! isOutgoingRelational)
			rewrites.add(rewrite);
				
		return rewrites;

	}

	public List<Rule> rewrite(boolean useEagerAggregation)
	{
		generateRuleGraph();
		List<Rule> rewrites = new ArrayList<Rule>();
		if (ruleGraph.isEmpty()) rewrites.add(this);
		else
		{
			DatalogVariable primaryKeyVariable = (DatalogVariable)getHead().getArgs().get(getHead().keyFields[0]);
			createDagFromRuleGraph(primaryKeyVariable);
			List<DatalogVariable> topologicalOrder = getTopologicalOrder(primaryKeyVariable);
			Map<DatalogVariable,Rule> rewritesByVariable = new HashMap<DatalogVariable,Rule>();
			for (Iterator<DatalogVariable> topologicalOrderIterator = topologicalOrder.iterator(); topologicalOrderIterator.hasNext();)
			{
				DatalogVariable v = topologicalOrderIterator.next();
				Rule rewrite = rewrite(v, rewritesByVariable, topologicalOrder);
				if (!(rewrite.getConditionSubgoals().isEmpty() && rewrite.getLitertalSubgoals().isEmpty()))
				{
					rewritesByVariable.put(v, rewrite);
					rewrites.add(rewrite);
				}
				else topologicalOrderIterator.remove();
			}
			
			/*Rule sinkRule = rewritesByVariable.get(primaryKeyVariable);
			Rule finalRule = new Rule();
			finalRule.addLiteralSubgoal(sinkRule.getHead());
			finalRule.setHead(new Predicate(getHead()));		
			rewrites.add(finalRule);*/
			Rule sinkRule = rewritesByVariable.get(primaryKeyVariable);
			sinkRule.setHead(new Predicate(getHead()));
			sinkRule.relationalType = RelationalType.NOT_RELATIONAL;
			if (useEagerAggregation) applyEagerAggregation(rewrites, rewritesByVariable);
			removeUnncessaryVariables(rewrites, rewritesByVariable);
		}
		return rewrites;
	}
	
	public List<Rule> rewriteEdgeBased(boolean useEagerAggregation)
	{
		generateRuleGraph();
		List<Rule> rewrites = new ArrayList<Rule>();
		if (ruleGraph.isEmpty()) rewrites.add(this);
		else
		{
			DatalogVariable primaryKeyVariable = (DatalogVariable)getHead().getArgs().get(getHead().keyFields[0]);
			createDagFromRuleGraph(primaryKeyVariable);
			List<DatalogVariable> topologicalOrder = getTopologicalOrder(primaryKeyVariable);
			Multimap<DatalogVariable,Rule> rewriteRuleDependency = HashMultimap.create();
			for (Iterator<DatalogVariable> topologicalOrderIterator = topologicalOrder.iterator(); topologicalOrderIterator.hasNext();)
			{
				DatalogVariable v = topologicalOrderIterator.next();
				List<Rule> rewriteWithEdges = rewriteEdgeBased(v, rewriteRuleDependency, topologicalOrder);
				for (Rule rewrite : rewriteWithEdges)
					if (!(rewrite.getConditionSubgoals().isEmpty() && rewrite.getLitertalSubgoals().isEmpty()))
						rewrites.add(rewrite);
					else topologicalOrderIterator.remove();
			}
			
			/*Rule sinkRule = rewritesByVariable.get(primaryKeyVariable);
			Rule finalRule = new Rule();
			finalRule.addLiteralSubgoal(sinkRule.getHead());
			finalRule.setHead(new Predicate(getHead()));		
			rewrites.add(finalRule);*/
			Rule sinkRule = rewrites.get(rewrites.size() - 1);
//			System.out.println("^^^" + getHead());
//			System.out.println("^^^" + getHead().substitute(substitutionMap));
			sinkRule.setHead(getHead().substitute(substitutionMap));
			//sinkRule.setHead(new Predicate(getHead()));
//			System.out.println(sinkRule.getHead());
			sinkRule.relationalType = RelationalType.NOT_RELATIONAL;
			if (useEagerAggregation) applyEagerAggregationEdgeBased(rewrites, rewriteRuleDependency);
			removeUnncessaryVariables(rewrites, rewriteRuleDependency, true);
		}
		return rewrites;
	}

	private void applyEagerAggregation(List<Rule> rules, Map<DatalogVariable,Rule> rewritesByVariable)
	{
		for (Rule rule : rules)
		{
			if (rule.isAggregate)
			{
				int lastArgumentIndex = rule.getHead().getArgs().size() - 1;
				Expression aggregateArgument = (Expression)(rule.getHead().getArgs().get(lastArgumentIndex));
				Set<DatalogVariable> variablesInAggregateArgument = aggregateArgument.getIncludedDatalogVariables();
				DatalogVariable primaryKeyVariable = (DatalogVariable)(rule.getHead().getArgs().get(getHead().keyFields[0]));
				//Set<DatalogVariable> primaryKeyVariableAsSet = new HashSet<>();
				
				//primaryKeyVariableAsSet.add(primaryKeyVariable);
				DatalogVariable newAggregationVariable = rule.generateRandomVariableName();
				
				Set<DatalogVariable> predecessors = new HashSet<DatalogVariable>();
				for (Entry<DatalogVariable,DatalogVariable> entry : dag.entries())
				{
					DatalogVariable v1 = entry.getKey();
					DatalogVariable v2 = entry.getValue();
					if (v2.equals(primaryKeyVariable)) predecessors.add(v1);
				}
				
				// The following case is not supported, so we return the input without processing it
				if (predecessors.size() != 1) continue;
				
				DatalogVariable predecessor = predecessors.iterator().next();

				Rule predecessorRule = rewritesByVariable.get(predecessor);
				
				if (!predecessorRule.canProduceVariables(variablesInAggregateArgument)) continue;

				boolean primaryKeyIsSameAsAggregationColumn = variablesInAggregateArgument.size() == 1 && variablesInAggregateArgument.contains(predecessor);
				boolean primaryKeyIsDisjointFromAggregationColumn = !variablesInAggregateArgument.contains(predecessor);
				Predicate predecessorRuleHead = predecessorRule.getHead();
				
				if (primaryKeyIsSameAsAggregationColumn)
				{
					predecessorRuleHead.addArg(aggregateArgument);
					
					Predicate predecessorLiteralSubgoal = null;
					for (Predicate literalSubgoal : rule.getLitertalSubgoals())
						if (literalSubgoal.getName().equals(predecessorRuleHead.getName()))
						{
							predecessorLiteralSubgoal = literalSubgoal;
							break;
						}
					predecessorLiteralSubgoal.addArg(newAggregationVariable);
					String newAggregateFunctionName = null;
					String existingAggregateFunctionName = ((UserDefinedFunction)aggregateArgument).getName();
					if (existingAggregateFunctionName.equalsIgnoreCase("CNT"))
						newAggregateFunctionName = "SUM";
					else newAggregateFunctionName = existingAggregateFunctionName;
					List<Expression> newAggregateFunctionArgs = new ArrayList<>();
					newAggregateFunctionArgs.add(newAggregationVariable);
					UserDefinedFunction newAggregateFunction = new UserDefinedFunction(newAggregateFunctionName, newAggregateFunctionArgs);
					predecessorRule.setAggregate();
					rule.getHead().getArgs().remove(lastArgumentIndex);
					rule.getHead().addArg(newAggregateFunction);
					
				}
				if (primaryKeyIsDisjointFromAggregationColumn)
				{
					predecessorRuleHead.getArgs().removeAll(variablesInAggregateArgument);
					predecessorRuleHead.addArg(aggregateArgument);

					Predicate predecessorLiteralSubgoal = null;
					for (Predicate literalSubgoal : rule.getLitertalSubgoals())
						if (literalSubgoal.getName().equals(predecessorRuleHead.getName()))
						{
							predecessorLiteralSubgoal = literalSubgoal;
							break;
						}
					predecessorLiteralSubgoal.getArgs().removeAll(variablesInAggregateArgument);
					predecessorLiteralSubgoal.addArg(newAggregationVariable);
					String newAggregateFunctionName = null;
					String existingAggregateFunctionName = ((UserDefinedFunction)aggregateArgument).getName();
					if (existingAggregateFunctionName.equalsIgnoreCase("COUNT"))
						newAggregateFunctionName = "SUM";
					else newAggregateFunctionName = existingAggregateFunctionName;
					List<Expression> newAggregateFunctionArgs = new ArrayList<>();
					newAggregateFunctionArgs.add(newAggregationVariable);
					UserDefinedFunction newAggregateFunction = new UserDefinedFunction(newAggregateFunctionName, newAggregateFunctionArgs);
					predecessorRule.setAggregate();
					
					rule.getHead().getArgs().remove(lastArgumentIndex);
					rule.getHead().addArg(newAggregateFunction);
	
				}
				
			}
		}
	}
	
	private void removeUnncessaryVariables(List<Rule> rules, Map<DatalogVariable,Rule> rewriteRuleDependency)
	{
		Multimap<DatalogVariable, Rule> rewriteRuleDependencyMultimap = HashMultimap.create();
		for (Entry<DatalogVariable, Rule> e : rewriteRuleDependency.entrySet())
			rewriteRuleDependencyMultimap.put(e.getKey(), e.getValue());
		removeUnncessaryVariables(rules, rewriteRuleDependencyMultimap, false);
	}

	private void removeUnncessaryVariables(List<Rule> rules, Multimap<DatalogVariable,Rule> rewriteRuleDependency, boolean edgeBased)
	{
		for (Rule rule : rules)
		{
			DatalogVariable primaryKeyVariable = (DatalogVariable)(rule.getHead().getArgs().get(getHead().keyFields[0]));
			
			Set<DatalogVariable> predecessors = new HashSet<DatalogVariable>();
			for (Entry<DatalogVariable,DatalogVariable> entry : dag.entries())
			{
				DatalogVariable v1 = entry.getKey();
				DatalogVariable v2 = entry.getValue();
				if (v2.equals(primaryKeyVariable)) predecessors.add(v1);
			}
			
			// The following case is not supported, so we return the input without processing it
			if (rewriteRuleDependency.get(primaryKeyVariable).size() != 1) continue;
			if (predecessors.size() != 1) continue;
			
			DatalogVariable predecessor = predecessors.iterator().next();

			Rule predecessorRule;
			if (edgeBased) predecessorRule = rewriteRuleDependency.get(primaryKeyVariable).iterator().next();
			else predecessorRule = rewriteRuleDependency.get(predecessor).iterator().next();

//			System.out.println("******************");
//			System.out.println(rule);
//			System.out.println(predecessorRule);
			Predicate predecessorRuleHead = predecessorRule.getHead();
			Predicate predecessorLiteralSubgoal = null;
			for (Predicate literalSubgoal : rule.getLitertalSubgoals())
				if (literalSubgoal.getName().equals(predecessorRuleHead.getName()))
				{
					predecessorLiteralSubgoal = literalSubgoal;
					break;
				}

			DatalogVariable predecessorPrimaryKeyVariable = (DatalogVariable)(predecessorRuleHead.getArgs().get(0));
			boolean isPredecessorSourceNodeVariableUnncessaryNecessary = false;
			
			Set<DatalogVariable> toRemove_predecessor_head = new HashSet<>();
			Set<DatalogVariable> toRemove_predecessor_subgoal = new HashSet<>();
			for (Object e : predecessorRuleHead.getArgs())
			{
				DatalogVariable v_predecessor_head;
				if (e instanceof DatalogVariable) v_predecessor_head = (DatalogVariable)e; else continue;
//				System.out.println("******************");
//				System.out.println(predecessorRuleHead);
//				System.out.println(predecessorRuleHead.getArgs());
//				System.out.println(predecessorLiteralSubgoal);
//				System.out.println(predecessorLiteralSubgoal.getArgs());
				DatalogVariable v_predecessor_subgaol = (DatalogVariable)predecessorLiteralSubgoal.getArgs().get(predecessorRuleHead.getArgs().indexOf(v_predecessor_head)); 
				if (v_predecessor_head == predecessorPrimaryKeyVariable)
					isPredecessorSourceNodeVariableUnncessaryNecessary = rule.checkIfVariableNecessary(predecessorLiteralSubgoal, v_predecessor_subgaol);
				else if (!rule.checkIfVariableNecessary(predecessorLiteralSubgoal, v_predecessor_subgaol))
				{
					toRemove_predecessor_head.add(v_predecessor_head);
					toRemove_predecessor_subgoal.add(v_predecessor_subgaol);
				}
			}
			for (DatalogVariable toRemove : toRemove_predecessor_head)
			{
				int toRemoveIndex = predecessorRuleHead.getArgs().indexOf(toRemove);
				int[] predecessorKeyFields = predecessorRuleHead.getKeyFields();
				for (int i = 0; i < predecessorKeyFields.length; i++)
				{
					int keyIndex = predecessorKeyFields[i];
					if (keyIndex > toRemoveIndex)
						predecessorKeyFields[i]--;
				}
				predecessorRuleHead.getArgs().remove(toRemoveIndex);
			}
			//predecessorRuleHead.getArgs().removeAll(toRemove_predecessor_head);
			predecessorLiteralSubgoal.getArgs().removeAll(toRemove_predecessor_subgoal);
			if (!isPredecessorSourceNodeVariableUnncessaryNecessary)
			{
				predecessorRule.setSourceNodeVariableUnncessary();
				//predecessorRuleHead.getArgs().remove(predecessorPrimaryKeyVariable);
				//predecessorLiteralSubgoal.getArgs().remove(predecessorPrimaryKeyVariable);
			}

		}		
	}
	
	private void applyEagerAggregationEdgeBased(List<Rule> rules, Multimap<DatalogVariable,Rule> rewriteRuleDependency)
	{
		for (Rule rule : rules)
		{
			if (rule.isAggregate)
			{
				int lastArgumentIndex = rule.getHead().getArgs().size() - 1;
				Expression aggregateArgument = (Expression)(rule.getHead().getArgs().get(lastArgumentIndex));
				Set<DatalogVariable> variablesInAggregateArgument = aggregateArgument.getIncludedDatalogVariables();
				DatalogVariable primaryKeyVariable = (DatalogVariable)(rule.getHead().getArgs().get(getHead().keyFields[0]));
				//Set<DatalogVariable> primaryKeyVariableAsSet = new HashSet<>();
				
				//primaryKeyVariableAsSet.add(primaryKeyVariable);
				DatalogVariable newAggregationVariable = rule.generateRandomVariableName();
				
				Set<DatalogVariable> predecessors = new HashSet<DatalogVariable>();
				for (Entry<DatalogVariable,DatalogVariable> entry : dag.entries())
				{
					DatalogVariable v1 = entry.getKey();
					DatalogVariable v2 = entry.getValue();
					if (v2.equals(primaryKeyVariable)) predecessors.add(v1);
				}
				
				// The following case is not supported, so we return the input without processing it
				if (rewriteRuleDependency.get(primaryKeyVariable).size() != 1) continue;
				if (predecessors.size() != 1) continue;
				
				DatalogVariable predecessor = predecessors.iterator().next();

				Rule predecessorRule = rewriteRuleDependency.get(primaryKeyVariable).iterator().next();
				
//				System.out.println("HERE4");
				if (!predecessorRule.canProduceVariables(variablesInAggregateArgument)) continue;
//				System.out.println("HERE3");
				
				boolean primaryKeyIsSameAsAggregationColumn = variablesInAggregateArgument.size() == 1 && variablesInAggregateArgument.contains(predecessor);
				boolean primaryKeyIsDisjointFromAggregationColumn = !variablesInAggregateArgument.contains(predecessor);
				Predicate predecessorRuleHead = predecessorRule.getHead();
				
				Predicate predecessorLiteralSubgoal = null;
				for (Predicate literalSubgoal : rule.getLitertalSubgoals())
					if (literalSubgoal.getName().equals(predecessorRuleHead.getName()))
					{
						predecessorLiteralSubgoal = literalSubgoal;
						break;
					}
				
				if (primaryKeyIsSameAsAggregationColumn)
				{
//					System.out.println("HERE1");
					predecessorRuleHead.addArg(aggregateArgument);
					
					predecessorLiteralSubgoal.addArg(newAggregationVariable);
					String newAggregateFunctionName = null;
					String existingAggregateFunctionName = ((UserDefinedFunction)aggregateArgument).getName();
					if (existingAggregateFunctionName.equalsIgnoreCase("CNT"))
						newAggregateFunctionName = "SUM";
					else newAggregateFunctionName = existingAggregateFunctionName;
					List<Expression> newAggregateFunctionArgs = new ArrayList<>();
					newAggregateFunctionArgs.add(newAggregationVariable);
					UserDefinedFunction newAggregateFunction = new UserDefinedFunction(newAggregateFunctionName, newAggregateFunctionArgs);
					predecessorRule.setAggregate();
					rule.getHead().getArgs().remove(lastArgumentIndex);
					rule.getHead().addArg(newAggregateFunction);
					
				}
				if (primaryKeyIsDisjointFromAggregationColumn)
				{
//					System.out.println("HERE2");
					predecessorRuleHead.getArgs().removeAll(variablesInAggregateArgument);
					predecessorRuleHead.addArg(aggregateArgument);

					predecessorLiteralSubgoal.getArgs().removeAll(variablesInAggregateArgument);
					predecessorLiteralSubgoal.addArg(newAggregationVariable);
					String newAggregateFunctionName = null;
					String existingAggregateFunctionName = ((UserDefinedFunction)aggregateArgument).getName();
					if (existingAggregateFunctionName.equalsIgnoreCase("COUNT"))
						newAggregateFunctionName = "SUM";
					else newAggregateFunctionName = existingAggregateFunctionName;
					List<Expression> newAggregateFunctionArgs = new ArrayList<>();
					newAggregateFunctionArgs.add(newAggregationVariable);
					UserDefinedFunction newAggregateFunction = new UserDefinedFunction(newAggregateFunctionName, newAggregateFunctionArgs);
					predecessorRule.setAggregate();
					
					rule.getHead().getArgs().remove(lastArgumentIndex);
					rule.getHead().addArg(newAggregateFunction);
	
				}
				predecessorRuleHead.setKeyFields(new int[]{predecessorRuleHead.getArgs().size() - 2});

				
			}
		}
	}
	
	private void setSourceNodeVariableUnncessary()
	{
		this.SourceNodeVariableUnncessaryUnnecessary = true;
	}
	
	public boolean isSourceNodeVariableUnncessary()
	{
		return SourceNodeVariableUnncessaryUnnecessary;
	}

	private boolean checkIfVariableNecessary(Predicate skipPredicate, DatalogVariable v)
	{
		if (getHead().getIncludedDatalogVaraibles().contains(v)) return true;
		for (Predicate p : getLitertalSubgoals())
		{
			if (skipPredicate == p) continue;
			if (p.getIncludedDatalogVaraibles().contains(v)) return true;
		}
		
		for (Expression e : getConditionSubgoals())
		{
			if (e.getIncludedDatalogVariables().contains(v)) return true;
		}
		return false;
	}
	
	private boolean canProduceVariables(Set<DatalogVariable> variables)
	{
		for (DatalogVariable v : variables)
		{
			boolean found = false;
			for (Predicate p : getLitertalSubgoals())
				if (p.getIncludedDatalogVaraibles().contains(v)) { found = true; break; }
			if (!found) return false;
			System.out.println("FOUND: " + v);
		}
		return true;
	}

	private DatalogVariable generateRandomVariableName()
	{
		Set<DatalogVariable> existingVariables = new HashSet<>();
		
		for (Expression e : head.getArgs())
			existingVariables.addAll(e.getIncludedDatalogVariables());
		for (Predicate<Expression> litearalSubgoal : getLitertalSubgoals())
			for (Expression e : litearalSubgoal.getArgs())
				existingVariables.addAll(e.getIncludedDatalogVariables());
		
		System.out.println("existingVariables "+existingVariables);
		
		DatalogVariable randomVariable = null;
		Random r = new Random();
		do
		{
			randomVariable = new DatalogVariable(String.valueOf((char) (r.nextInt(26) + 'A')) + "_" + this.getHead().getName());
		} while (existingVariables.contains(randomVariable));
		return randomVariable;
		
	}
	
}
