package giraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import algebra.Predicate;
import algebra.Program;
import algebra.RelationalType;
import algebra.Rule;


public class DatalogDependencyGraph {
	
	Program p;
	Multimap<Vertex,Vertex> g = HashMultimap.create();
	Multimap<Vertex,Vertex> dag = HashMultimap.create();
	Multimap<Vertex,Vertex> components = HashMultimap.create();
	Stack<Vertex> stack = new Stack<Vertex>();
	Map<Vertex,Vertex> vertexToComponet = new HashMap<Vertex,Vertex>();
	Multimap<Vertex,Vertex> componentToVertices = HashMultimap.create();
	Multimap<Vertex,Vertex> componentToPeripheralVertices = HashMultimap.create();
	Map<String,Vertex> relationsToVertices = new HashMap<String,Vertex>();
	Multimap<String,Rule> relationsToRules = HashMultimap.create();
	
	Set<Vertex> closedVertices = new HashSet<Vertex>();
	int index = 0;
	
	public DatalogDependencyGraph(Program p) {
		this.p = p;
		for (Rule r : p.getRules())
		{
			relationsToVertices.put(r.getHead().getName(), new Vertex(r.getHead().getName()));
			relationsToRules.put(r.getHead().getName(), r);
			for (Predicate bodyPredicate : r.getLitertalSubgoals())
				relationsToVertices.put(bodyPredicate.getName(), new Vertex(bodyPredicate.getName()));
		}

		for (Rule r : p.getRules())
			for (Predicate bodyPredicate : r.getLitertalSubgoals())
				g.put(relationsToVertices.get(bodyPredicate.getName()), relationsToVertices.get(r.getHead().getName()));

		findStronglyConnectedComponents();		
		createDagAndComponents();
		
	}
	
	public DatalogDependencyGraph() {		
	}
	
	public static void main(String[] args)
	{
		DatalogDependencyGraph g = new DatalogDependencyGraph();
		/*Vertex n1 = g.new Vertex("1");
		Vertex n2 = g.new Vertex("2");
		Vertex n3 = g.new Vertex("3");
		Vertex n4 = g.new Vertex("4");
		Vertex n5 = g.new Vertex("5");
		Vertex n6 = g.new Vertex("6");
		Vertex n7 = g.new Vertex("7");
		g.relationsToVertices.put("1", n1);
		g.relationsToVertices.put("2", n2);
		g.relationsToVertices.put("3", n3);
		g.relationsToVertices.put("4", n4);
		g.relationsToVertices.put("5", n5);
		g.relationsToVertices.put("6", n6);
		g.relationsToVertices.put("7", n7);
		g.g.put(n1, n2);
		g.g.put(n1, n3);
		g.g.put(n2, n4);
		g.g.put(n4, n2);
		g.g.put(n3, n5);
		g.g.put(n5, n3);
		g.g.put(n4, n6);
		g.g.put(n5, n6);
		g.g.put(n5, n7);*/
		
		Vertex n1 = g.new Vertex("e");
		Vertex n2 = g.new Vertex("v");
		Vertex n3 = g.new Vertex("L_X0");
		Vertex n4 = g.new Vertex("L_Y0");
		Vertex n5 = g.new Vertex("L_X1");
		Vertex n6 = g.new Vertex("L_Y1");
		Vertex n7 = g.new Vertex("L");
		g.relationsToVertices.put("e", n1);
		g.relationsToVertices.put("v", n2);
		g.relationsToVertices.put("L_X0", n3);
		g.relationsToVertices.put("L_Y0", n4);
		g.relationsToVertices.put("L_X1", n5);
		g.relationsToVertices.put("L_Y1", n6);
		g.relationsToVertices.put("L", n7);
		g.g.put(n1, n4);
		g.g.put(n1, n3);
		g.g.put(n4, n5);
		g.g.put(n4, n6);
		g.g.put(n3, n5);
		g.g.put(n3, n6);
		g.g.put(n5, n7);
		g.g.put(n2, n7);
		g.g.put(n7, n4);
		
		List<Rule> toProcess = g.getFirstToProcess();
		//System.out.println(toProcess);
		for (int i = 0; i< 10; i++)
		{
			Map<String,Boolean> changed = new HashMap<String,Boolean>();
			for (Rule v : toProcess)
				if (g.isSingleNodeComponent(g.relationsToVertices.get(v))) changed.put(v.getHead().getName(), true);
				else changed.put(v.getHead().getName(), Math.random() < 0.8);
			toProcess = g.getNextToProcess(changed);
			//System.out.println(toProcess);
		}
	}
	
	public class Vertex {
		
		public Vertex(String vertexId) {
			this.vertexId = vertexId;
			this.index = Integer.MAX_VALUE;
			this.component = Integer.MAX_VALUE;
		}
		
		public Vertex(Vertex v)
		{
			this.vertexId = v.vertexId;
			this.index = Integer.MAX_VALUE;
			this.component = Integer.MAX_VALUE;
		}
		
		String vertexId;
		int index;
		int component;
		
		public String toString()
		{
			return vertexId;
		}

	}
	
	List<Rule> getFirstToProcess()
	{		
		Map<String,Boolean> changed = new HashMap<String,Boolean>();
		for (Vertex v : getSourceVertices()) changed.put(v.vertexId, true);
		List<Rule> toProcess = getNextToProcess(changed);
		
		return toProcess;
	}
	
	List<Rule> getNextToProcess(Map<String,Boolean> relationsChanged)
	{
		List<Rule> toProcessBatch = new ArrayList<>();

		boolean endOfBatch = false;
		
		while(!endOfBatch)
		{
			Map<Vertex,Boolean> verticesChanged = new HashMap<Vertex,Boolean>();
			Set<Vertex> componentsUnderProcessing = new HashSet<Vertex>();
			for (Entry<String, Boolean> entry : relationsChanged.entrySet())
			{
				String relationName = entry.getKey();
				Boolean changed = entry.getValue();
				Vertex v = relationsToVertices.get(relationName);
				if (!isRecursiveComponent(vertexToComponet.get(v))) closedVertices.add(v);
				else verticesChanged.put(v, changed);
			}
			
			for (Vertex n : componentToVertices.keySet())
			{
				Collection<Vertex> verticesInComponent = new HashSet<Vertex>(componentToVertices.get(n));
				verticesInComponent.retainAll(verticesChanged.keySet());
				if (verticesInComponent.isEmpty()) continue;
				boolean componentChanged = false;
				for (Vertex v : verticesInComponent)
					if (verticesChanged.get(v)) { componentChanged = true; break; }
				if (!componentChanged) closedVertices.add(n);
				else componentsUnderProcessing.add(n);
			}
			
			List<Rule> toProcess = new ArrayList<Rule>();
			for (Entry<Vertex, Boolean> entry : verticesChanged.entrySet())
			{
				Vertex v = entry.getKey();
				Boolean vChanged = entry.getValue();
				if (vChanged)
					for (Vertex w : components.get(v))
						toProcess.addAll(relationsToRules.get(w.vertexId));
			}
			
			for (Vertex v : componentsUnderProcessing)
				toProcess.removeAll(getNonRecursiveRulesInComponent(v));
			
			for (Vertex v : dag.keySet())
			{
				if (closedVertices.contains(v) || componentsUnderProcessing.contains(v)) continue;
				boolean allPredecessorsClosed = true;
				for (Vertex w : dag.get(v))
				{
					if (!closedVertices.contains(w)) { allPredecessorsClosed = false; break; }
				}
				
				if (allPredecessorsClosed)
					if (!isRecursiveComponent(v)) toProcess.addAll(relationsToRules.get(v.vertexId));
					else toProcess.addAll(getNonRecursiveRulesInComponent(v));
						//for (Vertex w : componentToPeripheralVertices.get(v))
							//toProcess.add(w.vertexId);
			}
			
			relationsChanged.clear();
			for (Rule r : toProcess)
			{
				toProcessBatch.add(r);
				if (!(r.getRelationalType() == RelationalType.NOT_RELATIONAL))
				{
					endOfBatch = true;
					break; 
				}
				else relationsChanged.put(r.getHead().getName(), true);
			}
			if (toProcess.isEmpty()) endOfBatch = true;
		
		}
		
		return toProcessBatch;
	}

	private void findStronglyConnectedComponents()
	{
		for (Vertex v : g.keySet())
			if (v.index == Integer.MAX_VALUE)
				findStronglyConnectedComponents(v);
	}
	
	private void findStronglyConnectedComponents(Vertex v)
	{
		v.index = index;
		v.component = index;
		index++;
		stack.push(v);

		
		for (Vertex w : g.get(v))
		{
			if (w.index == Integer.MAX_VALUE)
			{
				findStronglyConnectedComponents(w);
				v.component = min(v.component, w.component);
			}
			else if (stack.contains(w))
				v.component = min(v.component, w.index);			
		}
		
		if (v.component == v.index)
		{
			Vertex vc = new Vertex(v.vertexId+"Comp");
			Set<Vertex> verticesInComponent = new HashSet<Vertex>();
			Vertex w;
			do
			{
				w = stack.pop();
				verticesInComponent.add(w);
			} while (w != v);
			
			if (verticesInComponent.size() == 1)
			{
				vertexToComponet.put(v, v);
				componentToVertices.put(v, v);
			}
			else
			{
				for (Vertex w1 : verticesInComponent)
				{
					vertexToComponet.put(w1, vc);
					componentToVertices.put(vc, w1);					
				}
			}
		}
	}
		
	private void createDagAndComponents()
	{
		for (Entry<Vertex, Vertex> e : g.entries())
		{
			Vertex src = e.getKey();
			Vertex dst = e.getValue();
			if (src.component == dst.component)
				components.put(src, dst);
			else
			{
				Vertex dagSrc, dagDst;
				if (isSingleNodeComponent(src)) dagSrc = src; else dagSrc = vertexToComponet.get(src);
				if (isSingleNodeComponent(dst)) dagDst = dst; else dagDst = vertexToComponet.get(dst);
				dag.put(dagDst, dagSrc);
				componentToPeripheralVertices.put(dagDst, dst);
			}
		}
	}
			
	private Set<Vertex> getSourceVertices()
	{
		Set<Vertex> sourceVertices = new HashSet<Vertex>();
		for (Vertex v : dag.values())
			if (isSingleNodeComponent(v) && dag.get(v).isEmpty()) sourceVertices.add(v);
		return sourceVertices;
	}
	
	private boolean isSingleNodeComponent(Vertex v)
	{
		return componentToVertices.get(v).size() == 1;
	}
		
	private int min(int a, int b)
	{
		return a < b ? a : b;
	}
	
	private Set<Rule> getNonRecursiveRulesInComponent(Vertex component)
	{
		Set<Rule> nonRecursiveRulesInComponent = new HashSet<Rule>();
		Set<String> relationsInComponent = getRelationsInComponent(component);
		for (Vertex w : componentToVertices.get(component))
		{
			for (Rule r : relationsToRules.get(w.vertexId))
				if (!isRecursiveRule(r, relationsInComponent)) nonRecursiveRulesInComponent.add(r);
		}
		return nonRecursiveRulesInComponent;
	}

	private Set<Rule> getRecursiveRulesInComponent(Vertex component)
	{
		Set<Rule> recursiveRulesInComponent = new HashSet<Rule>();
		Set<String> relationsInComponent = getRelationsInComponent(component);
		for (Vertex w : componentToVertices.get(component))
		{
			for (Rule r : relationsToRules.get(w.vertexId))
				if (isRecursiveRule(r, relationsInComponent)) recursiveRulesInComponent.add(r);
		}
		return recursiveRulesInComponent;
	}

	public boolean isRecursiveRule(Rule r)
	{
		Vertex component = getComponentForRule(r);
		Set<String> relationsInComponent = getRelationsInComponent(component);
		return isRecursiveRule(r, relationsInComponent);
	}

	private boolean isRecursiveRule(Rule r, Set<String> relationsInComponent)
	{
		boolean isRecursive = false;
		for (Predicate body : r.getLitertalSubgoals())
			if (relationsInComponent.contains(body.getName())) { isRecursive = true; break; }
		if (isRecursive) return true; else return false;
	}
	
	private Set<String> getRelationsInComponent(Vertex component)
	{
		Set<String> relationsInComponent = new HashSet<String>();
		for (Vertex w : componentToVertices.get(component))
			relationsInComponent.add(w.vertexId);
		return relationsInComponent;
	}
	
	private Vertex getComponentForRule(Rule r)
	{
		Vertex vertex = relationsToVertices.get(r.getHead().getName());
		Vertex component = vertexToComponet.get(vertex);
		return component;
	}

	private Set<Predicate> getRecursivePredicates(Rule r)
	{
		Set<Predicate> recursivePredicates = new HashSet<Predicate>();
		Vertex component = getComponentForRule(r);
		Set<String> relationsInComponent = getRelationsInComponent(component);
		for (Predicate body : r.getLitertalSubgoals())
			if (relationsInComponent.contains(body.getName())) { recursivePredicates.add(body); }
		return recursivePredicates;
	}
	
	public void setRecursivePredicatesForRules()
	{
		for (Rule r : p.getRules())
		{
			Set<Predicate> recursivePredicates = getRecursivePredicates(r);
			//System.out.println("recursive "+r + " " + recursivePredicates);
			r.setRecursivePredicates(recursivePredicates);
		}
	}
	
	public boolean isRecursiveComponent(Vertex component) {
		return !getRecursiveRulesInComponent(component).isEmpty();
	}
	
	public void adjustModularlyStratifiedForRules()
	{
		for (Vertex component : componentToVertices.keySet())
		{
			boolean modularlyStratifiedComponent = true;
			for (Rule r : getRecursiveRulesInComponent(component))
				modularlyStratifiedComponent = modularlyStratifiedComponent && r.isModularlyStratified();
			for (Rule r : getRecursiveRulesInComponent(component))
				r.setModularlyStratified(modularlyStratifiedComponent);
		}			
	}

}
