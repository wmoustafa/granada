package algebra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class Program {
	
	List<Rule> rules = new ArrayList<Rule>();
	
	public void addRule(Rule r)
	{
		rules.add(r);
	}
	
	public List<Rule> getRules()
	{
		return rules;
	}
	
	public Program rewrite(boolean useSemiJoin, boolean useEagerAggregation)
	{
		Program p = new Program();
		
	    for (Rule rule : getRules())
	    	if (!useSemiJoin) 
	    		p.rules.addAll(rule.rewrite(useEagerAggregation));
	    	else
	    		p.rules.addAll(rule.rewriteEdgeBased(useEagerAggregation));
	    
	    p.setAggregateRules();
	    p.removeRenamingRules();
	    	    
	    return p;		
	}
	
	public void setAggregateRules()
	{
		Set<String> aggregateRuleHeads = new HashSet<String>(); 
		for (Rule rule : rules)
			if (rule.isAggregate()) aggregateRuleHeads.add(rule.getHead().getName());
		for (Rule rule : rules)
			if (aggregateRuleHeads.contains(rule.getHead().getName())) rule.setAggregate();
	}
	
	private void removeRenamingRules() {
	    Map<String,String> substitutionMap = new HashMap<>();
	    List<Rule> renamingRules = new ArrayList<>();
	    for (Rule r : getRules())
	    	if (r.isRenamingRule()) {
	    		substitutionMap.put(r.head.getName(), r.getLitertalSubgoals().get(0).getName());
	    		renamingRules.add(r);
	    	}
	    rules.removeAll(renamingRules);
	    for (Rule r : rules)
	    	r.substitute(substitutionMap);		
	}
}