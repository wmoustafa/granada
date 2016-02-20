	package udf;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;


public class UserDefinedFunction {
	
	public static String Mode(Object arg1, Object arg2)
	{
		int a = (Integer)arg1;
		int b = (Integer)arg2;
		return (a>b?"A":"B");
	}
	
	public static String AP_CLASSIFY(Object arg1, Object arg2, Object arg3)
	{
		int a = (Integer)arg1;
		int b = (Integer)arg2;
		int c = (Integer)arg1;
		if (a>=b)
		{
			if (a>=c) return "DB";
			else return "SE";
		}
		else
		{
			if (b>=c) return "ML";
			else return "SE";
		}

	}

	public static Integer INT(Object arg1)
	{
		//System.out.println("&&&"+arg1);
		String s = arg1.toString();
		return (int)Float.parseFloat(s);
	}

	public static Integer AP_CONFIDENCE(Object arg1, Object arg2, Object arg3)
	{
		int a = (Integer)arg1;
		int b = (Integer)arg2;
		int c = (Integer)arg1;
		double sum=a+b+c;
		if (a>=b)
		{
			if (a>=c) return (int)(a/sum*100);
			else return (int)(c/sum*100);
		}
		else
		{
			if (b>=c) return (int)(b/sum*100);
			else return (int)(c/sum*100);
		}

	}

	public static String SHINGLE(Object o1, Object o2)
	{
		String s = (String)o1;
		Integer restrictiveness = (Integer)o2;
		char[] chars = s.toCharArray();
		Set<Character> charset = new HashSet<Character>();
		for (char c : chars) charset.add(c);
		PriorityQueue<Character> hashes = new PriorityQueue<Character>();
		for (Character c : charset) hashes.add(c);
		
		char[] result = new char[restrictiveness];
		for (int i=0; i<restrictiveness; i++)
		{
			Character top = hashes.poll();
			if (top==null) break;
			result[i] = top;
		}
		return new String(result);
	}
	
	
	public static Integer STR_SIM(Object o1, Object o2)
	{
		String s1 = o1.toString();
		String s2 = o2.toString();
    	char ch = s1.charAt(s1.length()-1);
    	if ('0'<=ch&&ch<='9') return 0;
    	ch = s2.charAt(s2.length()-1);
    	if ('0'<=ch&&ch<='9') return 0;
		double s=similarity(s1, s2);
		return (int)(s*100);

	}
	
	public static Integer ER_CLASSIFY(Object arg1, Object arg2)
	{
		int j = (Integer)arg1;
		int s = (Integer)arg2;
		if (j>10&&s>50) return 1; else return 0;

	}

	public static Integer ER_CONFIDENCE(Object arg1, Object arg2)
	{
		int j = (Integer)arg1;
		int s = (Integer)arg2;
		return (j+s)/2;

	}

	private static int minOfThree(int a, int b, int c) {
		 return Math.min(a, Math.min(b, c));
		 }
		 
		 private static int calculateCost(char s, char t) {
		 return s==t?0:1;
		 }
		 
		 private static int[][] initializeMatrix(int s, int t) {
		 int[][] matrix = new int[s + 1][t + 1];
		 for (int i = 0; i <= s; i++) {
		 matrix[i][0] = i;
		 }
		 
		 for (int j = 0; j <= t; j++) {
		 matrix[0][j] = j;
		 }
		 return matrix;
		 }
		 
		 public static double similarity(final String s, final String t) {
		 
		 char[] s_arr = s.toCharArray();
		 char[] t_arr = t.toCharArray();		
		 
		 if(s.equals(t)) { return 0; }
		 if (s_arr.length == 0) { return t_arr.length;}
		 if (t_arr.length == 0) { return s_arr.length;}
		 
		 int matrix[][] = initializeMatrix(s_arr.length, t_arr.length);
		 
		 for (int i = 0; i < s_arr.length; i++) {
		 for (int j = 1; j <= t_arr.length; j++) {
		    matrix[i+1][j] = minOfThree(matrix[i][j] + 1,	
		        matrix[i+1][j - 1] + 1, 
		        matrix[i][j - 1] + calculateCost(s_arr[i], t_arr[j-1]));
		 }
		 }
		 
		 return 1.0-matrix[s_arr.length][t_arr.length]/(double)Math.max(s.length(), t.length());
		 }

		 public static Integer Confidence(Object arg1, Object arg2)
	{
		int a = (Integer)arg1;
		int b = (Integer)arg2;
		if (a+b==0) return 0;
		int c = (a>b?a:b);
		return (int)((float)c/(a+b)*100);
	}
	
	public static Integer LabelMatch(Object arg1, Object arg2)
	{
		return arg1.equals(arg2) ? 1 : 0;
	}
	
	public static Integer Match(Object arg1, Object arg2)
	{
		String s1 = (String) arg1;
		String s2 = (String) arg2;
		
		if(s1.length()!=s2.length()) {
			throw new RuntimeException("Lengths should match");
		}
		
		double nummatch = 0;
		double size = s1.length();
		for(int i=0; i<size; i++) {
			if(s1.charAt(i)==s2.charAt(i)) {
				nummatch++;
			}
		}
		
		return ((int) ((nummatch/size)*1000.0));
	}
	
	// ICA
}
