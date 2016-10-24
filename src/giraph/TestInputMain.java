package giraph;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import schema.Tuple;

public class TestInputMain {
	public static void main(String[] args)
	{
		String input = "[[0,0][(0,1)[(16,46)](2,1)[(1,42)(16,20)](4,2)[](6,3)[(5,72)](8,5)[(12,40)](10,3)[(5,65)(1,14)](12,1)[](14,3)[(1,97)](16,2)[(18,96)](18,1)[]][(1,1,1)(2,0,0)(5,1,1)(8,0,0)(9,1,1)(11,1,1)(12,0,0)(13,1,1)(15,1,1)(16,0,0)(17,1,1)(18,0,0)(0,0,0)]]";
		
		Pattern id = Pattern.compile("\\[\\[(\\d+?,\\d+?)\\]");
		Matcher m = id.matcher(input);
		if(m.find())
		{
			String[] sv_id = m.group(1).split(",");
			System.out.println(Integer.parseInt(sv_id[0])+","+ Integer.parseInt(sv_id[1]));
		}
		
		
//		Pattern sv = Pattern.compile("(\\d,\\d)\\]\\[");
//		Pattern vdata = Pattern.compile("\\((\\d+,\\d+)\\)(\\[.*?\\])");
//		Pattern edges = Pattern.compile("\\((\\d+,\\d+)\\)");
//		Matcher v_matcher = vdata.matcher(input);
//		Matcher e_matcher = edges.matcher("");
//		System.out.println(v_matcher.groupCount());
//		while(v_matcher.find())
//		{
//			System.out.println(v_matcher.group(0));
//			System.out.println(v_matcher.group(1));
//			System.out.println(v_matcher.group(2));
//			e_matcher.reset(v_matcher.group(2));
//			while(e_matcher.find())
//			{
//				System.out.println(e_matcher.group(1));
//			}
//		}
		
		
		//Pattern match the input string, [(), [()[]()[]][()]]
		Pattern sv_data = Pattern.compile("(\\[.*?\\])(\\[.*?\\])\\]");
		Pattern vdata = Pattern.compile("\\((\\d+,\\d+)\\)(\\[.*?\\])");
		Pattern edges = Pattern.compile("\\((\\d+,\\d+)\\)");
		Pattern sv_edges = Pattern.compile("\\((\\d+,\\d+,\\d+)\\)");
		Matcher sv_matcher = sv_data.matcher(input.substring(6));
		Matcher v_matcher = vdata.matcher("");
		Matcher e_matcher = edges.matcher("");
		Matcher sve_matcher = sv_edges.matcher("");
		
		while(sv_matcher.find())
		{
			System.out.println(sv_matcher.group(0));
			System.out.println(sv_matcher.group(1));
			System.out.println(sv_matcher.group(2));
			
			v_matcher.reset(sv_matcher.group(1));
	
			while(v_matcher.find())
				{
				System.out.println(v_matcher.group(0));
				
				System.out.println(v_matcher.group(1)); // vertex id
				int[] vertexTuple = new int[2];
				String[] v_id = v_matcher.group(1).split(",");
				vertexTuple[0] = Integer.parseInt(v_id[0]);
				vertexTuple[1] = Integer.parseInt(v_id[1]);
				
				System.out.println(v_matcher.group(2)); //all edges
				e_matcher.reset(v_matcher.group(2));
				while(e_matcher.find())
				{
					System.out.println(e_matcher.group(1));
					int[] edgeTuple = new int[3];
					edgeTuple[0] = vertexTuple[0];
					String[] e_id = e_matcher.group(1).split(",");
					edgeTuple[1] = Integer.parseInt(e_id[0]);
					edgeTuple[2] = Integer.parseInt(e_id[1]);					
				}
			}
			
			System.out.println(sv_matcher.group(2)); //all sv edges
			sve_matcher.reset(sv_matcher.group(2));
			while(sve_matcher.find())
			{
				System.out.println(sve_matcher.group(1));
				String[] e_id = sve_matcher.group(1).split(",");
				int[] neighborSuperVertexTuple = new int[3];
				neighborSuperVertexTuple[0] = Integer.parseInt(e_id[0]);
				neighborSuperVertexTuple[1] = Integer.parseInt(e_id[1]);
				neighborSuperVertexTuple[2] = Integer.parseInt(e_id[2]);
			}
		}
	}
}
