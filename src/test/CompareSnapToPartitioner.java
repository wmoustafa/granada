package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import test.TestUtils.LongPair;

public class CompareSnapToPartitioner {
	
	public boolean compareOutputs(String f1, String f2) throws IOException
	{
		List<LongPair> list1 = new ArrayList<LongPair>();
		List<LongPair> list2 = new ArrayList<LongPair>();
		
		readFileIntoList(f1, list1);
		readFileIntoList(f2, list2);

		Collections.sort(list1);
		Collections.sort(list2);
		System.out.println(list1);
		System.out.println(list2);
		
		if(list1.equals(list2))
			return true;
		return false;
	}

	private void readFileIntoList(String file,List<LongPair> list) throws IOException
	{
		if (file.endsWith("csv"))
		{
			readGiraphIntoList(file,list);
		}
		else if(file.endsWith("datalog.txt"))
		{
			readDatalogIntoList(file,list);
		}
		else
		{
			readSnapIntoList(file, list);
		}
		
	}
	
	private void readSnapIntoList(String file,List<LongPair> list) throws IOException
	{
		BufferedReader b = new BufferedReader(new FileReader(new File(file)));
		String line = null;
		while((line = b.readLine()) != null)
		{			
//			System.out.println(line);
			String[] items = line.split("\t");
//			System.out.println(items);
			LongPair pair = new LongPair(Long.parseLong(items[0]),Long.parseLong(items[1]));
			list.add(pair);
		}
		b.close();
	}

	
	private void readGiraphIntoList(String file,List<LongPair> list) throws IOException
	{
		BufferedReader b = new BufferedReader(new FileReader(new File(file)));
		String line = null;
		while((line = b.readLine()) != null)
		{			
//			System.out.println(line);
			String[] items = line.split("\t");
//			System.out.println(items);
			int counter = 3;
			while (counter < items.length) {
				LongPair pair = new LongPair(Long.parseLong(items[1]),Long.parseLong(items[counter+2]));
				list.add(pair);
				counter += 4;
			}
		}
		b.close();
	}
	
	private void readDatalogIntoList(String file,List<LongPair> list) throws IOException
	{
		BufferedReader b = new BufferedReader(new FileReader(new File(file)));
		String line = null;
		while((line = b.readLine()) != null)
		{			
			Pattern sv_data = Pattern.compile("(\\[.*?\\])(\\[.*?\\])\\]");
			Pattern vdata = Pattern.compile("\\((\\d+,\\d+)\\)(\\[.*?\\])");
			Pattern edges = Pattern.compile("\\((\\d+,\\d+)\\)");
			Matcher sv_matcher = sv_data.matcher(line.substring(6));
			Matcher v_matcher = vdata.matcher("");
			Matcher e_matcher = edges.matcher("");
			
			long vertex_id;
			long dst_id;
			if(sv_matcher.find())
			{
				v_matcher.reset(sv_matcher.group(1));
		
				//Read vertex data
				while(v_matcher.find())
				{
					//Read vertex id
					String[] v_id = v_matcher.group(1).split(",");
					vertex_id = Long.parseLong(v_id[0]);
					
					//Read edges of current vertex
					e_matcher.reset(v_matcher.group(2));
					while(e_matcher.find())
					{
						String[] e_id = e_matcher.group(1).split(",");
						dst_id = Long.parseLong(e_id[0]);
						LongPair pair = new LongPair(vertex_id,dst_id);
						list.add(pair);
					}
				}				
			}
			else
			{
				throw new IOException("The input string did not match the regex pattern for vertex data."
						+ "Input = " + line);
			}
		}
		b.close();
	}
}
