package test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import test.TestUtils.LongPair;

import static org.junit.Assert.assertEquals;

public class CompareDatalogToGiraphOutputTest 
{
	public boolean compareOutputs(String fdatalog, String fgiraph) throws IOException
	{
		List<LongPair> dl_list = new ArrayList<LongPair>();
		List<LongPair> g_list = new ArrayList<LongPair>();
		
		readFileIntoList(fdatalog, dl_list);
		readFileIntoList(fgiraph, g_list);
		
		Collections.sort(dl_list);
		Collections.sort(g_list);
		
		if(dl_list.equals(g_list))
			return true;
		System.out.println("first = " + dl_list);
		System.out.println("second = " + g_list);
		return false;
	}

	
	private void readFileIntoList(String file,List<LongPair> list) throws IOException
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
	}
			
	
}
