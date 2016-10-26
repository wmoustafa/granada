package test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CompareOutputsTest 
{
	private String dl_file;
	private String g_file;
	private Boolean expectedResult;
	private CompareDatalogToGiraphOutputTest checker;
	private static final String PATH="/Users/papavas/Documents/UCSD/research/nec_git_repository/granada_no_json/granada/outputs/";
	
	   @Before
	   public void initialize() {
		   checker = new CompareDatalogToGiraphOutputTest();
	   }
	   
	   // Each parameter should be placed as an argument here
	   // Every time runner triggers, it will pass the arguments
	   // from parameters we defined in primeNumbers() method
		
	   public CompareOutputsTest(String dl_file, String g_file, Boolean expectedResult) {
	      this.dl_file = dl_file;
	      this.g_file = g_file;
	      this.expectedResult = expectedResult;
	   }

	   @Parameterized.Parameters
	   public static Collection inputFiles() {
	      return Arrays.asList(new Object[][] {
	         {PATH+"dl_small_20.1", PATH+"giraph_small_20.1" , true },
	         {PATH+"dl_pokec.1000", PATH+"giraph_pokec.1000" , true }
	      });
	   }

	   // This test will run 4 times since we have 5 parameters defined
	   @Test
	   public void testCompareDatalogToGiraphOutput() throws IOException {
	      System.out.println("Parameterized dl file is : " + dl_file);
	      assertEquals(expectedResult, checker.compareOutputs(dl_file, g_file));
	   }

}
