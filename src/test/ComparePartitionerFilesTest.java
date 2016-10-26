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
public class ComparePartitionerFilesTest {
	private String file1;
	private String file2;
	private Boolean expectedResult;
	private CompareSnapToPartitioner checker;
	private static final String PATH="/Users/papavas/Documents/UCSD/research/nec_git_repository/granada_no_json/granada/inputs/";
	
	   @Before
	   public void initialize() {
		   checker = new CompareSnapToPartitioner();
	   }
	   
	   // Each parameter should be placed as an argument here
	   // Every time runner triggers, it will pass the arguments
	   // from parameters we defined in primeNumbers() method
		
	   public ComparePartitionerFilesTest(String f1, String f2, Boolean expectedResult) {
	      this.file1 = f1;
	      this.file2 = f2;
	      this.expectedResult = expectedResult;
	   }

	   @Parameterized.Parameters
	   public static Collection inputFiles() {
	      return Arrays.asList(new Object[][] {
	         {PATH+"small_20.txt", PATH+"small_20.1.csv" , true },
	         {PATH+"small_20.txt", PATH+"small_20.1.datalog.txt" , true }
	      });
	   }

	   // This test will run 4 times since we have 5 parameters defined
	   @Test
	   public void testCompareSnaptoPartitionerOutput() throws IOException {
	      assertEquals(expectedResult, checker.compareOutputs(file1, file2));
	   }


}
