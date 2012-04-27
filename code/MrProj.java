import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.mahout.math.SparseMatrix;

public class MrProj{

	private String productionFile = "../data/production.txt";
	private String[] testFiles = 
		{"../data/test1.txt", "../data/test2.txt", 
			"../data/test3.txt", "../data/test4.txt",
			"../data/test5.txt", "../data/test6.txt",
			"../data/test7.txt", "../data/test8.txt",
			"../data/test9.txt", "../data/test10.txt",
			"../data/test11.txt"};


	// compute filter parameters for netid cam479 (Chet)
	private float fromNetID = 0.974f;
	private float desiredDensity = 0.59f;
	private float wMin = (float) (0.4 * fromNetID);
	private float wLimit = wMin + desiredDensity;
	
	private BufferedReader reader;
	
	private SparseColumnMatrix matrix;
	
	/**
	 * Constructor
	 */
	public MrProj(){
		int N = this.matrixSizeN();
		this.matrix = new SparseColumnMatrix(N);
	}
	
	/**
	 * Really fast way to count lines in a file.
	 * About 6x faster than readLines().
	 * Thanks, StackOverflow member "martinus"
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public int fastCount(String filename) throws IOException{
		InputStream is = new BufferedInputStream(new FileInputStream(filename));
		try{
			byte[] c = new byte[1024];
			int count = 0;
			int readChars = 0;
			while((readChars = is.read(c)) != -1){
				for(int i=0;i<readChars; ++i){
					if(c[i] == '\n'){
						++count;
					}
				}
			}
			return count;
		}finally{
			is.close();
		}
	}

	/**
	 * Calculate size of necessary matrix, given a number of lines.
	 * @return
	 */
	private int matrixSizeN(){
		int linecount = 100;
		return (int) Math.sqrt(linecount);
	}
	
	/**
	 * Get a buffered reader based on a specific file.
	 * @param filename
	 * @return
	 */
	private BufferedReader openFile(String filename){
		try{
			FileInputStream fs = new FileInputStream(filename);
			DataInputStream in = new DataInputStream(fs);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			return br;
		}catch(IOException ioe){
			return null;
		}
	}
	
	/**
	 * Parse a line from a file.
	 * @param line
	 * @return
	 */
	public float parseLine(String line){
		return Float.parseFloat(line.replace("\n", ""));
	}

	/**
	 * Get the next filtered input line.
	 * @return
	 */
	public boolean getNextFilteredInput() {
		String line;
		try {
			line = this.reader.readLine();
			float w = parseLine(line);
			return ( ((w >= wMin) && (w < wLimit)) ? true : false );
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Load in from file using algorithm described.
	 * @param N
	 */
	public void load(int N){
		for( int j = 0; j < N; j++ ) {
		    for( int i = 0; i < j; i++ ) {
		    	matrix.put(i, j, getNextFilteredInput());
		    	matrix.put(j, i, getNextFilteredInput());
		    }
		    matrix.put(j, j, getNextFilteredInput());
		}
	}


}


