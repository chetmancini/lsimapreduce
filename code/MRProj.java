import java.io.*;

public class MRProj{

	String productionFile = "../data/production.txt";
	FileInputStream fs = new FileInputStream(productionFile);
	DataInputStream in = new DataInputStream(fs);
	BufferedReader br = new BufferedReader(new InputStreamReader(in));

	// compute filter parameters for netid ajd28
	float fromNetID = 0.974;
	float desiredDensity = 0.59;
	float wMin = 0.4 * fromNetID;
	float wLimit = wMin + desiredDensity;

	public float parseLine(String line){
		return Float.parseFloat(line.replace("\n", ""))
	}

	// assume 0.0 <= wMin <= wLimit <= 1.0
	public boolean getNextFilteredInput() {
		float w = 0.0 //... (read and parse next input line) ...
		return ( ((w >= wMin) && (w < wLimit)) ? true : false );
	}


	public void load(int N){
		for( int j = 0; j < N; j++ ) {
		    for( int i = 0; i < j; i++ ) {
		        a[i, j] = getNextFilteredInput();
		        a[j, i] = getNextFilteredInput();
		    }
		    a[j, j] = getNextFilteredInput();
		}
	}


}


