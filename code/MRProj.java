public class MRProj{

	// compute filter parameters for netid ajd28
	float fromNetID = 0.974;
	float desiredDensity = 0.59;
	float wMin = 0.4 * fromNetID;
	float wLimit = wMin + desiredDensity; 

	// assume 0.0 <= wMin <= wLimit <= 1.0
	public boolean getNextFilteredInput() {
		float w = 0.0 //... (read and parse next input line) ...
		return ( ((w >= wMin) && (w < wLimit)) ? true : false );
	}


	public void run(){
		for( int j = 0; j < N; j++ ) {
		    for( int i = 0; i < j; i++ ) {
		        a[i, j] = getNextFilteredInput();
		        a[j, i] = getNextFilteredInput();
		    }
		    a[j, j] = getNextFilteredInput();
		}
	}


}

