package org.lsi.mapreduce;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.lsi.containers.BitMatrix;
import org.lsi.containers.IntegerPair;
import org.lsi.containers.KeyValue;

/**
 * MrProjStatic
 * static version of other class.
 */
public class MrProj{

    public static String productionFile = "../data/production.txt";
    public static String[] testFiles = 
        {"../data/test1.txt", "../data/test2.txt", 
            "../data/test3.txt", "../data/test4.txt",
            "../data/test5.txt", "../data/test6.txt",
            "../data/test7.txt", "../data/test8.txt",
            "../data/test9.txt", "../data/test10.txt",
            "../data/test11.txt"};
    
    public static URL productionUrl = getProductionUrl();
    public static URL[] dataURLs = getUrls();

    public static float fromNetID = 0.974f;
    public static float desiredDensity = 0.59f;
    public static float wMin = (float) (0.4 * fromNetID);
    public static float wLimit = wMin + desiredDensity;

    /**
     * Get the URL of the production file.
     * @return
     */
    public static URL getProductionUrl(){
    	try {
			return new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/production.txt");
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
    }
    
    /**
     * Get an array of the url objects for the data files.
     * @return
     */
    public static URL[] getUrls(){
    	try{
	    	URL[] urls =
	    	{	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data1.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data2.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data3.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data4.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data5.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data6.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data7.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data8.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data9.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data10.txt"),
	        	new URL("http://edu-cornell-cs-cs5300s12-assign5-data.s3.amazonaws.com/data11.txt")
	        };
	    	return urls;
    	}catch (MalformedURLException e){
    		e.printStackTrace();
    		return null;
    	}
    }

    /**
     * Return if a given G divides N as described in the
     * connected components algorithm.
     */
    public static boolean dividesN(int N, int g){
        return (N % g) == 0;
    }

    /**
     * Calculate the weighted average.
     * @param componentSizes list of the components sizes in number of trees.
     * @param gridSize the size of the grid
     */
    public static double weightedAverage(List<Integer> componentSizes, int gridSize){
        double ret = 0.0;
        for(int ci : componentSizes){
            ret += (ci * ci / (double) gridSize);
        }
        return ret;
    }


    /**
     * Really fast way to count lines in a file.
     * About 6x faster than readLines().
     * Thanks, StackOverflow member "martinus"
     */
    public static int fastCount(String filename) throws IOException{
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try{
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            while((readChars = is.read(c)) != -1){
                for(int i = 0; i < readChars; ++i){
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
     * Get an index value from an (i, j) pair.
     */
    public static int getIndex(int i, int j, int N){
        return i + (j * N);
    }
    
    /**
     * Get index with repsect to a column group.
     * @param localI
     * @param localJ
     * @param N size of the side of a matirx.
     * @return the local index.
     */
    public static int getIndexLocal(int localI, int localJ, int N){
    	return localI + (localJ * N);
    }

    /**
     * Get the i component of an index.
     */
    public static int getI(int index, int N){
        return index % N;
    }
    
    /**
     * TODO: implment
     * @param index
     * @param N
     * @return the i component of the column group.
     */
    public static int getILocal(int localIndex, int N){
    	return localIndex % N;
    }

    /**
     * Get j component of an index
     */
    public static int getJ(int index, int N){
        return (int) index / N;
    }
    
    /**
     * TODO: implement
     * @param localIndex the localized index for this column group
     * @param N the side of a side of the matrix.
     * @return the local j component of the local index.
     */
    public static int getJLocal(int localIndex, int N){
    	return (int) localIndex / N;
    }
    
    /**
     * Get an integerpair (i,j) from an index
     * @param index the GLOBAL index.
     * @param N size of side of the matrix
     * @return a pair of indices i and j.
     */
    public static IntegerPair getIJ(int index, int N){
    	return new IntegerPair(MrProj.getI(index, N), MrProj.getJ(index, N));
    }
    
    /**
     * Get the next filtered input line.
     * @return the next line from the reader.
     */
    public static boolean getNextFilteredInput(BufferedReader reader) {
        try {
            return getBoolean(parseLineToFloat(reader.readLine()));
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Load in from file using algorithm described.
     * @param N the size of one side of the matrix.
     */
    public static BitMatrix getMatrix(int N, URL url){
    	BufferedReader reader = MrProj.openURL(url);
    	BitMatrix bitmatrix = new BitMatrix(N);
        for( int j = 0; j < N; j++ ) {
            for( int i = 0; i < j; i++ ) {
                bitmatrix.put(i, j, getNextFilteredInput(reader));
                bitmatrix.put(j, i, getNextFilteredInput(reader));
            }
            bitmatrix.put(j, j, getNextFilteredInput(reader));
        }
        return bitmatrix;
    }


    /**
     * Parse a line from a file.
     * @param line the line from the input.
     * @return a float representation of the line.
     */
    public static float parseLineToFloat(String line){
        return Float.parseFloat(line.replace("\n", ""));
    }
    
    /**
     * Parse a line for the second mapper.
     * Format:
     * [INT][TAB][INT]_[INT]
     * 
     * @param line the input line
     * @return parsed values
     */
    public static KeyValue<IntWritable, IntIntWritableTuple> parseLineSecondMapper(Text line){
    	StringTokenizer toker = new StringTokenizer(line.toString(), "_ \t");
    	int first = Integer.parseInt(toker.nextToken());
    	int second = Integer.parseInt(toker.nextToken());
    	int third = Integer.parseInt(toker.nextToken());
    	KeyValue<IntWritable, IntIntWritableTuple> ret = 
    			new KeyValue<IntWritable, IntIntWritableTuple>(
    					new IntWritable(first), 
    					new IntIntWritableTuple(second, third));
    	return ret;
    }
    
    
    /**
     * Convert a float to a tree or not (boolean)
     * @param in input float (from string)
     * @return whether or not there is a tree.
     */
    public static boolean getBoolean(float in){
    	return (((in >= MrProj.wMin) && (in < MrProj.wLimit)) ? true : false );
    }
    
    /**
     * Get a buffered reader based on a specific file.
     * @param filename
     * @return
     */
    public static BufferedReader openFileTest(String filename){
        try{
            FileInputStream fs = new FileInputStream(filename);
            DataInputStream in = new DataInputStream(fs);
            return new BufferedReader(new InputStreamReader(in));
        }catch(IOException ioe){
            return null;
        }
    }


    /**
     * Open a web resource
     * @param url the url to open
     */
    public static BufferedReader openStringUrl(String url){
    	URL toOpen;
		try {
			toOpen = new URL(url);
			return MrProj.openURL(toOpen);
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		}
    	
    }
    
    /**
     * Open URL.
     * @param url
     * @return buffered reader for that URL.
     */
    public static BufferedReader openURL(URL url){
    	try {
			return new BufferedReader(new InputStreamReader(url.openStream()));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
    }
    
    /**
     * Get a global integer pair from a line number.
     * @param line
     * @return the global integer pair.
     */
    public static IntegerPair getIndices(int line){
		int futureRowOrColumn = (int) Math.floor(Math.sqrt(line));
		int closestLowerPerfectSquare = (int) Math.pow(futureRowOrColumn, 2);
		int offset = line - closestLowerPerfectSquare;
		int i,j;
		if ((offset % 2) == 0){
			i = futureRowOrColumn;
			j = offset / 2;
		}else{
			i = (offset - 1) / 2;
			j = futureRowOrColumn;
		}
		return new IntegerPair(i, j);
    }
    
    
    /**
     * Get the column groups, numbered from 0
     * Chet: I'm pretty sure this works, assuming getIndices works!
     * 
     * Example: colwidth=3, N=10
     * |0 1|2|3 4|5|6 7|8|9|
     *  -0---     ---2---
     *      ---1---     -3-
     *
     * @param id the index id
     * @param columnWidth width of a column group, in columns.
     * @param N the width of the matrix
     * @return the column group indices
     */
	public static int[] getColumnGroupNbrsFromLine(int line, int columnWidth, int N){
		IntegerPair indices = MrProj.getIndices(line);
		// Calling float in quotient is useful to avoid pre-rounding
		int firstgroup = (int) Math.max(0, Math.floor( (float) indices.getJ() / (columnWidth-1) - 0.000001 ));
		//int firstgroup = Math.max(0, Math.round(((float) indices.getJ()) / ((float) (columnWidth - 1)) - 1) );
		if (indices.getJ() % (columnWidth-1) == 0 && indices.getJ() != 0 && indices.getJ() != N-1){
			int[] ret = new int[2];
			ret[0] = firstgroup;
			ret[1] = firstgroup+1;
			return ret;
		}else{
			int[] ret = new int[1];
			ret[0] = firstgroup;
			return ret;
		}
	}
	
	public static int getIdFromLine(int line, int N){
		IntegerPair indices = MrProj.getIndices(line);
		return indices.getI()+N*indices.getJ();
	}
	
	public static int getIdInColumnGroupFromId(int id, int columnGroupNbr, int columnWidth, int N){
		return id - (columnWidth-1) * N * columnGroupNbr;
	}
	
	public static int getIdInColumnGroupFromLine(int line, int columnGroupNbr, int columnWidth, int N){
		return getIdFromLine(line, N) - (columnWidth-1) * N * columnGroupNbr;
	}
	
	/* Not tested */
	public static int getGlobalFromIdInColumnGroup(int idInColumnGp, int columnNbr, int columnWidth, int N) {
		return idInColumnGp + columnNbr * columnWidth * N;
	}

	
	/**
	 * Is in boundary column with respect to local index.

	 * @param localIndex
	 * @param columnWidth
	 * @param N size of one side
	 * @return whether or not the item at the index is in a boundary column
	 */
	public static boolean isInBoundaryColumnLocal(int localIndex, int columnWidth, int N){
		if(localIndex < N){
			return true; // is on the left edge
		}else if((localIndex / N)== (columnWidth-1)){
			return true; // is on the right edge
		}else{
			return false;//somewhere in the middle in no mans land
		}
	}
	
	
}
