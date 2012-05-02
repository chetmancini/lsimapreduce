package unused;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.net.URL;

import lsimr.src.main.java.org.lsi.containers.BitMatrix;

/**
 * Map Reduce Project.
 */
public class ProjFileReader{

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

    private BitMatrix bitmatrix;

    private int N;
    
    /**
     * Constructor
     */
    public ProjFileReader(){
        this.N = this.matrixSizeN();
        this.bitmatrix = new BitMatrix(N);
    }

    /**
     * Return if a given G divides N as described in the
     * connected components algorithm.
     */
    public boolean dividesN(int g){
        return (this.N % g) == 0;
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
     * Get column groups.
     */
    public List<ColumnGroup> getColumnGroups(int groupCount){
        ArrayList<ColumnGroup> groups = new ArrayList<ColumnGroup>();
        int size = this.N / groupCount;
        if(!dividesN(groupCount)){
            size++;
        }
        int i = 0;
        while(i<this.N){
            ColumnGroup next;
            if(i==0){
                next = new ColumnGroup(i, i+size); 
            }else{
                next = new ColumnGroup(i-1, i+size);
            }
            groups.add(next);
            i+=size;
        }
        ColumnGroup fin = new ColumnGroup(i-1, this.N);
        groups.add(fin);

        return groups;
    }

    public boolean isBoundaryColumn(int colIndex, int groupCount){
        for(ColumnGroup g : this.getColumnGroups(groupCount)){
            if(g.isBoundaryColumn(colIndex)){
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get a buffered reader based on a specific file.
     * @param filename
     * @return
     */
    private void openFileTest(String filename){
        try{
            FileInputStream fs = new FileInputStream(filename);
            DataInputStream in = new DataInputStream(fs);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            this.reader = br;
        }catch(IOException ioe){
            this.reader = null;
        }
    }

    /**
     * Open a web resource
     */
    private void openUrl(String url){
        try{
            URL toOpen = new URL(url);
            BufferedReader br = new BufferedReader(new InputStreamReader(toOpen.openStream()));
            this.reader = br;
        }catch(Exception e){
            this.reader = null;
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
                bitmatrix.put(i, j, getNextFilteredInput());
                bitmatrix.put(j, i, getNextFilteredInput());
            }
            bitmatrix.put(j, j, getNextFilteredInput());
        }
    }


}


