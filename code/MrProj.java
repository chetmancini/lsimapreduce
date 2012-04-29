import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

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

    public static float fromNetID = 0.974f;
    public static float desiredDensity = 0.59f;
    public static float wMin = (float) (0.4 * fromNetID);
    public static float wLimit = wMin + desiredDensity;

    public static int N;


    /**
     * Return if a given G divides N as described in the
     * connected components algorithm.
     */
    public static boolean dividesN(int N, int g){
        return (N % g) == 0;
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
    public static int getIndex(int i, int j){
        return i + (j * N);
    }

    /**
     * Get the i component of an index.
     */
    public static int getI(int index){
        return index % N;
    }

    /**
     * Get j component of an index
     */
    public static int getJ(int index){
        return (int) index / N;
    }


    /**
     * Parse a line from a file.
     * @param line
     * @return
     */
    public static float parseLine(String line){
        return Float.parseFloat(line.replace("\n", ""));
    }

    /**
     * Get a buffered reader based on a specific file.
     * @param filename
     * @return
     */
    private BufferedReader openFileTest(String filename){
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
     */
    private BufferedReader openUrl(String url){
        try{
            URL toOpen = new URL(url);
            return new BufferedReader(new InputStreamReader(toOpen.openStream()));
        }catch(Exception e){
            return null;
        }
    }

}