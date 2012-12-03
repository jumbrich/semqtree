package ie.deri.urq.realidy.bench.utils;

import java.text.NumberFormat;

public class Formater {
    private static String _format = "%02d";
    private static NumberFormat fmt = NumberFormat.getInstance();
    static {
	fmt.setMaximumFractionDigits(2);
	fmt.setMinimumFractionDigits(2);
    }
    
    public static String getLine(String msg, int width){
	int length = msg.trim().length();
	StringBuffer sb = new StringBuffer(width+msg.length());
	for(int i = length; i < width; i++){
	    sb.append(" ");
	}
	sb.append(msg.trim());
	return sb.toString();
    }

    public static String getMemory(long mem) {
	float KBytes = mem/(float)1024;
	float MBytes = KBytes/(float)1024;
	float GBytes = MBytes/(float)1024;

	if(GBytes>=1)
	    return fmt.format(GBytes)+" GB";
	else if(MBytes >=1){
	    return fmt.format(MBytes)+" MB";
	}
	else return fmt.format(KBytes)+" KB";
    }

    
    private static long hourInMs = 1000 * 60 * 60 ;
    private static long minInMs = 1000 * 60;
    private static long ms = 1000;
    
    public static String readableTime( long aMillis ) {
	long hours = ( aMillis  / ( hourInMs) );
	long rest = aMillis  % ( hourInMs );
	long minutes = ( rest / ( minInMs ) );
	rest = rest % ( minInMs );
	long seconds = ( rest / ms );
	long millis = ( rest % ms );

	return String.format( _format, hours ) + ":" + String.format( _format, minutes ) + ":" + String.format( _format, seconds ) + "." + String.format( "%03d", millis );
    }
}
