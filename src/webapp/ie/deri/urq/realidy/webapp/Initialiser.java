package ie.deri.urq.realidy.webapp;

import java.io.File;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class Initialiser implements ServletContextListener{
    private final static Logger log = Logger.getLogger(Initialiser.class.getSimpleName());
    public static final String INDICES = "i";
    
    public void contextDestroyed(ServletContextEvent arg0) {
	
    }

    public void contextInitialized(ServletContextEvent arg0) {
	File diskLocation = new File(arg0.getServletContext().getInitParameter("DATA_DIR"));
    }

}
