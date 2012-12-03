package ie.deri.urq.realidy.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.eclipse.jetty.webapp.WebAppContext;

public class Server extends CLIObject {

	@Override
	protected void addOptions(Options opts) {
		// TODO Auto-generated method stub

	}

	@Override
	protected void execute(CommandLine cmd) {
		org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(8080);
		String PATH_TO_WAR = "./dist/semqtree.war";
		WebAppContext webapp = new WebAppContext();
		webapp.setContextPath("/");
		webapp.setWar(PATH_TO_WAR);
		server.setHandler(webapp);
	 
		try {
			server.start();
			server.join();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String getDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
	public static void main(String[] args) throws Exception {
		Server s = new Server();
//		s.run(new String[]{});
		org.eclipse.jetty.server.Server server = new org.eclipse.jetty.server.Server(8080);
		 
        WebAppContext context = new WebAppContext();
        context.setDescriptor("webapp/web.xml");
        context.setResourceBase("webapp/");
        context.setContextPath("/");
        context.setParentLoaderPriority(true);
 
        server.setHandler(context);
 
        server.start();
        server.join();
	}
}
