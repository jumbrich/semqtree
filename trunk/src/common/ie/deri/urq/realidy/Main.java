package ie.deri.urq.realidy;

import ie.deri.urq.realidy.cli.CLIObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import org.slf4j.LoggerFactory;

public class Main {
	private final static org.slf4j.Logger log = LoggerFactory.getLogger(Main.class);
	private static final String PREFIX = "ie.deri.urq.realidy.cli";
	public static void main(String[] args) throws ClassNotFoundException, IOException {
		try {
			if (args.length < 1) {
				StringBuffer sb = new StringBuffer();
				sb.append("where <util> one of");
				Class [] classes = getClasses(PREFIX);
				log.info("Found "+classes.length+" cli objects");
				for(Class c: classes){
					log.info("try to load {}",c.getName());
					if(!c.getSimpleName().equals("CLIObject")){
						CLIObject o = (CLIObject) Main.class.getClassLoader().loadClass(PREFIX+"."+c.getSimpleName()).newInstance();
						sb.append("\n\t").append(o.getCommand()).append(" -- ").append(o.getDescription());
					}
				}
				usage(sb.toString());
			}
			CLIObject cli = (CLIObject)Class.forName(PREFIX + "."+args[0]).newInstance();
			cli.run(Arrays.copyOfRange(args, 1, args.length));
			
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	private static void usage(String msg) {
		System.err.println(msg);
		System.exit(-1);
	}
	private static Class[] getClasses(String packageName) throws ClassNotFoundException, IOException {
		String uri;
		ArrayList<Class> classes = new ArrayList<Class>();
		try {
			uri = Main.class.getResource("/ie/deri/urq/realidy/cli").toURI().toASCIIString();
			if(uri.startsWith("jar:file:")){
				classes = classesFromJar(uri);
			}
			else{
				System.out.println("here");
				ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
				String path = packageName.replace('.', '/');
				Enumeration<URL> resources = classLoader.getResources(path);
				List<File> dirs = new ArrayList<File>();
				while (resources.hasMoreElements()) {
					URL resource = resources.nextElement();
					dirs.add(new File(resource.getFile()));
				}
				
				for (File directory : dirs) {
					classes.addAll(findClasses(directory, packageName));
				}
			}
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
//		
//		
		return classes.toArray(new Class[classes.size()]);
	}
	private static ArrayList<Class> classesFromJar(String uri) throws FileNotFoundException, IOException, ClassNotFoundException {
		ArrayList<Class> classes = new ArrayList<Class>();
		String jarURI = uri.substring("jar:file:".length(),uri.lastIndexOf("!"));
		JarInputStream jarFile = new JarInputStream(new FileInputStream(jarURI));
        JarEntry jarEntry;
        while (true) {
            jarEntry = jarFile.getNextJarEntry();
//            System.out.println(jarEntry);
            if (jarEntry == null) {
                break;
            }
            if ((jarEntry.getName().startsWith(PREFIX.replace(".", "/"))) &&
                    (jarEntry.getName().endsWith(".class"))) {
                String classEntry = jarEntry.getName().replaceAll("/", "\\.");
               classes.add(Class.forName(classEntry.substring(0, classEntry.indexOf(".class"))));
            }
        }
        return classes;
	}
	private static List<Class> findClasses(File directory, String packageName) throws ClassNotFoundException {
		List<Class> classes = new ArrayList<Class>();
		if (!directory.exists()) {
			return classes;
		}
		File[] files = directory.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				assert !file.getName().contains(".");
				classes.addAll(findClasses(file, packageName + "." + file.getName()));
			} else if (file.getName().endsWith(".class")) {
				classes.add(Class.forName(packageName + '.' + file.getName().substring(0, file.getName().length() - 6)));
			}
		}
		return classes;
	}
}
