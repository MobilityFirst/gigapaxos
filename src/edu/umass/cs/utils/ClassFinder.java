package edu.umass.cs.utils;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @author arun
 * 
 *         Borrowed this code from stackoverflow:
 *         http://stackoverflow.com/questions/
 *         15519626/how-to-get-all-classes-names-in-a-package
 *
 */
public class ClassFinder {

	private static final char PKG_SEPARATOR = '.';

	private static final char DIR_SEPARATOR = '/';

	private static final String CLASS_FILE_SUFFIX = ".class";

	private static final String BAD_PACKAGE_ERROR = "Unable to get resources from path '%s'. Are you sure the package '%s' exists?";

	/**
	 * @param scannedPackage
	 * @return All classes in {@scannedPackage}
	 */
	public static List<Class<?>> find(String scannedPackage) {
		String scannedPath = scannedPackage.replace(PKG_SEPARATOR,
				DIR_SEPARATOR);
		URL scannedUrl = Thread.currentThread().getContextClassLoader()
				.getResource(scannedPath);
		if (scannedUrl == null) {
			throw new IllegalArgumentException(String.format(BAD_PACKAGE_ERROR,
					scannedPath, scannedPackage));
		}
		File scannedDir = new File(scannedUrl.getFile());
		List<Class<?>> classes = new ArrayList<Class<?>>();
		for (File file : scannedDir.listFiles()) {
			classes.addAll(find(file, scannedPackage));
		}
		return classes;
	}

	/**
	 * @param regex
	 * @return All directories that match regex
	 */
	public static List<Class<?>> findRegex(String regex) {
		String classpath = DefaultTest.class.getClassLoader().getResource(".")
				.toString().replace("file:", "");
		ArrayList<String> packages = Util.recursiveFind(classpath, regex);
		ArrayList<Class<?>> classes = new ArrayList<Class<?>>();
		for (String pkg : packages)
			if (new File(pkg).isDirectory())
				classes.addAll(find(pkg.replace(classpath, "").replace(
						DIR_SEPARATOR, PKG_SEPARATOR)));
		return classes;
	}

	/**
	 * @param clazz
	 * @return Classes other than {@code clazz} in {@code clazz}'s package.
	 */
	public static List<Class<?>> findRest(Class<?> clazz) {
		List<Class<?>> classes = find(clazz.getCanonicalName().replace(
				"." + clazz.getSimpleName(), ""));
		classes.remove(clazz);
		return classes;
	}

	private static List<Class<?>> find(File file, String scannedPackage) {
		List<Class<?>> classes = new ArrayList<Class<?>>();
		String resource = scannedPackage + PKG_SEPARATOR + file.getName();
		if (file.isDirectory()) {
			for (File child : file.listFiles()) {
				classes.addAll(find(child, resource));
			}
		} else if (resource.endsWith(CLASS_FILE_SUFFIX)) {
			int endIndex = resource.length() - CLASS_FILE_SUFFIX.length();
			String className = resource.substring(0, endIndex);
			try {
				classes.add(Class.forName(className));
			} catch (ClassNotFoundException ignore) {
			}
		}
		return classes;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out
				.println(ClassFinder.find("edu.umass.cs.gigapaxos.paxosutil"));
	}
}