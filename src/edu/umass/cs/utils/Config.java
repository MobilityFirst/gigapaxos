package edu.umass.cs.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author arun
 *
 */
public class Config extends Properties {
	private static final Logger log = Logger.getLogger(Config.class.getName());

	/**
	 * 
	 */
	private static final long serialVersionUID = 4637861931101543278L;

	/**
	 *
	 */
	public static interface DefaultValueEnum {
		/**
		 * @return Default value of this object.
		 */
		public Object getDefaultValue();
	};

	/**
	 *
	 */
	public static interface ConfigurableEnum extends DefaultValueEnum {
		/**
		 * @return The default properties file for this enum.
		 */
		public String getDefaultConfigFile();

		/**
		 * @return The JVM property key used to specify a non-default config
		 *         file using -Dkey=/path/to/config/file.
		 */
		public String getConfigFileKey();
	}

	/**
	 * Used to disallow some parameters from being configured during production
	 * runs but allow them to be configured for testing or instrumentation
	 * purposes.
	 */
	public static interface Disableable {
		/**
		 * @return True if this parameter can not be changed during runtime.
		 */
		public boolean isDisabled();
	}

	/**
	 * An interface that makes it convenient to use commons-CLI style option
	 * while defining all fields as an enum at one place.
	 * 
	 */
	public static interface CLIOption extends DefaultValueEnum {

		/**
		 * @return String name of the option.
		 */
		public String getOptionName();

		/**
		 * @return String argument name if it takes one.
		 */
		public String getArgName();

		/**
		 * @return True if it takes an argument.
		 */
		public boolean hasArg();

		/**
		 * @return Description of the option.
		 */
		public String getDescriptio();
	};

	/**
	 * Returns a {@link Config} object with properties read from
	 * {@code configFile}
	 * 
	 * @param configFile
	 * @throws IOException
	 */
	Config(String configFile) throws IOException {
		this.readConfigFile(configFile);
	}

	Config() {
	}

	private static final HashMap<Class<?>, Config> configMap = new HashMap<Class<?>, Config>();

	private static final HashMap<Class<?>, Boolean> disableCommandLine = new HashMap<Class<?>, Boolean>();

	private static final HashMap<Object, Object> cacheMap = new HashMap<Object, Object>();

	private static boolean isDefaultValueEnum(Class<?> clazz) {
		for (Class<?> iface : clazz.getInterfaces())
			if (iface.equals(DefaultValueEnum.class))
				return true;
		return false;
	}

	/**
	 * Will first look for the file name specified in systemProperty and then
	 * try the configFile.
	 * 
	 * @param type
	 * @param defaultConfigFile
	 * @param systemPropertyKey
	 * @return Registered config object for this type.
	 * @throws IOException
	 */
	public static Config register(Class<?> type, String systemPropertyKey,
			String defaultConfigFile) throws IOException {
		if (!type.isEnum() && !isDefaultValueEnum(type))
			return null;
		// one time
		if (Config.getConfig(type) != null)
			return Config.getConfig(type);
		// else
		String configFile = System.getProperty(systemPropertyKey) != null ? System
				.getProperty(systemPropertyKey) : defaultConfigFile;
		try {
			Config config = null;
			configMap.put(type, config = new Config(configFile));
			return config;
		} catch (IOException ioe) {
			// we still use defaults
			configMap.put(type, new Config());
			log.warning(Config.class.getSimpleName() + " unable to find file "
					+ configFile + "; using default values for type " + type);
			throw ioe;
		}
	}

	private static HashMap<Object, Object> cmdLine = new HashMap<Object, Object>();

	/**
	 * For registering command-line args.
	 * 
	 * @param args
	 * @return Config object with updated config parameters.
	 */
	public static HashMap<?, ?> register(String[] args) {
		for (String arg : args) {
			if (!arg.contains("="))
				continue;
			if (arg.startsWith("-"))
				arg = arg.replaceFirst("-", "");
			String[] tokens = arg.split("=");
			if (tokens.length == 2) {
				cmdLine.put(tokens[0], tokens[1]);
				/* remove cacheMap entry so that it can get repopulated with the
				 * new value. */
				for (Object key : cacheMap.keySet().toArray())
					if (key.toString().equals(tokens[0]))
						cacheMap.remove(key);
			}
		}
		return cmdLine;
	}

	/**
	 * @param type
	 * @return Config object registered for type.
	 */
	public static Config getConfig(Class<?> type) {
		return configMap.get(type);
	}

	/**
	 * @param type
	 * @param systemPropertyKey
	 * @param defaultConfigFile
	 * @return Properties object created afresh and is case-sensitive.
	 * @throws IOException
	 */
	public static Properties getProperties(Class<?> type,
			String systemPropertyKey, String defaultConfigFile)
			throws IOException {
		String configFile = null;
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(configFile = (System
					.getProperty(systemPropertyKey) != null ? System
					.getProperty(systemPropertyKey) : defaultConfigFile)));
		} catch (IOException ioe) {
			log.warning(Config.class.getSimpleName() + " unable to find file "
					+ configFile + "; using default values for type " + type);
			throw ioe;
		}
		return properties;
	}

	/**
	 * @return {@code this}
	 */
	public Config setSystemProperties() {
		String key = null;
		for (String prop : this.stringPropertyNames())
			if (prop.startsWith("-D")
					&& System.getProperty(key = prop.replaceFirst("-D", "")) == null)
				System.setProperty(key, this.getProperty(prop));
		return this;
	}

	/**
	 * @param field
	 * @return The configuration value of field if its type was previously
	 *         registered. If it was not previously registered, an exception
	 *         will be thrown.
	 */
	public static Object getGlobal(Enum<?> field) {
		// if disabled, return default value
		if (field instanceof Config.Disableable
				&& ((Config.Disableable) field).isDisabled()
				&& field instanceof DefaultValueEnum)
			return ((Config.DefaultValueEnum) field).getDefaultValue();
		
		Object retval;
		if ((retval = cacheMap.get(field)) != null) {
			return retval;
		}

		Class<?> clazz = field.getDeclaringClass();
		String strField = field.toString();
		/* command-line takes precedence over default values unless explicitly
		 * told to completely ignore command-line input. */
		if ((retval = cmdLine.get(strField)) != null
				&& (!disableCommandLine.containsKey(clazz) || disableCommandLine
						.get(clazz))) {
			return retval;
		}

		// else
		Config config = configMap.get(clazz);
		if (config != null && (retval = config.get(field)) != null) {
			return retval;
		}
		// auto-register and recursively invoke
		else if (field instanceof Config.ConfigurableEnum && config == null) {
			try {
				Config.register(clazz,
						((ConfigurableEnum) field).getConfigFileKey(),
						((ConfigurableEnum) field).getDefaultConfigFile());
				return getGlobal(field);
			} catch (IOException e) {
				configMap.put(clazz, null);
				return ((Config.ConfigurableEnum) field).getDefaultValue();
			}
		} else if (field instanceof DefaultValueEnum)
			return putObject(field,
					((DefaultValueEnum) field).getDefaultValue());

		throw new RuntimeException("No matching "
				+ Config.class.getSimpleName() + " registered for field "
				+ field + " and/or " + field + " not instance of "
				+ DefaultValueEnum.class.getSimpleName());
	}

	private static Object putObject(Enum<?> field, Class<?> clazz, Object obj) {
		if (clazz.isInstance(obj))
			return putObject(field, obj);
		return obj;
	}

	private static Object putObject(Enum<?> field, Object obj) {
		if (!cacheMap.containsKey(field))
			cacheMap.put(field, obj);
		return obj;
	}

	/**
	 * @param field
	 * @return Boolean config parameter.
	 */
	public static boolean getGlobalBoolean(Enum<?> field) {
		Object obj = getGlobal(field);
		return (boolean) (obj instanceof Boolean ? obj : putObject(field,
				Boolean.class, (Boolean.valueOf(obj.toString().trim()))));
	}

	/**
	 * @param field
	 * @return Integer config parameter.
	 */
	public static int getGlobalInt(Enum<?> field) {
		Object obj = getGlobal(field);
		return (int) (obj instanceof Integer ? obj : putObject(field,
				Integer.class,
				Integer.valueOf(getGlobal(field).toString().trim())));
	}

	/**
	 * @param field
	 * @return Long config parameter.
	 */
	public static long getGlobalLong(Enum<?> field) {
		Object obj = getGlobal(field);
		return (long) (obj instanceof Long ? obj : putObject(field, Long.class,
				Long.valueOf(getGlobal(field).toString().trim())));
	}

	/**
	 * @param field
	 * @return Double config parameter.
	 */
	public static double getGlobalDouble(Enum<?> field) {
		Object obj = getGlobal(field);
		return (double) (obj instanceof Double ? obj : putObject(field,
				Double.class,
				Double.valueOf(getGlobal(field).toString().trim())));
	}

	/**
	 * @param field
	 * @return Short config parameter.
	 */
	public static short getGlobalShort(Enum<?> field) {
		Object obj = getGlobal(field);
		return (short) (obj instanceof Short ? obj : putObject(field,
				Short.class, Short.valueOf(getGlobal(field).toString().trim())));
	}

	/**
	 * @param field
	 * @return String config parameter.
	 */
	public static String getGlobalString(Enum<?> field) {
		return (String) putObject(field, (getGlobal(field).toString()));
	}

	/**
	 * @param field
	 * @return The configuration value of {@code field}.
	 */
	public Object get(Enum<?> field) {
		String strField = field.toString();
		Object retval;
		if ((retval = this.get(strField)) != null)
			return retval;
		else
			return ((Config.DefaultValueEnum) field).getDefaultValue();
	}

	/**
	 * @param field
	 * @return Boolean value of field.
	 */
	public boolean getBoolean(Enum<?> field) {
		return Boolean.valueOf(get(field).toString());
	}

	/**
	 * @param field
	 * @return Integer value of field.
	 */
	public int getInt(Enum<?> field) {
		return Integer.valueOf(get(field).toString());
	}

	/**
	 * @param field
	 * @return Long value of field.
	 */
	public long getLong(Enum<?> field) {
		return Long.valueOf(get(field).toString());
	}

	/**
	 * @param field
	 * @return Long value of field.
	 */
	public short getShort(Enum<?> field) {
		return Short.valueOf(get(field).toString());
	}

	/**
	 * @param field
	 * @return Long value of field.
	 */
	public String getString(Enum<?> field) {
		return (get(field).toString());
	}

	private void readConfigFile(String configFile) throws IOException {
		InputStream is = new FileInputStream(configFile);
		this.load(is);
		for (Object prop : this.keySet()) {
			log.log(Level.FINE, "Set property {0}={1}", new Object[] { prop,
					this.getProperty(prop.toString()) });
		}
	}

	private static enum Fields implements Config.DefaultValueEnum {
		FIRST(1), SECOND("Monday"), THIRD(30000000000L);

		final Object value;

		Fields(Object value) {
			this.value = value;
		}

		public Object getDefaultValue() {
			return value;
		}

	};

	private static void testMethod1() {
		System.out.println("Default value of " + Fields.THIRD + " is "
				+ Fields.THIRD.getDefaultValue()
				+ " and the configured value is "
				+ Config.getGlobal(Fields.THIRD));
	}

	private static void testMethod2() throws IOException {
		assert (Fields.SECOND.getDefaultValue().equals("Monday"));
		Config config = new Config(
				"/Users/arun/GNS/src/edu/umass/cs/utils/config.properties");
		System.out.println(Fields.class);

		// asserting global config matches specific config
		for (Fields field : Fields.values())
			assert (Config.getGlobal(field).equals(config.get(field)));

		// specific field value assertions
		assert (Fields.FIRST.getDefaultValue().toString().equals(Config
				.getGlobal(Fields.FIRST)));
		assert (Fields.SECOND.getDefaultValue().toString().toUpperCase()
				.equals(config.get(Fields.SECOND)));
		assert (!Fields.THIRD.getDefaultValue().toString()
				.equals(config.get(Fields.THIRD)));
	}

	/**
	 * Example of defining an enum that can be used to define commons-cli
	 * options.
	 * 
	 */
	static enum OptionFields implements CLIOption {
		// not optionable
		FIRST(23),

		// optionable without an argName
		SECOND(Boolean.TRUE, "secondFlag", true, "a flag that determines blah"),

		// optionable with an argName
		THIRD("THREE", "thirdOption", false, "the third rock", "file"),

		// optionable with argName=value
		NINTH("9", "ninthOption", false, "ninth option used for all else",
				"file", true);

		final Object defaultValue;
		final String optionName;
		final boolean hasArg;
		final String description;
		final String argName;
		final boolean withValueSeparator;

		OptionFields(Object defaultValue) {
			this(defaultValue, null, false, null, null);
		}

		OptionFields(Object defaultValue, String optionName, boolean hasArg,
				String description) {
			this(defaultValue, optionName, hasArg, description, null);
		}

		OptionFields(Object defaultValue, String optionName, boolean hasArg,
				String description, String argName) {
			this(defaultValue, optionName, hasArg, description, argName, false);
		}

		OptionFields(Object defaultValue, String optionName, boolean hasArg,
				String description, String argName, boolean withValueSeparator) {
			this.defaultValue = defaultValue;
			this.optionName = optionName;
			this.hasArg = hasArg;
			this.description = description;
			this.argName = argName;
			this.withValueSeparator = withValueSeparator;
		}

		@Override
		public String getOptionName() {
			return this.optionName;
		}

		@Override
		public Object getDefaultValue() {
			return this.defaultValue;
		}

		@Override
		public String getArgName() {
			return this.argName;
		}

		@Override
		public boolean hasArg() {
			return this.hasArg;
		}

		@Override
		public String getDescriptio() {
			return this.description;
		}
	};

	/**
	 * Disable command-line input for type.
	 * 
	 * @param type
	 * @return Config object for type.
	 */
	public static Config disableCommandLine(Class<?> type) {
		disableCommandLine.put(type, true);
		return configMap.get(type);
	}

	private static boolean caseSensitive = false;

	/**
	 * Makes Config keys case-sensitive if {@code b} is true.
	 * 
	 * @param b
	 */
	public static void setCaseSensitive(boolean b) {
		caseSensitive = b;
	}

	private Object toLowerCase(Object key) {
		return caseSensitive || !(key instanceof String)
				|| ((String) key).startsWith("-D") ? key : ((String) key)
				.toLowerCase();
	}

	/**
	 * The methods {@link #put(Object, Object)}, {@link #getProperty(String)},
	 * and {@link #getProperty(String, String)} below make this Config
	 * case-insensitive.
	 */
	public Object put(Object key, Object value) {
		return super.put(toLowerCase(key), value);
	}

	public String getProperty(String key) {
		return super.getProperty((String) toLowerCase(key));
	}

	public Object get(Object key) {
		return super.get(toLowerCase(key));
	}

	public String getProperty(String key, String defaultValue) {
		return super.getProperty((String) toLowerCase(key), defaultValue);
	}

	/**
	 * @param key
	 * @return True of key present.
	 */
	public boolean containsKey(String key) {
		return super.containsKey(toLowerCase(key));
	}

	/**
	 * @return Local logger.
	 */
	public static Logger getLogger() {
		return log;
	}

	/**
	 * Runtime usage of Config through explicit object instance creation.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Util.assertAssertionsEnabled();
		Config.register(Fields.class, "systemPropertyKey",
				"/Users/arun/GNS/src/edu/umass/cs/utils/config.properties");
		testMethod1(); // static usage option
		testMethod2(); // object instance option

		System.out.println("[success]");
	}
}
