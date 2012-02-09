// $Id$

package net.sf.persist;

import java.lang.reflect.Method;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Mapping {

	public abstract Method getGetterForColumn(String columnName);

	public abstract Method getSetterForColumn(String columnName);

	// ---------- utility methods ----------

	/**
	 * Factory method to create a Mapping based on a Class. Will return a
	 * NoTableAnnotation if the class has a NoTable annotation set, or
	 * TableAnnotation otherwise.
    * Will not use guessed names and will fail if the default table does not exist.
	 */
	public static final Mapping getMapping(final DatabaseMetaData metaData, final Class objectClass,
			final NameGuesser nameGuesser) {
      return getMapping( metaData, objectClass, nameGuesser, null, false );
   }

   /**
    * Factory method to create a Mapping based on a Class.  Will only use the default table names.
    * @param useGuessedNames Specify TRUE if you wish the the mapping to use the guessed names for tables 
    *        and/or columns which do not already exist.
    * @return Will return a NoTableAnnotation if the class has a NoTable annotation set, or
	 *         TableAnnotation otherwise.
    */
   public static final Mapping getMapping(final DatabaseMetaData metaData, final Class objectClass,
			final NameGuesser nameGuesser, final boolean useGuessedNames) {
      return getMapping( metaData, objectClass, nameGuesser, null, useGuessedNames );
   }

   /**
    * Factory method to create a Mapping based on a Class.  Allows you to override the default table name, but will
    * not use guessed names for columns.
    * @param tableName Overrides the default name of the table to use for this class. Useful in case you wish to create
    *        multiple tables from a single class.  (eg, an Address class might be used to create a table for
    *        "mailing_address" and one for "billing_address".
    * @return Will return a NoTableAnnotation if the class has a NoTable annotation set, or
	 *         TableAnnotation otherwise.
    */
   public static final Mapping getMapping(final DatabaseMetaData metaData, final Class objectClass,
			final NameGuesser nameGuesser, final String tableName ) {
      return getMapping( metaData, objectClass, nameGuesser, tableName, false );
   }

   /**
    * Factory method to create a Mapping based on a Class.  Allows you to override the default table name AND used
    * guessed column names.
    * @param tableName Overrides the default name of the table to use for this class. Useful in case you wish to create
    *        multiple tables from a single class.  (eg, an Address class might be used to create a table for
    *        "mailing_address" and one for "billing_address".)  Can be null.
    * @param useGuessedNames Specify TRUE if you wish the the mapping to use the guessed names for tables
    *        and/or columns which do not already exist.
    * @return Will return a NoTableAnnotation if the class has a NoTable annotation set, or
	 *         TableAnnotation otherwise.
    */
   public static final Mapping getMapping(final DatabaseMetaData metaData, final Class objectClass,
			final NameGuesser nameGuesser, final String tableName, final boolean useGuessedNames ) {

//      System.out.println( "Persist-Mapping: getting mapping for [" + objectClass.getName() + "] objects from table [" + tableName + "]" );

		// get @NoTable annotation
		final net.sf.persist.annotations.NoTable noTableAnnotation = (net.sf.persist.annotations.NoTable) objectClass
				.getAnnotation(net.sf.persist.annotations.NoTable.class);

		// if @NoTable is set, build a NoTableAnnotation
		if (noTableAnnotation != null) {
			return new NoTableMapping(objectClass, nameGuesser);
		}

		// otherwise, build a TableAnnotation
		else {
			try {
				return new TableMapping( metaData, objectClass, nameGuesser, tableName, useGuessedNames );
			} catch (SQLException e) {
				throw new PersistException(e);
			}
		}
	}

	/**
	 * Returns an array with maps for annotations, getters and setters. Keys in
	 * each map are field names.
    * @return An array of HashMaps which map field names to particular things.  The maps returned are,
    *         in order, (1) annotations, (2) getter methods, (3) setter methods, and (4) the data type for the field.
	 */
	protected static final Map[] getFieldsMaps(final Class objectClass) {

      if (Log.isDebugEnabled( Log.ENGINE )) {
         Log.debug( Log.ENGINE, "Getting field maps, in class Mapping" );
      }

      final int GETTER = 0;
      final int SETTER = 1;

		final Method[] methods = objectClass.getMethods();

		// create map with all getters and setters
      // use a list for the cases where there are more than one getter or setter
      // we will use this list to match up the "correct" getters and setters later

      if ( Log.isDebugEnabled( Log.ENGINE ) ) {
         Log.debug( Log.ENGINE, "Assembling lists of all getters and setters." );
      }

//		final Map<String, Method[]> allMethods = new HashMap();
      final Map<String, List<Method>[]> allMethods = new HashMap();
      for (Method method : methods) {
			final String name = method.getName();

         if ( Log.isDebugEnabled( Log.ENGINE ) ) {
            Log.debug( Log.ENGINE, "Examining method: " + name );
         }

         // get the field name from the method name; if the method name is invalid, skip this method.
         final String suffix = extractFieldName( name );
         if ( suffix == null ) {
            if ( Log.isDebugEnabled( Log.ENGINE ) ) {
               Log.debug( Log.ENGINE, "Skipping method: " + name + "; Invalid name." );
            }
            continue;
         }

//			final String suffix = name.substring(3); // strip off the leading "get" or "set"

         // Check to see if this datum is already mapped.  If not, initialize this datum in the map.
//			Method[] getterSetter = allMethods.get(suffix);
			List<Method>[] getterSetter = allMethods.get(suffix);
			if (getterSetter == null) {
				getterSetter = new List[2];
            getterSetter[GETTER] = new ArrayList<Method>();
            getterSetter[SETTER] = new ArrayList<Method>();
				allMethods.put(suffix, getterSetter);
			}

//			if (name.startsWith("get")) {
         if (name.startsWith("get") || name.startsWith("is")) {
//            || annotation != null && annotation.getter()
//				getterSetter[GETTER] = method;
            getterSetter[GETTER].add(method);
			} else if (name.startsWith("set")) {
//				getterSetter[SETTER] = method;
            getterSetter[SETTER].add(method);
			}
		}

		// assemble annotations, getters and setters maps
		// a field is only taken into consideration if it has a getter and a
		// setter
      // also create map containing the return type (ie, column data type)
      // for each getter (ie, each column)

		final Map<String, net.sf.persist.annotations.Column> annotationsMap = new HashMap();
		final Map<String, Method> gettersMap = new HashMap();
		final Map<String, Method> settersMap = new HashMap();
        final Map<String, Class> typesMap = new HashMap();

		for (String suffix : allMethods.keySet()) {

         if ( Log.isDebugEnabled( Log.ENGINE ) ) {
            Log.debug( Log.ENGINE, "Winnowing and verifying lists of getters and setters for datum suffixed: " + suffix );
         }

			final List<Method>[] getterSetter = allMethods.get(suffix);

			// only consider fields to have both getters and setters
//         if (getterSetter[GETTER] != null && getterSetter[SETTER] != null) {
//         if (getterSetter[GETTER].size() != 0 && getterSetter[SETTER].size() != 0) {
         if ( getterSetter[GETTER].size() == 0 || getterSetter[SETTER].size() == 0 ) {
            if ( Log.isDebugEnabled( Log.ENGINE ) ) {
               Log.debug( Log.ENGINE, " Found invalid number of getters and setters, require at least one of each." );
               Log.debug( Log.ENGINE, " Found " + getterSetter[GETTER].size() + " getters and " +
                                      getterSetter[SETTER].size() + " setters.  Moving to next datum suffix." );
            }
         }
         else {

				// field name (prefix with first char in lower case)
                // commented out so that we can do it all at once in the extractFieldName method
//				final String fieldName = suffix.substring(0, 1).toLowerCase() + suffix.substring(1);
                final String fieldName = suffix;


            // whittle down the list of getters and setters to a single, matched pair
            ArrayList<Method> temp;

            // first ... getters
            // we make the assumption that "valid" getters should not have parameters
            temp = new ArrayList<Method>();
            for (Method m : getterSetter[GETTER]) {
               if (m.getParameterTypes().length > 0) {
//                  getterSetter[GETTER].remove(m);
                  if (Log.isDebugEnabled( Log.ENGINE )) {
                     Log.debug( Log.ENGINE, " Getter [" + m + "] discarded; has multiple parameters: " + m.getParameterTypes().length );
                  }
                  continue;
               }
               if (m.getReturnType() == void.class) {
//                  getterSetter[GETTER].remove(m);
                  if (Log.isDebugEnabled( Log.ENGINE )) {
                     Log.debug( Log.ENGINE, " Getter [" + m + "] discarded; has void as return parameter." );
                  }
                  continue;
               }
               if ( ! Persist.isNativeType( m.getReturnType())) {
                  if (Log.isDebugEnabled( Log.ENGINE )) {
                     Log.debug( Log.ENGINE, " Getter [" + m + "] discarded; not a supported data type." );
                  }
                  continue;
               }
               temp.add(m);
            }
            getterSetter[GETTER] = temp;

            // now verify that we have only one getter in the list; if not, skip to next "suffix"
            if ( getterSetter[GETTER].size() != 1 ) {
//                  throw new PersistException("After winnowing list of getters, there are none left!");
               if ( Log.isDebugEnabled( Log.ENGINE ) ) {
                  Log.debug( Log.ENGINE, " After winnowing list of getters, there are " + getterSetter[GETTER].size() +
                                         " left! (Should have only one (1).) Moving to next datum suffix." );
               }
               continue;
            }

            // now ... setters
            // Retrieve return type of remaining getter.  All setters will be compared to this.
            Class returnType = getterSetter[GETTER].get( 0 ).getReturnType();

            // we make the assumption that valid setters should only have one parameter and
            // that it must be the same type as that returned by the getter
            temp = new ArrayList<Method>();
            for (Method m : getterSetter[SETTER]) {
               Class[] paramTypes = m.getParameterTypes();
               if (paramTypes.length != 1) {
//                  getterSetter[SETTER].remove(m);
                  if (Log.isDebugEnabled( Log.ENGINE )) {
                     Log.debug( Log.ENGINE, " Setter [" + m + "] discarded; should have one (1) parameter, but has: " +
                                            paramTypes.length );
                  }
                  continue;
               }
               if (paramTypes[0] != returnType) {
//                  getterSetter[SETTER].remove(m);
                  if (Log.isDebugEnabled( Log.ENGINE )) {
                     Log.debug( Log.ENGINE, " Setter [" + m + "] discarded; should take parameter type: " + returnType.toString() +
                                            " but takes: " + paramTypes[0].toString() );
                  }
                  continue;
               }
               temp.add(m);
            }
            getterSetter[SETTER] = temp;
            
            // now verify that we have only one setter in the list; if not, skip to next "suffix"
            if ( getterSetter[SETTER].size() != 1 ) {
               if ( Log.isDebugEnabled( Log.ENGINE ) ) {
                  Log.debug( Log.ENGINE, " After winnowing list of setters, there are " + getterSetter[SETTER].size() +
                                         " left! (Should have only one (1).) Moving to next datum suffix." );
               }
               continue;
            }
            
            if ( Log.isDebugEnabled( Log.ENGINE ) ) {
               Log.debug( Log.ENGINE, " Finished winnowing list of getters and setters:" );
               Log.debug( Log.ENGINE, "  Found getter [ " + getterSetter[GETTER].get(0) +
                                      " ] which returns a [ " + getterSetter[GETTER].get(0).getReturnType() + " ]" );
               Log.debug( Log.ENGINE, "  Found setter [ " + getterSetter[SETTER].get(0) +
                                      " ] which takes a [ " + getterSetter[SETTER].get(0).getParameterTypes()[0] + " ]" );               
               Log.debug( Log.ENGINE, " Verifying methods and annotations." );
            }

            // now we can proceed knowning

				// column annotation
				final net.sf.persist.annotations.Column getterAnnotation = getterSetter[GETTER].get(0)
						.getAnnotation(net.sf.persist.annotations.Column.class);
				final net.sf.persist.annotations.Column setterAnnotation = getterSetter[SETTER].get(0)
						.getAnnotation(net.sf.persist.annotations.Column.class);

				// if NoColumn is specified, don't use the field
				final net.sf.persist.annotations.NoColumn noPersistGetter = getterSetter[GETTER].get(0)
						.getAnnotation(net.sf.persist.annotations.NoColumn.class);

				final net.sf.persist.annotations.NoColumn noPersistSetter = getterSetter[SETTER].get(0)
						.getAnnotation(net.sf.persist.annotations.NoColumn.class);

				// check conflicting NoColumn and Column annotations
				if (noPersistGetter != null || noPersistSetter != null) {
					if (getterAnnotation != null || setterAnnotation != null) {
						throw new PersistException("Field [" + fieldName + "] from class [" + objectClass.getName()
								+ "] has conflicting NoColumn and Column annotations");
					}
					continue;
				}

				// assert that getters and setters have valid and compatible types
            // these have already been checked (as non-fatal errors) in the "winnowing" code above
            // and can probably be removed
				if (getterSetter[SETTER].get(0).getParameterTypes().length != 1) {
					throw new PersistException("Setter [" + getterSetter[SETTER]
							+ "] should have a single parameter but has " + getterSetter[SETTER].get(0).getParameterTypes().length);
				}
				if (getterSetter[GETTER].get(0).getReturnType() == void.class) {
					throw new PersistException("Getter [" + getterSetter[GETTER] + "] must have a return parameter");
				}
				if (getterSetter[GETTER].get(0).getReturnType() != getterSetter[SETTER].get(0).getParameterTypes()[0]) {
					throw new PersistException("Getter [" + getterSetter[GETTER] + "] and setter [" + getterSetter[SETTER]
							+ "] have incompatible types");
				}

				// check for annotations on the getter/setter
				net.sf.persist.annotations.Column annotation = null;

				if (getterAnnotation != null && setterAnnotation != null) {

					// if both getter and setter have annotations, make sure
					// they are equals
					if (!getterAnnotation.equals(setterAnnotation)) {

						final String getterAnn = getterAnnotation.toString().substring(
								getterAnnotation.toString().indexOf('(') + 1,
								getterAnnotation.toString().lastIndexOf(')'));

						final String setterAnn = setterAnnotation.toString().substring(
								setterAnnotation.toString().indexOf('(') + 1,
								setterAnnotation.toString().lastIndexOf(')'));

						throw new PersistException("Annotations for getter [" + getterSetter[GETTER] + "] and setter ["
								+ getterSetter[SETTER] + "] have different annotations [" + getterAnn + "] [" + setterAnn
								+ "]");
					}

					annotation = getterAnnotation;
				} else if (getterAnnotation != null) {
					annotation = getterAnnotation;
				} else if (setterAnnotation != null) {
					annotation = setterAnnotation;
				}

				// make getter and setter accessible
				getterSetter[GETTER].get(0).setAccessible(true);
				getterSetter[SETTER].get(0).setAccessible(true);

				annotationsMap.put(fieldName, annotation);
				gettersMap.put(fieldName, getterSetter[GETTER].get(0));
				settersMap.put(fieldName, getterSetter[SETTER].get(0));
                typesMap.put( fieldName, getterSetter[GETTER].get(0).getReturnType() );

			}

         if ( Log.isDebugEnabled( Log.ENGINE ) ) {
            Log.debug( Log.ENGINE, "Done processing datum suffixed: " + suffix );
         }
		}

      if ( Log.isDebugEnabled( Log.ENGINE ) ) {
       Log.debug( Log.ENGINE, "Recorded types for following suffixes: " + typesMap.keySet().toString() );
    }

		return new Map[] { annotationsMap, gettersMap, settersMap, typesMap };
	}

    /**
     * Utility method to extract a field name from a getter or setter method name.  Basically
     * removes the "is", "get" or "set" prefix from the method name.
     * @param name a method name 
     * @return a field name, or null if method name doesn't match the getter/setter format.
     */
    public static String extractFieldName( String name ) {

        String fieldName;

        if (name.length() > 2 && name.startsWith("is")) {
            fieldName = name.substring( 2 );
        } else if ( name.length() > 3 && ( name.startsWith( "get" ) ||
                                           name.startsWith("set"))) {
            fieldName = name.substring( 3 );
        } else {
            fieldName = null;
        }

        // lowercase the first letter
        if ( fieldName != null )
            return fieldName.substring( 0, 1 ).toLowerCase() + fieldName.substring( 1 );
        else
            return null;

    }

}


