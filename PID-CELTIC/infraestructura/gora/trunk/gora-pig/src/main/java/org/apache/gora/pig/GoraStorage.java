package org.apache.gora.pig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.ArrayUtils;
import org.apache.gora.mapreduce.GoraInputFormat;
import org.apache.gora.mapreduce.GoraInputFormatFactory;
import org.apache.gora.mapreduce.GoraOutputFormat;
import org.apache.gora.mapreduce.GoraOutputFormatFactory;
import org.apache.gora.mapreduce.GoraRecordReader;
import org.apache.gora.mapreduce.GoraRecordWriter;
import org.apache.gora.persistency.impl.PersistentBase;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.gora.util.AvroUtils;
import org.apache.gora.util.GoraException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.Expression;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoraStorage extends LoadFunc implements StoreFuncInterface, LoadMetadata {

  public static final Logger LOG = LoggerFactory.getLogger(GoraStorage.class);

  /**
   * Key in UDFContext properties that marks config is set (set at backend nodes)  
   */
  private static final String GORA_CONFIG_SET = "gorastorage.config.set" ;
  private static final String GORA_STORE_SCHEMA = "gorastorage.pig.store.schema" ;

  protected Job job;
  protected JobConf localJobConf ; 
  protected String udfcSignature = null ;
  
  protected String keyClassName ;
  protected String persistentClassName ;
  protected Class<?> keyClass;
  protected Class<? extends PersistentBase> persistentClass;
  protected Schema persistentSchema ;
  private   DataStore<?, ? extends PersistentBase> dataStore ;
  protected GoraInputFormat<?,? extends PersistentBase> inputFormat ;
  protected GoraRecordReader<?,? extends PersistentBase> reader ;
  protected PigGoraOutputFormat<?,? extends PersistentBase> outputFormat ;
  protected GoraRecordWriter<?,? extends PersistentBase> writer ;
  protected PigSplit split ;
  protected ResourceSchema readResourceSchema ;
  protected ResourceSchema writeResourceSchema ;

  /** Fields splitted from location, indicating what fields to load/save */
  protected List<String> loadSaveFields = new ArrayList<String>() ;

  /** Fields to load as Query - same as {@link loadSaveFields} but without 'key' */
  protected String[] loadQueryFields ;
  
  /** Setted to 'true' if location is '*'. All fields will be loaded into a tuple when reading,
   * and all tuple fields will be copied to the persistent instance when saving. */
  protected boolean loadSaveAllFields = false ;

  /**
   * Creates a new 
   * @param keyClassName
   * @param persistentClassName
   */
  public GoraStorage(String keyClassName, String persistentClassName) {
      this(keyClassName, persistentClassName, "*") ;
  }

  /**
   * Creates a new GoraStorage and set the keyClass from the key class name.
   * @param keyClassName key class. Full name with package (org.apache....)
   * @param persistentClassName persistent class. Full name with package 
   * @param fields comma separated fields to load/save | '*' for all.
   *   '*' loads all fields from the persistent class.
   *   '*' saves all fields of each tuple to persist (not mandatory all fields of the persistent class).
   */
  public GoraStorage(String keyClassName, String persistentClassName, String csvFields) {
    super();
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage constructor() {}", this);
    
    this.keyClassName = keyClassName ;
    this.persistentClassName = persistentClassName ;
    try {
      this.keyClass = Class.forName(keyClassName);
      Class<?> persistentClazz = Class.forName(persistentClassName);
      this.persistentClass = persistentClazz.asSubclass(PersistentBase.class);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    

    // Precompute the set of fields for load and save
    
    // This dirty lines are needed for parse Fields :( We don't have a job configuration
    // to connect to the datastore and do it clean.
    java.lang.reflect.Field schemaAttribute;
    try {
      schemaAttribute = this.persistentClass.getField("_SCHEMA");
      this.persistentSchema = (Schema) schemaAttribute.get(null) ;

      if (csvFields.contains("*")) {
        this.setLoadSaveAllFields(true) ;
        return;
      }
        
      // CSV fields declared in constructor. We will use the intersection of the list declared in constructor
      // and belonging to the Persistent instance.
      String[] fieldsInConstructor = csvFields.split("\\s*,\\s*") ; // splits "field, field, field, field"
      List<String> declaredConstructorFields = new ArrayList<String>(Arrays.asList(fieldsInConstructor)) ;
      
      List<Field> avroFields = this.persistentSchema.getFields() ;
      List<String> avroFieldsNames = new ArrayList<String>() ;
      for(Field f: avroFields) {
        avroFieldsNames.add(f.name()) ;
      }
      declaredConstructorFields.retainAll(avroFields) ;
      this.setLoadQueryFields(declaredConstructorFields.toArray(new String[0])) ;
      
      // Populate the set with the fields that will be load/saved
      this.getLoadSaveFields().add("key") ;
      this.getLoadSaveFields().addAll(declaredConstructorFields) ;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
  }

  /**
   * Returns the internal DataStore for <code>&lt;keyClass,persistentClass&gt;</code>
   * using configuration set in job (from setLocation()).
   * Creates one datastore at first call.
   * @return DataStore for &lt;keyClass,persistentClass&gt;
   * @throws GoraException on DataStore creation error.
   */
  protected DataStore<?, ? extends PersistentBase> getDataStore() throws GoraException {
    if (this.localJobConf == null) {
      throw new GoraException("Calling getDataStore(). setLocation()/setStoreLocation() must be called first!") ;
    }
    if (this.dataStore == null) {
      this.dataStore = DataStoreFactory.getDataStore(this.keyClass, this.persistentClass, this.localJobConf) ;
    }
    return this.dataStore ;
  }
  
  /**
   * Gets the job, initialized the localJobConf (the actual used to create a datastore) and splits from 'location' the fields to load/save
   *  
   */
  @Override
  public void setLocation(String location, Job job) throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage setLocation() {} {}", location, this);
    this.job = job;
    this.localJobConf = this.initializeLocalJobConfig(job) ;
  }

  /**
   * Returns UDFProperties based on <code>udfcSignature</code>, <code>keyClassName</code> and <code>persistentClassName</code>.
   */
  private Properties getUDFProperties() {
      return UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] {this.udfcSignature,this.keyClassName,this.persistentClassName});
  }
  
  private JobConf initializeLocalJobConfig(Job job) {
    Properties udfProps = getUDFProperties();
    Configuration jobConf = job.getConfiguration();
    JobConf localConf = new JobConf(jobConf); // localConf starts as a copy of jobConf
    if (udfProps.containsKey(GORA_CONFIG_SET)) {
      // Already configured (maybe from frontend to backend)
      for (Entry<Object, Object> entry : udfProps.entrySet()) {
        localConf.set((String) entry.getKey(), (String) entry.getValue());
      }
    } else {
      // Not configured. We load to localConf the configuration and put it in udfProps
      Configuration goraConf = new Configuration();
      for (Entry<String, String> entry : goraConf) {
        // JobConf may have some conf overriding ones in hbase-site.xml
        // So only copy hbase config not in job config to UDFContext
        // Also avoids copying core-default.xml and core-site.xml
        // props in hbaseConf to UDFContext which would be redundant.
        if (localConf.get(entry.getKey()) == null) {
          udfProps.setProperty(entry.getKey(), entry.getValue());
          localConf.set(entry.getKey(), entry.getValue());
        }
      }
      udfProps.setProperty(GORA_CONFIG_SET, "true");
    }
    return localConf;
  }
  
  @Override
  @SuppressWarnings({ "rawtypes" })
  public InputFormat getInputFormat() throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage getInputFormat() {}", this);
    this.inputFormat = GoraInputFormatFactory.createInstance(this.keyClass, this.persistentClass);

    Query query = this.getDataStore().newQuery() ;
    if (!this.isLoadSaveAllFields()) {
      query.setFields(this.getLoadQueryFields()) ;
    }
    GoraInputFormat.setInput(this.job, query, false) ;
    
    inputFormat.setConf(this.job.getConfiguration()) ;
    return this.inputFormat ; 
  }

  @Override
  public LoadCaster getLoadCaster() throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage getLoadCaster()", this);
    return null;
    // return new Utf8StorageConverter();
  }

  @Override
  @SuppressWarnings({ "rawtypes" })
  public void prepareToRead(RecordReader reader, PigSplit split)
      throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage prepareToRead {}", this);
    this.reader = (GoraRecordReader<?, ?>) reader;
    this.split = split;
  }

  @Override
  public Tuple getNext() throws IOException {
    LOG.trace("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage getNext() {}", this);

    try {
      if (!this.reader.nextKeyValue()) return null;
    } catch (Exception e) {
      throw new IOException(e);
    }

    PersistentBase persistentObj;
    Object persistentKey ;

    try {
      persistentKey = this.reader.getCurrentKey() ;
      persistentObj = this.reader.getCurrentValue();
    } catch (Exception e) {
      throw new IOException(e);
    }

    if (this.isLoadSaveAllFields()) {
      return persistent2Tuple(persistentKey, persistentObj, persistentObj.getSchema());
    } else {
      return persistent2Tuple(persistentKey, persistentObj, this.loadSaveFields) ;
    }

  }
  
  /**
   * Creates a pig tuple from a PersistentBase instance
   *
   * Internally calls persistentField2PigType(Schema, Object) for each field
   * 
   * @param persistentKey Key of the PersistentBase object
   * @param persistentObj PersistentBase instance
   * @return Tuple with schemafields+1 elements (1<sup>st</sup> element is the row key) 
   * @throws ExecException
   *           On setting tuple field errors
   */
  private static Tuple persistent2Tuple(Object persistentKey, PersistentBase persistentObj, List<String> fields) throws ExecException {
    Tuple tuple = TupleFactory.getInstance().newTuple(fields.size());
    Schema avroSchema = persistentObj.getSchema() ;
    
    int fieldIndex = 0 ;
    for (String fieldName : fields) {
      if ("key".equals(fieldName)) {
        tuple.set(fieldIndex, persistentKey) ;
      } else {
        Schema fieldSchema = avroSchema.getField(fieldName).schema() ;
        Object fieldValue = persistentObj.get(persistentObj.getFieldIndex(fieldName)) ;
        tuple.set(fieldIndex, persistentField2PigType(fieldSchema, fieldValue)) ;
      }
      fieldIndex++ ;
    }
    
    return tuple ;
  }
  
  /**
   * Creates a pig tuple from a PersistentBase instance
   *
   * Internally calls persistentField2PigType(Schema, Object) for each field
   * 
   * @param persistentKey Key of the PersistentBase object
   * @param persistentObj PersistentBase instance
   * @return Tuple with schemafields+1 elements (1<sup>st</sup> element is the row key) 
   * @throws ExecException
   *           On setting tuple field errors
   */
  private static Tuple persistent2Tuple(Object persistentKey, PersistentBase persistentObj, Schema schema) throws ExecException {
    Iterator<Field> fieldsIterator = schema.getFields().iterator();
    Tuple tuple = TupleFactory.getInstance().newTuple(schema.getFields().size() + 1);
    
    // TODO Check the key first?
    tuple.set(0,persistentKey) ;
    
    for (int fieldIndex = 0; fieldsIterator.hasNext(); fieldIndex++) {
      Field schemaField = fieldsIterator.next();
      Object fieldValue = persistentObj.get(fieldIndex) ;

      tuple.set(fieldIndex+1, persistentField2PigType(schemaField.schema(), fieldValue)) ;
    }
    
    return tuple ;
  }

  /**
   * Recursively converts PersistentBase fields to Pig type: Tuple | Bag | String | Long | ...
   * 
   * The mapping is as follows:
   * null         -> null
   * Boolean      -> Boolean
   * Enum         -> Integer
   * ByteBuffer   -> DataByteArray
   * Utf8         -> String
   * Float        -> Float
   * Double       -> Double
   * Integer      -> Integer
   * Long         -> Long
   * Union        -> X
   * Record       -> Tuple
   * Array        -> Bag
   * Map<Utf8,b'> -> HashMap<String,Object>
   * 
   * @param schema Source schema
   * @param data Source data: PersistentBase | String | Long,...
   * @return Pig type: Tuple | Bag | String | Long | ...
   * @throws ExecException 
   */
  private static Object persistentField2PigType(Schema schema, Object data) throws ExecException {
    
    Type schemaType = schema.getType();

    switch (schemaType) {
      case NULL:    return null ;
      case BOOLEAN: return (Boolean)data ; 
      case ENUM:    return new Integer(((Enum<?>)data).ordinal()) ;
      case BYTES:   return new DataByteArray(((ByteBuffer)data).array()) ;
      case STRING:  return ((Utf8)data).toString() ;
        
      case FLOAT:
      case DOUBLE:
      case INT:
      case LONG:    return data ;

      case UNION:
        int unionIndex = GenericData.get().resolveUnion(schema, data) ;
        Schema unionTypeSchema = schema.getTypes().get(unionIndex) ;
        return persistentField2PigType(unionTypeSchema, data) ;

      case RECORD:
        List<Field> recordFields = schema.getFields() ;
        int numRecordElements = recordFields.size() ;
        
        Tuple recordTuple = TupleFactory.getInstance().newTuple(numRecordElements);
        
        for (int i=0; i<numRecordElements ; i++ ) {
          recordTuple.set(i, persistentField2PigType(recordFields.get(i).schema(), ((PersistentBase)data).get(i))) ;
        }
        return recordTuple ;

      case ARRAY:
        GenericArray<?> arr = (GenericArray<?>) data;
        DataBag bag = BagFactory.getInstance().newDefaultBag() ;
        Schema arrValueSchema = schema.getElementType() ;
        for(Object element: arr) {
          Object pigElement = persistentField2PigType(arrValueSchema, element) ;
          if (pigElement instanceof Tuple) {
            bag.add((Tuple)pigElement) ;
          } else {
            Tuple arrElemTuple = TupleFactory.getInstance().newTuple(1) ;
            arrElemTuple.set(0, pigElement) ;
            bag.add(arrElemTuple) ;
          }
        }
        return bag ;

      case MAP:
        @SuppressWarnings("unchecked")
        HashMap<Utf8,?> avroMap = (HashMap<Utf8, ?>) data ;
        // Convert Utf8 avro types to String
        HashMap<String,Object> map = new HashMap<String,Object>() ;
        for (Entry<Utf8,?> e : avroMap.entrySet()) {
          map.put(e.getKey().toString(), persistentField2PigType(schema.getValueType(), e.getValue())) ;
        }
        return map ;

      case FIXED:
        // TODO: Implement FIXED data type
        throw new RuntimeException("Fixed type not implemented") ;

      default:
        LOG.error("Unexpected schema type {}", schemaType) ;
        throw new RuntimeException("Unexpected schema type " + schemaType) ;
    }
  
  }
  
  @Override
  public void setUDFContextSignature(String signature) {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage setUDFContextSignature() {}", this);
    this.udfcSignature = signature;
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) 
      throws IOException {
    // Do nothing
    return location ;
  }
  
  /**
   * Retrieves the Pig Schema from the declared fields in constructor and the Avro Schema
   * Avro Schema must begin with a record.
   * Pig Schema will be a Tuple (in 1st level) with $0 = "key":rowkey
   */
  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage getSchema() {}", this);
    // Reuse if already created
    if (this.readResourceSchema != null) return this.readResourceSchema ;
    
    ResourceFieldSchema[] resourceFieldSchemas = null ;
    
    if (this.isLoadSaveAllFields()) {
      // Fields declared in constructor = "*"
      int numFields = this.persistentSchema.getFields().size() ;
      resourceFieldSchemas = new ResourceFieldSchema[numFields+1] ;
      resourceFieldSchemas[0] = new ResourceFieldSchema().setType(DataType.findType(this.keyClass)).setName("key") ;
      Iterator<Field> fieldsIterator = this.persistentSchema.getFields().iterator();
      for (int fieldIndex = 1; fieldsIterator.hasNext(); fieldIndex++) {
        Field schemaField = fieldsIterator.next();
        resourceFieldSchemas[fieldIndex] = this.avro2ResouceFieldSchema(schemaField.schema()).setName(schemaField.name()) ;
      }
    } else {
      // Fields declared in constructor is a CSV
      int numFields = loadSaveFields.size() ;
      resourceFieldSchemas = new ResourceFieldSchema[numFields] ;
      resourceFieldSchemas[0] = new ResourceFieldSchema().setType(DataType.findType(this.keyClass)).setName("key") ;
      for (int fieldIndex = 1; fieldIndex < numFields ; fieldIndex++) {
        Field schemaField = this.persistentSchema.getField(this.loadQueryFields[fieldIndex-1]) ;
        resourceFieldSchemas[fieldIndex] = this.avro2ResouceFieldSchema(schemaField.schema()).setName(schemaField.name()) ;
      }
    }

    ResourceSchema resourceSchema = new ResourceSchema().setFields(resourceFieldSchemas) ;

    // Save Pig schema inside the instance
    this.readResourceSchema = resourceSchema ;
    
    return resourceSchema ;
  }

  private ResourceFieldSchema avro2ResouceFieldSchema(Schema schema) throws IOException {

    Type schemaType = schema.getType();

    switch (schemaType) {
      case NULL:    return new ResourceFieldSchema().setType(DataType.NULL) ;
      case BOOLEAN: return new ResourceFieldSchema().setType(DataType.BOOLEAN) ; 
      case ENUM:    return new ResourceFieldSchema().setType(DataType.INTEGER) ;
      case BYTES:   return new ResourceFieldSchema().setType(DataType.BYTEARRAY);
      case STRING:  return new ResourceFieldSchema().setType(DataType.CHARARRAY) ;
      case FLOAT:   return new ResourceFieldSchema().setType(DataType.FLOAT) ;
      case DOUBLE:  return new ResourceFieldSchema().setType(DataType.DOUBLE) ;
      case INT:     return new ResourceFieldSchema().setType(DataType.INTEGER) ;
      case LONG:    return new ResourceFieldSchema().setType(DataType.LONG) ;
  
      case UNION:
        // Returns the first not-null type
        if (schema.getTypes().size() != 2) {
          LOG.warn("Field UNION {} must be ['null','othertype']. Maybe wrong definition?") ;
        }
        for (Schema s: schema.getTypes()) {
          if (s.getType() != Type.NULL) return avro2ResouceFieldSchema(s) ;
        }
        throw new RuntimeException("Union with only ['null']?") ;
  
      case RECORD:
        // A record in Gora is a Tuple in Pig
        int numRecordFields = schema.getFields().size() ;
        Iterator<Field> recordFields = schema.getFields().iterator();
        ResourceFieldSchema returnRecordResourceFieldSchema = new ResourceFieldSchema().setType(DataType.TUPLE) ;

        ResourceFieldSchema[] recordFieldSchemas = new ResourceFieldSchema[numRecordFields] ;
        for (int fieldIndex = 0; recordFields.hasNext(); fieldIndex++) {
          Field schemaField = recordFields.next();
          recordFieldSchemas[fieldIndex] = this.avro2ResouceFieldSchema(schemaField.schema()).setName(schemaField.name()) ;
        }
        
        returnRecordResourceFieldSchema.setSchema(new ResourceSchema().setFields(recordFieldSchemas)) ;

        return returnRecordResourceFieldSchema ;
          
      case ARRAY:
        // An array in Gora is a Bag in Pig
        // Maybe should be a Map with string(numeric) index to ensure order, but Avro and Pig data model are different :\
        ResourceFieldSchema returnArrayResourceFieldSchema = new ResourceFieldSchema().setType(DataType.BAG) ;
        Schema arrayElementType = schema.getElementType() ;
        
        returnArrayResourceFieldSchema.setSchema(
            new ResourceSchema().setFields(
                new ResourceFieldSchema[]{
                    new ResourceFieldSchema().setType(DataType.TUPLE).setName("t").setSchema(
                        new ResourceSchema().setFields(
                            new ResourceFieldSchema[]{
                                avro2ResouceFieldSchema(arrayElementType)
                            }
                        )
                    )
                }
            )
        ) ;

        return returnArrayResourceFieldSchema ;
  
      case MAP:
        // A map in Gora is a Map in Pig, but in pig is only chararray=>something
        ResourceFieldSchema returnMapResourceFieldSchema = new ResourceFieldSchema().setType(DataType.MAP) ;
        Schema mapValueType = schema.getValueType();
        
        returnMapResourceFieldSchema.setSchema(
            new ResourceSchema().setFields(
                new ResourceFieldSchema[]{
                    avro2ResouceFieldSchema(mapValueType)
                }
            )
        ) ;
        
        return returnMapResourceFieldSchema ;
  
      case FIXED:
        // TODO Implement FIXED data type
        throw new RuntimeException("Fixed type not implemented") ;
  
      default:
        LOG.error("Unexpected schema type {}", schemaType) ;
        throw new RuntimeException("Unexpected schema type " + schemaType) ;
    }
    
  }
  
  @Override
  public ResourceStatistics getStatistics(String location, Job job)
      throws IOException {
    // TODO Not implemented since Pig does not use it (feb 2013)
    return null;
  }

  @Override
  /**
   * Disabled by now (returns null).
   * Later we will consider only one partition key: row key
   */
  public String[] getPartitionKeys(String location, Job job) throws IOException {
    // TODO Disabled by now
    return null ;
    //return new String[] {"key"} ;
  }

  @Override
  /**
   * Ignored by now since getPartitionKeys() return null
   */
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
    // TODO Ignored since getPartitionsKeys() return null
    throw new IOException() ;
  }

  @Override
  public String relToAbsPathForStoreLocation(String location, Path curDir)
      throws IOException {
    // Do nothing
    return location ;
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public OutputFormat getOutputFormat() throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage getOutputFormat() {}", this);
    try {
      this.outputFormat = GoraOutputFormatFactory.createInstance(PigGoraOutputFormat.class, this.keyClass, this.persistentClass);
    } catch (Exception e) {
      throw new IOException(e) ;
    }
    GoraOutputFormat.setOutput(this.job, this.getDataStore(), false) ;
    this.outputFormat.setConf(this.job.getConfiguration()) ;
    return this.outputFormat ; 
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage setStoreLocation() {}", this) ;
    this.job = job ;
    this.localJobConf = this.initializeLocalJobConfig(job) ;
  }

  @Override
  /**
   * Checks the pig schema using names, using the first element of the tuple as key (fieldname = 'key').
   * (key:key, name:recordfield, name:recordfield, name:recordfi...)
   * 
   * Sets UDFContext property GORA_STORE_SCHEMA with the schema to send it to the backend.
   * 
   * Not present names for recordfields will be treated as null .
   */
  public void checkSchema(ResourceSchema s) throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage checkSchema() {}", this);
    
    // Expected pig schema: tuple (key, recordfield, recordfield, recordfi...)
    ResourceFieldSchema[] pigFieldSchemas = s.getFields();
    
    List<String> pigFiledSchemasNames = new ArrayList<String>(Arrays.asList(s.fieldNames())) ;
    if (pigFiledSchemasNames.containsAll(this.loadSaveFields)) {
      // Saving based on the names of the fields from a CSV

      for (ResourceFieldSchema pigFieldSchema: pigFieldSchemas) {
        if (this.loadSaveFields.contains(pigFieldSchema.getName())) {
          if (pigFieldSchema.getName().equals("key")) {
            if (pigFieldSchema.getType() != DataType.findType(this.keyClass)) {
              throw new IOException("Key type expected: "+ this.keyClass.getName() + ", found Pig type " + DataType.genTypeToNameMap().get(pigFieldSchema.getType())) ;
            }
          } else {
            checkEqualSchema(pigFieldSchema, this.persistentSchema.getField(pigFieldSchema.getName()).schema()) ;
          }
        }        
      }
    
    } else {
      // Fields declared in constructor IN ORDER in the saving schema
      
      // Check the key at $0 == "key"
      if (pigFieldSchemas[0].getType() != DataType.findType(this.keyClass)) {
        throw new IOException("Key type expected: "+ this.keyClass.getName() + ", found Pig type " + DataType.genTypeToNameMap().get(pigFieldSchemas[0].getType())) ;
      }
      
      for (int fieldIndex = 1; fieldIndex < pigFieldSchemas.length; fieldIndex++) {
        checkEqualSchema(pigFieldSchemas[fieldIndex], this.persistentSchema.getField(pigFieldSchemas[fieldIndex].getName()).schema()) ;
      }
    }
    
    // Save the schema to UDFContext to use it on backend when writing data
    getUDFProperties().setProperty(GoraStorage.GORA_STORE_SCHEMA, s.toString()) ;
    
  }
  
  /**
   * Checks a Pig field schema comparing with avro schema, based on pig field's name (for record fields).
   * 
   * @param pigFieldSchema A Pig field schema
   * @param avroSchema Avro schema related with pig field schema.
   * @throws IOException
   */
  private void checkEqualSchema(ResourceFieldSchema pigFieldSchema, Schema avroSchema) throws IOException {

    byte pigType  = pigFieldSchema.getType() ;
    String fieldName = pigFieldSchema.getName() ;

    Type avroType = avroSchema.getType() ;

    // Switch that checks if avro type matches pig type, or if avro is union and some nested type matches pig type.
    switch (pigType) {
      case DataType.BAG: // Avro Array
        if (!avroType.equals(Type.ARRAY) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type ARRAY for "+fieldName+", got type " + avroType.name()) ;  
        checkEqualSchema(pigFieldSchema.getSchema().getFields()[0], avroSchema.getElementType()) ;
        break ;
      case DataType.BOOLEAN:
        if (!avroType.equals(Type.BOOLEAN) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type BOOLEAN for "+fieldName+", got type " + avroType.name()) ;  
        break ;
      case DataType.BYTEARRAY:
        if (!avroType.equals(Type.BYTES) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type BYTES for "+fieldName+", got type " + avroType.name()) ;
        break ;
      case DataType.CHARARRAY: // String
        if (!avroType.equals(Type.STRING) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type STRING for "+fieldName+", got type " + avroType.name()) ;
        break;
      case DataType.DOUBLE:
        if (!avroType.equals(Type.DOUBLE) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type DOUBLE for "+fieldName+", got type " + avroType.name()) ;
        break ;
      case DataType.FLOAT:
        if (!avroType.equals(Type.FLOAT) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type FLOAT for "+fieldName+", got type " + avroType.name()) ;
        break ;
      case DataType.INTEGER: // Int or Enum
        if (!avroType.equals(Type.INT) && !avroType.equals(Type.ENUM) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type INT or ENUM for "+fieldName+", got type " + avroType.name()) ;
        break ;
      case DataType.LONG:
        if (!avroType.equals(Type.LONG) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type LONG for "+fieldName+", got type " + avroType.name()) ;
        break ;
      case DataType.MAP: // Avro Map
        if (!avroType.equals(Type.MAP) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type MAP for "+fieldName+", got type " + avroType.name()) ;
        break ;
      case DataType.NULL: // Avro nullable??
        if(!avroType.equals(Type.NULL) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type NULL for "+fieldName+", got type " + avroType.name()) ;
        break ;
      case DataType.TUPLE: // Avro Record
        if (!avroType.equals(Type.RECORD) && !checkUnionSchema(avroSchema, pigFieldSchema))
          throw new IOException("Expected Avro type RECORD for "+fieldName+", got type " + avroType.name()) ;
        break ;
      default:
        throw new IOException("Unexpected Pig schema type " + DataType.genTypeToNameMap().get(pigType) + " for avro schema field " + avroSchema.getName() +": " + avroType.name()) ;
    }
    
  }

  /**
   * Checks and tries to match a pig field schema with an avro union schema. 
   * @param avroSchema Schema with
   * @param pigFieldSchema
   * @return true: if a match is found
   *         false: if avro schema is not UNION
   * @throws IOException(message, Exception()) if avro schema is UNION but not match is found for pig field schema.
   */
  private boolean checkUnionSchema(Schema avroSchema, ResourceFieldSchema pigFieldSchema) throws IOException {
    if (!avroSchema.getType().equals(Type.UNION)) return false ;

    for (Schema unionElementSchema: avroSchema.getTypes()) {
      try {
        checkEqualSchema(pigFieldSchema, unionElementSchema) ;
        return true ;
      }catch (IOException e){
        // Exception from inner union, rethrow
        if (e.getCause() != null) {
          throw e ;
        }
        // else ignore
      }
    }
    // throws IOException(message,Exception()) to mark nested union exception.
    throw new IOException("Expected some field in "+avroSchema.getName()+" for pig schema type"+DataType.genTypeToNameMap().get(pigFieldSchema.getType()), new Exception("Union not satisfied")) ;
  }
  
  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void prepareToWrite(RecordWriter writer) throws IOException {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage prepareToWrite() {}", this);
    this.writer = (GoraRecordWriter<?,? extends PersistentBase>) writer ;
    
    // Get the schema of data to write from UDFContext (coming from frontend checkSchema())
    String strSchema = getUDFProperties().getProperty(GoraStorage.GORA_STORE_SCHEMA) ;
    if (strSchema == null) {
      throw new IOException("Could not find schema in UDF context. Should have been set in checkSchema() in frontend.") ;
    }
    
    // Parse de the schema from string stored in properties object
    this.writeResourceSchema = new ResourceSchema(Utils.getSchemaFromString(strSchema)) ;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void putNext(Tuple t) throws IOException {

    ResourceFieldSchema[] tupleFieldSchemas = this.writeResourceSchema.getFields() ;
    PersistentBase persistentObj ;  
    
    try {
       persistentObj = this.persistentClass.newInstance() ;
    } catch (InstantiationException e) {
      throw new IOException(e) ;
    } catch (IllegalAccessException e) {
      throw new IOException(e) ;
    }

    for (int i=1; i<tupleFieldSchemas.length; i++) {
      persistentObj.put(persistentObj.getFieldIndex(tupleFieldSchemas[i].getName()),
                        this.writeField(persistentObj.getSchema().getField(tupleFieldSchemas[i].getName()).schema(),
                                        tupleFieldSchemas[i],
                                        t.get(i))) ;
    }

    try {
      ((GoraRecordWriter<Object,PersistentBase>) this.writer).write(t.get(0), (PersistentBase) persistentObj) ;
    } catch (InterruptedException e) {
      throw new IOException(e) ;
    }

  }

  /**
   * Converts one pig field data to PersistentBase Data.
   * 
   * @param avroSchema PersistentBase schema used to create new nested records
   * @param field Pig schema of the field being converted
   * @param pigData Pig data relative to the schema
   * @return PersistentBase data
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  private Object writeField(Schema avroSchema, ResourceFieldSchema field, Object pigData) throws IOException {

    // If data is null, return null (check if avro schema is right)
    if (pigData == null) {
      if (avroSchema.getType() != Type.UNION && avroSchema.getType() != Type.NULL) {
        throw new IOException("Tuple field is null, but Avro Schema is not union nor null") ;
      } else {
        return null ;
      }
    }
    
    // If avroSchema is union, it will not be the null field, so select the proper one
    if (avroSchema.getType() == Type.UNION) {
      avroSchema = avroSchema.getTypes().get(1) ;
    }
    
    switch(field.getType()) {
      case DataType.DOUBLE:
      case DataType.FLOAT:
      case DataType.LONG:
      case DataType.BOOLEAN:
      case DataType.NULL: return (Object)pigData ;
      case DataType.CHARARRAY: return new Utf8((String)pigData) ;
      case DataType.INTEGER:
          if (avroSchema.getType() == Type.ENUM) {
            AvroUtils.getEnumValue(avroSchema, (Integer)pigData);
          }else{
            return (Integer)pigData ;
          }
      case DataType.BYTEARRAY: return ByteBuffer.wrap(((DataByteArray)pigData).get()) ;
      case DataType.MAP:
        HashMap<Utf8,Object> persistentMap = new HashMap<Utf8,Object>() ;
        for (Map.Entry<String, Object> pigMapElement: ((Map<String,Object>)pigData).entrySet()) {
          persistentMap.put(new Utf8(pigMapElement.getKey()), this.writeField(avroSchema.getValueType(),field.getSchema().getFields()[0], pigMapElement.getValue())) ;
        }
        return persistentMap ;
      case DataType.BAG:
        Array<Object> persistentArray = new Array<Object>((int)((DataBag)pigData).size(),avroSchema) ;
        for (Object pigArrayElement: (DataBag)pigData) {
          if (avroSchema.getElementType().getType() == Type.RECORD) {
            // If element type is record, the mapping Persistent->PigType deletes one nested tuple:
            // We want the map as: map((a1,a2,a3), (b1,b2,b3),...) instead of map(((a1,a2,a3)), ((b1,b2,b3)), ...)
            persistentArray.add(this.writeField(avroSchema.getElementType(), field.getSchema().getFields()[0], pigArrayElement)) ;
          } else {
            // Every map has a tuple as element type. Since this is not a record, that "tuple" container must be ignored
            persistentArray.add(this.writeField(avroSchema.getElementType(), (field.getSchema().getFields()[0]).getSchema().getFields()[0], ((Tuple)pigArrayElement).get(0))) ;
          }
        }
        return persistentArray ;
      case DataType.TUPLE:
        try {
          PersistentBase persistentRecord = (PersistentBase) Class.forName(avroSchema.getFullName()).newInstance();
          
          ResourceFieldSchema[] tupleFieldSchemas = field.getSchema().getFields() ;
          
          for (int i=0; i<tupleFieldSchemas.length; i++) {
            persistentRecord.put(persistentRecord.getFieldIndex(tupleFieldSchemas[i].getName()),
                this.writeField(avroSchema.getField(tupleFieldSchemas[i].getName()).schema(),
                                tupleFieldSchemas[i],
                                ((Tuple)pigData).get(i))) ;
          }
          return persistentRecord ;
        } catch (InstantiationException e) {
          throw new IOException(e) ;
        } catch (IllegalAccessException e) {
          throw new IOException(e) ;
        } catch (ClassNotFoundException e) {
          throw new IOException(e) ;
        }
      default:
        throw new IOException("Unexpected field " + field.getName() +" with Pig type "+ DataType.genTypeToNameMap().get(field.getType())) ;
    }
    
  }

  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage setStoreFuncUDFContextSignature() {}", this);    
    this.udfcSignature = signature ;
  }

  @Override
  public void cleanupOnFailure(String location, Job job) throws IOException {
    // Do nothing 
    LOG.debug("***"+(UDFContext.getUDFContext().isFrontend()?"[FRONTEND]":"[BACKEND]")+" GoraStorage cleanupOnFailure() {}", this);    
  }

  @Override
  public void cleanupOnSuccess(String location, Job job) throws IOException {
    // Do nothing
  }

  public Set<String> getLoadSaveFields() {
    return loadSaveFields;
  }

  public void setLoadSaveFields(Set<String> loadSaveFields) {
    this.loadSaveFields = loadSaveFields;
  }

  public boolean isLoadSaveAllFields() {
    return loadSaveAllFields;
  }

  public void setLoadSaveAllFields(boolean loadSaveAllFields) {
    this.loadSaveAllFields = loadSaveAllFields;
  }

  public String[] getLoadQueryFields() {
    return loadQueryFields;
  }

  public void setLoadQueryFields(String[] loadQueryFields) {
    this.loadQueryFields = loadQueryFields;
  }

}
