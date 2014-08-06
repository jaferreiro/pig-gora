package org.apache.gora.pig;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Set;

import junit.framework.Assert;

import org.apache.gora.examples.generated.Metadata;
import org.apache.gora.examples.generated.WebPage;
import org.apache.gora.store.DataStore;
import org.apache.gora.store.DataStoreFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.builtin.mock.Storage.Data;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Sets;

public class GoraStorageTest {

  /** The configuration */
  protected static Configuration            configuration;

  private static HBaseTestingUtility        utility;
  private static PigServer                  pigServer;
  private static DataStore<String, WebPage> dataStore;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    System.setProperty(HBaseTestingUtility.TEST_DIRECTORY_KEY, "build/test-data");
    Configuration localExecutionConfiguration = new Configuration();
    localExecutionConfiguration.setStrings("hadoop.log.dir", localExecutionConfiguration.get("hadoop.tmp.dir"));
    utility = new HBaseTestingUtility(localExecutionConfiguration);
    utility.startMiniCluster(1);
    utility.startMiniMapReduceCluster(1);
    configuration = utility.getConfiguration();

    configuration.writeXml(new FileOutputStream("target/test-classes/core-site.xml"));

    Properties props = new Properties();
    props.setProperty("fs.default.name", configuration.get("fs.default.name"));
    props.setProperty("mapred.job.tracker", configuration.get("mapred.job.tracker"));
    pigServer = new PigServer(ExecType.MAPREDUCE, props);

    dataStore = DataStoreFactory.getDataStore(String.class, WebPage.class, configuration);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    pigServer.shutdown();
    utility.shutdownMiniMapReduceCluster();
  }

  @Before
	public void setUp() throws Exception {

    dataStore.delete("key1") ;
    dataStore.delete("key7") ;
    
    WebPage w = dataStore.getBeanFactory().newPersistent() ;
	  Metadata m ;
	  
	  w.setUrl("http://gora.apache.org") ;
	  w.setContent(ByteBuffer.wrap("Texto 1".getBytes())) ;
	  w.addToParsedContent("elemento1") ;
    w.addToParsedContent("elemento2") ;
    w.putToOutlinks("k1", "v1") ;
    m = new Metadata() ;
    m.setVersion(3) ;
    w.setMetadata(m) ;
    
    dataStore.put("key1", w) ;
    
    w.clear() ;
    w.setUrl("http://www.google.com") ;
    w.setContent(ByteBuffer.wrap("Texto 2".getBytes())) ;
    w.addToParsedContent("elemento7") ;
    w.addToParsedContent("elemento15") ;
    w.putToOutlinks("k7", "v7") ;
    m = new Metadata() ;
    m.setVersion(7) ;
    w.setMetadata(m) ;
    
    dataStore.put("key7", w) ;
    dataStore.flush() ;
	}

  @After
  public void tearDown() throws Exception {
  }

  /**
   * Test loading/saving a subset of the fields defined in the constructor
   */
  @Test
  public void testLoadSaveSubset() throws IOException {

    pigServer.setJobName("gora-pig test - load all fields");
    pigServer.registerJar("target/gora-pig-0.4-indra-SNAPSHOT.jar");
    pigServer.registerQuery("paginas = LOAD '.' using org.apache.gora.pig.GoraStorage (" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'url, content') ;",1);
    pigServer.registerQuery("resultado = FOREACH paginas GENERATE key, UPPER(url) as url, content ;",2);
    pigServer.registerQuery("STORE resultado INTO '.' using org.apache.gora.pig.GoraStorage(" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'url') ;",3);
    
    WebPage webpageUpper = dataStore.get("key1") ;
    Assert.assertNotNull("Record with key 'key1' not found", webpageUpper) ;
    
    WebPage expected = dataStore.getBeanFactory().newPersistent() ;
    expected.setUrl("HTTP://GORA.APACHE.ORG") ;
    expected.setContent(ByteBuffer.wrap("Texto 1".getBytes())) ;
    expected.putToOutlinks("k1", "v1") ;
    expected.addToParsedContent("elemento1") ;
    expected.addToParsedContent("elemento2") ;
    Metadata m = new Metadata() ;
    m.setVersion(3) ;
    expected.setMetadata(m) ;

    Assert.assertEquals(expected, webpageUpper) ;
    
    webpageUpper = dataStore.get("key7") ;
    Assert.assertNotNull("Expected record with key 'key7' not found", webpageUpper) ;
    expected = dataStore.getBeanFactory().newPersistent() ;
    expected.setUrl("HTTP://WWW.GOOGLE.COM") ;
    expected.setContent(ByteBuffer.wrap("Texto 2".getBytes())) ;
    expected.putToOutlinks("k7", "v7") ;
    expected.addToParsedContent("elemento7") ;
    expected.addToParsedContent("elemento15") ;
    m = new Metadata() ;
    m.setVersion(7) ;
    expected.setMetadata(m) ;
    Assert.assertEquals(expected, webpageUpper) ;
    
  }

  /**
   * Tests loading/saving all fields with '*'
   */
  @Test
  public void testLoadSaveAllFields() throws IOException {

    pigServer.setJobName("gora-pig test - load all fields");
    pigServer.registerJar("target/gora-pig-0.4-indra-SNAPSHOT.jar");
    pigServer.registerQuery("paginas = LOAD '.' using org.apache.gora.pig.GoraStorage (" +
    		"'java.lang.String'," +
    		"'org.apache.gora.examples.generated.WebPage'," +
    		"'*') ;",1);
    pigServer.registerQuery("resultado = FOREACH paginas GENERATE key, UPPER(url) as url, content, outlinks," +
    		"                                   {} as parsedContent:{(chararray)}, (1, []) as metadata:(version:int, data:map[]) ;",2);
    pigServer.registerQuery("STORE resultado INTO '.' using org.apache.gora.pig.GoraStorage(" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'*') ;",3);
    
    WebPage webpageUpper = dataStore.get("key1") ;
    Assert.assertNotNull("Expected record with key 'key1' not found", webpageUpper) ;
    
    WebPage expected = dataStore.getBeanFactory().newPersistent() ;
    expected.setUrl("HTTP://GORA.APACHE.ORG") ;
    expected.setContent(ByteBuffer.wrap("Texto 1".getBytes())) ;
    expected.putToOutlinks("k1", "v1") ;
    expected.addToParsedContent("elemento1") ;
    expected.addToParsedContent("elemento2") ;
    Metadata m = new Metadata() ;
    m.setVersion(1) ;
    expected.setMetadata(m) ;

    Assert.assertEquals(expected, webpageUpper) ;
    
    webpageUpper = dataStore.get("key7") ;
    Assert.assertNotNull("Expected record with key 'key7' not found", webpageUpper) ;
    expected = dataStore.getBeanFactory().newPersistent() ;
    expected.setUrl("HTTP://WWW.GOOGLE.COM") ;
    expected.setContent(ByteBuffer.wrap("Texto 2".getBytes())) ;
    expected.putToOutlinks("k7", "v7") ;
    expected.addToParsedContent("elemento7") ;
    expected.addToParsedContent("elemento15") ;
    m = new Metadata() ;
    m.setVersion(1) ;
    expected.setMetadata(m) ;
    Assert.assertEquals(expected, webpageUpper) ;
  }

  @Test
  public void testDeleteRows() throws IOException {
    
    FileSystem hdfs = FileSystem.get(configuration) ;
    hdfs.copyFromLocalFile(new Path("src/test/resources/test-delete-rows.csv"), new Path("test-delete-rows.csv")) ;
    
    pigServer.setJobName("gora-pig test - delete rows");
    pigServer.registerJar("target/gora-pig-0.4-indra-SNAPSHOT.jar");
    pigServer.registerQuery("delete_rows = LOAD 'test-delete-rows.csv' AS (key:chararray) ;") ;
    pigServer.registerQuery("STORE delete_rows INTO '.' using org.apache.gora.pig.GoraDeleteStorage(" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'rows') ;");
    
    WebPage webpage = dataStore.get("key1") ;
    Assert.assertNull("Record with key 'key1' found", webpage) ;

    webpage = dataStore.get("key7") ;
    Assert.assertNotNull("Record with key 'key7' not found", webpage) ;
    
  }
  
  @Test
  public void testDeleteMapValues() throws IOException {
    
  }
}
 