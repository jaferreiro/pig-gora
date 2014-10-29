package org.apache.gora.pig;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GoraStorageTest {

  /** The configuration */
  protected static Configuration            configuration;

  private static HBaseTestingUtility        utility;
  private static PigServer                  pigServer;
  private static DataStore<String, WebPage> dataStore;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
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
    
    WebPage w = WebPage.newBuilder().build() ;
	  
	  w.setUrl("http://gora.apache.org") ;
	  w.setContent(ByteBuffer.wrap("Texto 1".getBytes())) ;
	  w.getParsedContent().add("elemento1") ;
    w.getParsedContent().add("elemento2") ;
    w.getOutlinks().put("k1", "v1") ;
    Metadata m = Metadata.newBuilder().build() ;
    m.setVersion(3) ;
    w.setMetadata(m) ;
    
    dataStore.put("key1", w) ;
    
    w.clear() ;
    w.setUrl("http://www.google.com") ;
    w.setContent(ByteBuffer.wrap("Texto 2".getBytes())) ;
    w.getParsedContent().add("elemento7") ;
    w.getParsedContent().add("elemento15") ;
    w.getOutlinks().put("k7", "v7") ;
    m = Metadata.newBuilder().build() ;
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
    pigServer.registerJar("target/gora-pig-0.4.1.jar");
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
    
    WebPage expected = WebPage.newBuilder().build() ;
    expected.setUrl("HTTP://GORA.APACHE.ORG") ;
    expected.setContent(ByteBuffer.wrap("Texto 1".getBytes())) ;
    expected.getOutlinks().put("k1", "v1") ;
    expected.getParsedContent().add("elemento1") ;
    expected.getParsedContent().add("elemento2") ;
    Metadata m = Metadata.newBuilder().build() ;
    m.setVersion(3) ;
    expected.setMetadata(m) ;

    Assert.assertEquals(expected, webpageUpper) ;
    
    webpageUpper = dataStore.get("key7") ;
    Assert.assertNotNull("Expected record with key 'key7' not found", webpageUpper) ;
    expected = WebPage.newBuilder().build() ;
    expected.setUrl("HTTP://WWW.GOOGLE.COM") ;
    expected.setContent(ByteBuffer.wrap("Texto 2".getBytes())) ;
    expected.getOutlinks().put("k7", "v7") ;
    expected.getParsedContent().add("elemento7") ;
    expected.getParsedContent().add("elemento15") ;
    m = Metadata.newBuilder().build() ;
    m.setVersion(7) ;
    expected.setMetadata(m) ;
    Assert.assertEquals(expected, webpageUpper) ;
    
  }

  @Test
  public void testUpdateMap() throws IOException {
    FileSystem hdfs = FileSystem.get(configuration) ;
    // Values to add to output links of webpage
    hdfs.copyFromLocalFile(new Path("src/test/resources/test-delete-map-values-addvals.csv"), new Path("test-delete-map-values-addvals.csv")) ;
    
    pigServer.setJobName("gora-pig test - update map");
    pigServer.registerJar("target/gora-pig-0.4.1.jar");
    pigServer.registerQuery("map_values = LOAD 'test-delete-map-values-addvals.csv' USING PigStorage('|') AS (outlinks:map[chararray]) ;") ;
    pigServer.registerQuery("pages = LOAD '.' using org.apache.gora.pig.GoraStorage (" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'outlinks') ;",1);
    pigServer.registerQuery("pages_updated = FOREACH pages GENERATE key, map_values.outlinks as outlinks ;") ;
    pigServer.registerQuery("STORE pages_updated INTO '.' using org.apache.gora.pig.GoraStorage(" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'outlinks') ;",3);

    WebPage webpage = dataStore.get("key1") ;
    Assert.assertTrue("Record 'key1' expected to have 'k1#v1' in outlinks", webpage.getOutlinks().get("k1").toString().compareTo("v1") == 0) ;
    Assert.assertTrue("Record 'key1' expected to have 'k2#v2' in outlinks", webpage.getOutlinks().get("k2").toString().compareTo("v2") == 0) ;
    Assert.assertTrue("Record 'key1' expected to have 'k3#v3' in outlinks", webpage.getOutlinks().get("k3").toString().compareTo("v3") == 0) ;
    //for (Utf8 key: webpage.getOutlinks().keySet()) {
    //  System.out.println(key + " : " + webpage.getOutlinks().get(key)) ;
    //}
  }
  
  /**
   * Tests loading/saving all fields with '*'
   */
  @Test
  public void testLoadSaveAllFields() throws IOException {

    pigServer.setJobName("gora-pig test - load all fields");
    pigServer.registerJar("target/gora-pig-0.4.1.jar");
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
    expected.getOutlinks().put("k1", "v1") ;
    expected.getParsedContent().add("elemento1") ;
    expected.getParsedContent().add("elemento2") ;
    Metadata m = Metadata.newBuilder().build() ;
    m.setVersion(1) ;
    expected.setMetadata(m) ;

    Assert.assertEquals(expected, webpageUpper) ;
    
    webpageUpper = dataStore.get("key7") ;
    Assert.assertNotNull("Expected record with key 'key7' not found", webpageUpper) ;
    expected = dataStore.getBeanFactory().newPersistent() ;
    expected.setUrl("HTTP://WWW.GOOGLE.COM") ;
    expected.setContent(ByteBuffer.wrap("Texto 2".getBytes())) ;
    expected.getOutlinks().put("k7", "v7") ;
    expected.getParsedContent().add("elemento7") ;
    expected.getParsedContent().add("elemento15") ;
    m =  Metadata.newBuilder().build() ;
    m.setVersion(1) ;
    expected.setMetadata(m) ;
    Assert.assertEquals(expected, webpageUpper) ;
  }

  @Test
  public void testDeleteRows() throws IOException {
    FileSystem hdfs = FileSystem.get(configuration) ;
    hdfs.copyFromLocalFile(new Path("src/test/resources/test-delete-rows.csv"), new Path("test-delete-rows.csv")) ;
    
    pigServer.setJobName("gora-pig test - delete rows");
    pigServer.registerJar("target/gora-pig-0.4.1.jar");
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
    FileSystem hdfs = FileSystem.get(configuration) ;
    // Values to add to output links of webpage
    hdfs.copyFromLocalFile(new Path("src/test/resources/test-delete-map-values-addvals.csv"), new Path("test-delete-map-values-addvals.csv")) ;
    // Values to delete from output links of webpage with key 'key1'
    hdfs.copyFromLocalFile(new Path("src/test/resources/test-delete-map-values.csv"), new Path("test-delete-map-values.csv")) ;
    
    pigServer.setJobName("gora-pig test - delete map values");
    pigServer.registerJar("target/gora-pig-0.4.1.jar");
    pigServer.registerQuery("map_values = LOAD 'test-delete-map-values-addvals.csv' USING PigStorage('|') AS (outlinks:map[chararray]) ;") ;
    pigServer.registerQuery("pages = LOAD '.' using org.apache.gora.pig.GoraStorage (" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'outlinks') ;",1);
    pigServer.registerQuery("pages_updated = FOREACH pages GENERATE key, map_values.outlinks as outlinks ;") ;
    pigServer.registerQuery("STORE pages_updated INTO '.' using org.apache.gora.pig.GoraStorage(" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'outlinks') ;",3);

    // Now, both pages must have "k2#v2" and "k3#v3" in outlinks

    WebPage webpage = dataStore.get("key1") ;
    Assert.assertTrue("Record 'key1' expected to have 'k1' in outlinks", webpage.getOutlinks().containsKey("k1")) ;
    Assert.assertTrue("Record 'key1' expected to have 'k2' in outlinks", webpage.getOutlinks().containsKey("k2")) ;
    Assert.assertTrue("Record 'key1' expected to have 'k3' in outlinks", webpage.getOutlinks().containsKey("k3")) ;
    Assert.assertTrue("Record 'key1' expected to have 'k1#v1' in outlinks", webpage.getOutlinks().get("k1").toString().compareTo("v1") == 0) ;
    Assert.assertTrue("Record 'key1' expected to have 'k2#v2' in outlinks", webpage.getOutlinks().get("k2").toString().compareTo("v2") == 0) ;
    Assert.assertTrue("Record 'key1' expected to have 'k3#v3' in outlinks", webpage.getOutlinks().get("k3").toString().compareTo("v3") == 0) ;

    webpage = dataStore.get("key7") ;
    Assert.assertTrue("Record 'key7' expected to have 'k7' in outlinks", webpage.getOutlinks().containsKey("k7")) ;
    Assert.assertTrue("Record 'key7' expected to have 'k2' in outlinks", webpage.getOutlinks().containsKey("k2")) ;
    Assert.assertTrue("Record 'key7' expected to have 'k3' in outlinks", webpage.getOutlinks().containsKey("k3")) ;
    Assert.assertTrue("Record 'key7' expected to have 'k7#v7' in outlinks", webpage.getOutlinks().get("k7").toString().compareTo("v7") == 0) ;
    Assert.assertTrue("Record 'key7' expected to have 'k2#v2' in outlinks", webpage.getOutlinks().get("k2").toString().compareTo("v2") == 0) ;
    Assert.assertTrue("Record 'key7' expected to have 'k3#v3' in outlinks", webpage.getOutlinks().get("k3").toString().compareTo("v3") == 0) ;
    
    pigServer.registerQuery("delete_values = LOAD 'test-delete-map-values.csv' USING PigStorage('|') AS (key:chararray, outlinks:map[chararray]) ; ") ;
    pigServer.registerQuery("STORE delete_values INTO '.' using org.apache.gora.pig.GoraDeleteStorage(" +
        "'java.lang.String'," +
        "'org.apache.gora.examples.generated.WebPage'," +
        "'values') ;");
    
    // Now, page with "key1" must not contain in outlinks key "k1", but contain "k2" and "k3"

    webpage = dataStore.get("key1") ;
    Assert.assertTrue("Record 'key1' expected to NOT have 'k1' in outlinks", !webpage.getOutlinks().containsKey("k1")) ;
    Assert.assertTrue("Record 'key1' expected to have 'k2' in outlinks", webpage.getOutlinks().containsKey("k2")) ;
    Assert.assertTrue("Record 'key1' expected to have 'k3' in outlinks", webpage.getOutlinks().containsKey("k3")) ;
    Assert.assertTrue("Record 'key1' expected to have 'k2#v2' in outlinks", webpage.getOutlinks().get("k2").toString().compareTo("v2") == 0) ;
    Assert.assertTrue("Record 'key1' expected to have 'k3#v3' in outlinks", webpage.getOutlinks().get("k3").toString().compareTo("v3") == 0) ;

    webpage = dataStore.get("key7") ;
    Assert.assertTrue("Record 'key7' expected to have 'k7' in outlinks", webpage.getOutlinks().containsKey("k7")) ;
    Assert.assertTrue("Record 'key7' expected to have 'k2' in outlinks", webpage.getOutlinks().containsKey("k2")) ;
    Assert.assertTrue("Record 'key7' expected to have 'k3' in outlinks", webpage.getOutlinks().containsKey("k3")) ;
    Assert.assertTrue("Record 'key7' expected to have 'k7#v7' in outlinks", webpage.getOutlinks().get("k7").toString().compareTo("v7") == 0) ;
    Assert.assertTrue("Record 'key7' expected to have 'k2#v2' in outlinks", webpage.getOutlinks().get("k2").toString().compareTo("v2") == 0) ;
    Assert.assertTrue("Record 'key7' expected to have 'k3#v3' in outlinks", webpage.getOutlinks().get("k3").toString().compareTo("v3") == 0) ;
  }
}
 