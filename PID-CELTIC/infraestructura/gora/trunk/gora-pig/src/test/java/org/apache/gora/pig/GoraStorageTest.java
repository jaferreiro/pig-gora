package org.apache.gora.pig;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class GoraStorageTest {

    /** The configuration */
    protected static Configuration configuration;

    private static HBaseTestingUtility utility;
    private static PigServer pigServer ;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
        System.setProperty(HBaseTestingUtility.TEST_DIRECTORY_KEY, "build/test-data");
        Configuration localExecutionConfiguration = new Configuration() ;
        localExecutionConfiguration.setStrings("hadoop.log.dir", localExecutionConfiguration.get("hadoop.tmp.dir")) ;
        utility = new HBaseTestingUtility(localExecutionConfiguration);
        utility.startMiniCluster(1);
        utility.startMiniMapReduceCluster(1) ;
        configuration = utility.getConfiguration();
        
        configuration.writeXml(new FileOutputStream("target/test-classes/core-site.xml")) ;
        
        Properties props = new Properties();
        props.setProperty("fs.default.name", configuration.get("fs.default.name"));
        props.setProperty("mapred.job.tracker", configuration.get("mapred.job.tracker"));
        pigServer = new PigServer(ExecType.MAPREDUCE, props);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
        pigServer.shutdown() ;
        utility.shutdownMiniMapReduceCluster() ;
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws IOException {
        FileSystem fs = FileSystem.get(configuration) ;

        Path inputHdfsFile = new Path("frases.txt") ;
        fs.delete(inputHdfsFile, true) ;
        
        fs.copyFromLocalFile(false, true, new Path("target/test-classes/datos/frases.txt"), inputHdfsFile) ;

        pigServer.setJobName("normalizacion UDF test") ;
        pigServer.registerJar("target/normalizador-0.0.1-SNAPSHOT.jar") ;
        pigServer.registerJar("target/lib/*.jar") ;
        pigServer.registerFunction("normalizar", new FuncSpec("es.indra.innovationlabs.bigdata.normalizador.UDFNormalizador")) ;
        pigServer.registerQuery("textos = LOAD 'frases.txt' as (idioma:chararray, texto:chararray) ;") ;
        pigServer.registerQuery("textos_normalizados = FOREACH textos GENERATE normalizar(texto,idioma) ;" ) ;
        
        Iterator<Tuple> resultados = pigServer.openIterator("textos_normalizados") ;
        Assert.assertTrue("Se esperaban resultados tras la ejecución en Pig, pero no se ha recibido ninguno", resultados.hasNext()) ;

        BufferedReader frasesEsperadas = new BufferedReader(new FileReader("target/test-classes/datos/esperado.txt")) ;
        try {
            while (resultados.hasNext()) {
                Tuple resultado = resultados.next() ;
                String fraseResultado = (String) resultado.get(0) ;
                String fraseEsperada = frasesEsperadas.readLine() ;
                Assert.assertEquals(fraseEsperada,fraseResultado) ;
            }
        } finally {
            frasesEsperadas.close() ;
        }
	}

}
