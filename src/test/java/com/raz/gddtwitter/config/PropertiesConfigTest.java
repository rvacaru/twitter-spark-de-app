package com.raz.gddtwitter.config;


import com.raz.gddtwitter.config.properties.AppProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = PropertiesConfig.class)
@TestPropertySource(properties = {
        "spark.master=beAMaster",
        "spark.sql.shuffle.partitions=withThisShufflePartitions",
        "hdfs.default.name=andThisHdfsUri",
        "topics.hdfs.path.sample=andWithACertainSampleFilePath",
})
public class PropertiesConfigTest {

    @Autowired private AppProperties appProperties;

    @Test
    public void testAppPropertiesAreDefinedCorrectly() {
        assertEquals("beAMaster", appProperties.sparkMaster());
        assertEquals("withThisShufflePartitions", appProperties.sparkShufflePartitions());
        assertEquals("andThisHdfsUri", appProperties.hdfsName());
        assertEquals("andWithACertainSampleFilePath", appProperties.sampleHdfsPath());
    }

    @Test
    public void testAppPropertiesHaveDefaultValueWhenNotDefined() {
        assertEquals("hdfs:///default/path/twitter-sample.json", appProperties.twitterSampleHdfsPath());
    }

}
