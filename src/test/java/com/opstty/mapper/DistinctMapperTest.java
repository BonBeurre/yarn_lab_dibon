package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.conf.Configuration;

public class DistinctMapperTest {
    private MapDriver<Object, Text, Text, NullWritable> mapDriver;

    @Before
    public void setUp() {
        DistinctMapper mapper = new DistinctMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        // Add this configuration
        Configuration conf = new Configuration();
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization," +
                        "org.apache.hadoop.io.serializer.WritableSerialization");
        mapDriver.setConfiguration(conf);
    }

    @Test
    public void testMapper() throws Exception {
        // Test with a valid input line
        String inputLine = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars";
        mapDriver.withInput(new Text(inputLine), new Text())
                .withOutput(new Text("7"), NullWritable.get())
                .runTest();

        // Test with another valid input line
        inputLine = "(48.8685686134, 2.31331809304);8;Calocedrus;decurrens;Cupressaceae;1854;20.0;195.0;Cours-la-Reine, avenue Franklin-D.-Roosevelt, avenue Matignon, avenue Gabriel;Cèdre à encens;;11;Jardin des Champs Elysées";
        mapDriver.withInput(new Text(inputLine), new Text())
                .withOutput(new Text("8"), NullWritable.get())
                .runTest();

        // Test with header line (should be ignored)
        inputLine = "GEOPOINT;ARRONDISSEMENT;GENRE;ESPECE;FAMILLE;ANNEE PLANTATION;HAUTEUR;CIRCONFERENCE;ADRESSE;NOM COMMUN;VARIETE;OBJECTID;NOM_EV";
        mapDriver.withInput(new Text(inputLine), new Text())
                .runTest();
    }
}