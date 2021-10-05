package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class Trees_Mapper_Test {
    @Mock
    private Mapper.Context context;
    private Trees_Mapper trees_mapper;
    @Before
    public void setup() {
        this.trees_mapper = new Trees_Mapper();
    }
    @Test
    public void testMap() throws IOException, InterruptedException {
        String value = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars";

        this.trees_mapper.map(null, new Text(value), this.context);
        verify(this.context, times(20))
                .write(new IntWritable(7), new IntWritable(1));
    }
}