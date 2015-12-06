package com.ronald;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AppMapper extends Mapper <Object, Text, Text, Text>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        // Estructura: A	0_B:4,C:2,D:5
        String[] split = value.toString().split("_");
        
        String[] nodo = split[0].split("\t");
        String nId = nodo[0];
        int costo = Integer.parseInt(nodo[1]);
        
        String[] vecinos = split[1].split(",");
        int length = vecinos.length;
        
        if(costo != -1)
        {
            for(int i=0; i<length; i++)
            {
                if(!vecinos[i].equals(" "))
                {
                    String[] vecino = vecinos[i].split(":");
                    String v_nodo = vecino[0];
                    int v_peso = Integer.parseInt(vecino[1]);
                    
                    context.write(new Text(v_nodo), new Text(v_peso + costo + "_" + " "));
                }
            }
        }
        
        context.write(new Text(nId), new Text(costo + "_" + split[1]));
    }
}
