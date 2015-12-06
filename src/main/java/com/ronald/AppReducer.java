package com.ronald;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

class AppReducer extends Reducer<Text, Text, Text, Text>
{
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
        int costo_total = -1;
        String s_vecinos = " ";
        
        for(Text value : values)
        {
            // Estructura: A	0_B:4,C:5,D:3
            String parts[] = value.toString().split("_");
            int costo = Integer.parseInt(parts[0]);
            String vecinos = parts[1];
            
            if(costo != -1) // Si el costo no es infinito
            {
                if(costo_total == -1) { costo_total = costo; } // Si aun no se asigno un costo minimo
                else { if(costo < costo_total) { costo_total = costo; } } // Si el nuevo costo es menor
            }

            if(!vecinos.equals(" ")) { s_vecinos = vecinos; }
        }
        context.write(key, new Text(costo_total + "_" + s_vecinos));
    }
}
