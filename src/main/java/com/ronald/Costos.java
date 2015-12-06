package com.ronald;

class Costos
{
    private static int distance[][]=
                                    {
                                        {0,3,4,1,-1,-1,-1},
                                        {-1,0,-1,-1,2,-1,-1},
                                        {-1,-1,0,-1,-1,-1,5},
                                        {-1,-1,-1,0,-1,1,-1},
                                        {-1,-1,-1,-1,0,-1,3},
                                        {-1,-1,3,-1,-1,0,-1},
                                        {-1,-1,-1,-1,-1,-1,0}
                                    };
                                    
    public static int get(char a,char b)
    {
        int Inta = a - 'A';
        int Intb = b - 'A';
        return distance[Inta][Intb];
    }
}