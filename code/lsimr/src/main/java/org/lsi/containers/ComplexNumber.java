package org.lsi.containers;
public class ComplexNumber implements Comparable <ComplexNumber>{
    public Integer index = 0;
    public Integer groupid = 0;

    public ComplexNumber(Integer groupid, Integer index)
    {
        this.index = index;
        this.groupid = groupid;
    }

    public int compareTo(ComplexNumber c)
    {
        if(this.groupid<c.groupid)
            return -1;
        else if(this.groupid==c.groupid)
        {
            if(this.index<c.index)
                return -1;
            else if(this.index==c.index)
                return 0;
        }
        return 1;
    }
}
