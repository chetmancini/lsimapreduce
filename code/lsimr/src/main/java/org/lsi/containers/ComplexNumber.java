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
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != getClass())
            return false;

        ComplexNumber rhs = (ComplexNumber) obj;
        return this.groupid.equals(rhs.groupid) &&
                this.index.equals(rhs.index);
    }

    public int hashCode() {
        return this.groupid.hashCode() ^
                this.index.hashCode();
    }

    public boolean lessThan(ComplexNumber c)
    {
        return (this.compareTo(c)<0);
    }

    public boolean greaterThan(ComplexNumber c)
    {
        return (this.compareTo(c)>0);
    }

    public String toString()
    {
        return "("+this.groupid+","+this.index+")";
    }
}
