class Math
{
int a,b,c;
void sum(int a,int b)
{
c=a+b;
System.out.println("Sum of the addtion value is:"+c);
}
void sub(int a,int b)
{
 c=b-a;
System.out.println("The subtract value is :"+c);
}
void multi(int a,int b)
{
c=a*b;
System.out.println("The multiplication value is :"+c);
}
void division(int a,int b)
{
c=a/b;
System.out.println("The division value is :"+c);
}
void mod(int a,int b)
{
c=a%b;
System.out.println("The Mod value is :"+c);
}
}
class MathDemo
{
public static void main(String args[])
{
Math m=new Math();
m.sum(26,58);
m.sub(26,58);
m.multi(26,58);
m.division(58,26);
m.mod(58,26);
}
}
