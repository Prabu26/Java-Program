class Angle
{
int height;
int width;
void area(int h,int w)
{
height=h;
width=w;
int result=h*w;
System.out.println("The area is :"+result);
}
}
class DemoAngle
{
public static void main(String args[])
{
Angle a=new Angle();
a.area(10,26);
}}