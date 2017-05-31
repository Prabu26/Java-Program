class Rectangle
{
int height;
int width;
void area()
{
int result=height*width;
System.out.println("The area is"+result);
}
}
class RectDemo
{
public static void main(String args[])
{
Rectangle r=new Rectangle();
r.height=10;
r.width=26;
r.area();
}
}
