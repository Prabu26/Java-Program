1) Predict the output 
public class A
{
public void printMessage(String str1)
{
System.out.println("Hello"+str1);
}
public String printMessage(String str1){
return "Hello"+str1;
}
public static void main(String[] args)
{
A obj= new A();
obj.printMessage("John");
}
}
ans: error at the time of compilation

2) which is not inherited?
ans:constructor

3) which is true regarding subclassing?
ans: it inherits all non-private members of the class ( i think, check it)

4) find the error in following ccode
 public static void main(String[] args)
{
int nums[]= {3,4,5}; //line1
for(int nums:num) //line2
{
System.out.println(num); //line3
}
ans: error in line2
