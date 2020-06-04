#include<iostream>
#include<string>

// 整数x，转换成十进制表示字符串 的长度
int len1(int x)
{
    std::string s=std::to_string(x);
    return s.size();
}

// 0 -> "0" -> 1
// 1 -> "1" -> 1
// 9 -> "9" -> 1
// 10 -> "10" -> 2
int len2(int x)
{
    int len=1;
    while(x>=10)
    {   
        x = x/10;
        len++;
    }   
    return len;
}

int main()
{
    for(int i=0; i<1100; i+=9)
    {   
        std::cout<<i<<"\t"<<len1(i)<<"\t"<<len2(i)<<std::endl;
     
    }   
    return 0;
}               
