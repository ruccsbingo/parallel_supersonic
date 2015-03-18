#include <iostream>

class A{
    private:
        int a;

    public:
        A():a(1){
        }
        virtual void print(){
            std::cout<<a<<std::endl;
        }
};

class B:public A{
    private:
        int a;
        int b;
    public:
        B():a(2), b(3){
        }
        void print(){
            std::cout<<b<<std::endl;
        }
};

int main(){
    A *a;
    B *b = new B();
    //a=b;
    a = new B();

    a->print();
    return 0;
}
