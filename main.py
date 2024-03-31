def func1():
    print('func1')
    return 2

def func2():
    print('func2')
    return 3

def func3():
    print('func3')
    return None

if func3() and func1() > func3():
    print('Entrou')
print('Saiu')