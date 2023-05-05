# 这是一行注释

import sys
import copy
import traceback


def test_copy():
    l1 = [1, 2, 3]
    # 直接赋值
    l2 = l1
    print(l1 == l2, end=' ')
    print(l1 is l2)
    # 浅拷贝
    l3 = list(l1)
    print(l1 == l3, end=' ')
    print(l1 is l3)
    # 深拷贝
    l4 = copy.deepcopy(l1)
    print(l1 == l4, end=' ')
    print(l1 is l4)


def test_dict():
    # general
    t1 = ('齐锴', "齐君质", '齐相宜')
    age_dict = dict.fromkeys(t1)
    age_dict['齐锴'] = 39
    age_dict['齐君质'] = 9
    age_dict['齐相宜'] = 3
    print(age_dict.get('齐锴'))
    # del d2['齐相宜']
    if '齐相宜' in age_dict:
        print('in')
    else:
        print('not in')
    print(age_dict)
    print(type(age_dict.keys()))
    for k, v in age_dict.items():
        print('%s的年龄是%d岁' % (k, v))
    # format
    format_dict = {"name": '齐锴', 'age': 39}
    print("名字是%(name)s, 年龄是%(age)d岁" % format_dict)


def test_string_list():
    # string
    s1 = "thisisastring"
    s2 = "这是一个字符串"
    print(s1[2:5], len(s1), type(s1[5]))
    print(s2[2:5], len(s2), type(s2[5]))
    print(len(s1), min(s1), max(s1), "b" in s1)
    # list
    l1 = [1, "str", True, "hhh"]  # create 1
    print(l1, type(l1))
    l2 = list(s1)  # create 2
    l2.extend(l1)
    l2.insert(0, "inserted")
    print(l2)
    del l2[7]
    del l2[3:6]
    print(l2)
    l2.pop(3)
    print(l2)
    l2.pop()
    print(l2)
    l2.remove("str")
    print(l2)
    sss = "cda"
    if sss in l2:
        l2.remove(sss)
    else:
        print(sss, "not in ")
    print(l2.count('t'))
    print(l2.index('t'))  # 只返回第一个


def test_input_print():
    a = input("Enter Something:")
    print(a + " type : " + str(type(a)))
    print(a, "type :", str(type(a)))
    print(a, "type :", str(type(a)), sep="|")
    print("You Enter is %s" % a)


def test_general():
    n = 10_23_894_294
    print(type(n))
    print(n)
    n = "fuck the world"
    print(type(n))
    print(n)
    n = "super start is me"
    print(n)
    print(sys.getdefaultencoding())


def test_bytes():
    b = "这是最好的时代".encode('UTF-8')
    print("直接打印:", b)
    print("解码打印：", b.decode('UTF-8'))


def test_string():
    ss = "this is a gOOd string"
    print('the length of ss is :', len(ss))
    print(ss.split())
    print(ss.split('s'))
    l1 = ['this', 'is', 'a', 'good', 'string']
    print(' '.join(l1))
    print('A'.join(l1))
    print(ss.count("is a"))
    print(ss.title())
    print(ss.upper())
    print(ss.lower())
    s2 = ' this is a bad string     '
    print(s2.strip())
    print(s2)


def test_bmi():
    height = float(input("Enter your height:"))
    weight = float(input("Enter your weight:"))
    bmi = weight / (height ** 2)
    print(bmi)
    if bmi < 18.5:
        print("Too thin")
    elif 18.5 <= bmi < 24.9:
        print("Very goog")
    elif 24.9 <= bmi < 29.9:
        print("A little fat")
    else:
        print("Fat")


def test_tuiduan():
    # list
    a_range = list(range(10))
    print(a_range)
    a_list = [x * x for x in a_range]
    print(a_list)
    # dict
    ss = "this is a good string"
    ss_list = ss.split()
    print(ss_list)
    dict1 = {x: len(x) for x in ss_list}
    print(dict1)


def test_sort():
    """
    这是一个排序函数
    """
    l1 = [3, 123, 6, 23, 537, 543, 21]
    print(sorted(l1))
    print(sorted(l1, reverse=True))


class Apple:
    glo = "Global"

    def __init__(self, c, s):
        self.size = s
        self.color = c

    def show_my(self):
        print(self.color, self.size)


def test_except():
    try:
        a = input("输入被除数：")
        b = input("输入除数：")
        if not a.isdigit() or not b.isdigit():
            raise ValueError("输入有问题")
        c = int(int(a) / int(b))
        print("相除的结果是：", c)
    except ValueError as e1:
        traceback.print_exc()
    except Exception as e:
        traceback.print_exc()
    else:
        print("一切正常，程序继续运行")
    finally:
        print("无论如何都到我这儿")


if __name__ == '__main__':
    test_except()
    # apple = Apple("Red", "Big")
    # apple.show_my()
    # print(apple.glo, Apple.__annotations__)
    # test_general()
    # test_bytes()
    # test_input_print()
    # test_string_list()
    # test_dict()
    # test_copy()
    # test_string()
    # test_bmi()
    # test_tuiduan()
    # test_sort()
    # help(test_sort)
    # print(test_sort.__doc__)
