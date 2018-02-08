import functools


class Example():
    def __init__(self):
        self.x = 34

    def p(self):
        print(self.x)

    def morph(self):
        self.__class__ = Example2
        self.p = functools.partial(Example2.p, self)
        Example2.__init__(self, 14)

class Example2(Example):
    def __init__(self, a):
        super().__init__()
        self.x = a

    def p(self):
        print("hello:", self.x)


e1 = Example()
e2 = Example2(24)

e1.p()
e2.p()

print(e1.__class__, e2.__class__)

e1.morph()
print(e1.__class__, e2.__class__)
e1.p()