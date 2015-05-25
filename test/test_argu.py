class Trevor():

    def __init__(self):
        self.age = 10

    def add(self, vari, num):
        vari = vari + num
        return vari

    def older(self):
        self.age = self.add(self.age, 5)
        print self.age

if __name__ == '__main__':
    t = Trevor()
    t.older()
