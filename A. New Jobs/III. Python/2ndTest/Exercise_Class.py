class Animal:
    def __init__(self, name):
        self.name = name

    def Info(self):
        print("Loài động vật ,","Tên:",self.name)

    def NameInfo(self):
        print("Tên:",self.name)



class Mammals (Animal):
    def __init__(self,name,envi,runspeed):
        super().__init__(name)
        self.envi = envi
        self.runspeed = runspeed

    def Info(self):
        print("Thú có vú ,","Tên:", self.name, ", Môi trường sống:", self.envi, ", Avg Move Speed:", self.runspeed)

    def EnviInfo(self):
        print("Môi trường sống:",self.envi)



class Fish (Animal):
    def __init__(self,name,envi,swimspeed):
        super().__init__(name)
        self.envi = envi
        self.swimspeed = swimspeed

    def Info(self):
        print("Cá ,","Tên:", self.name, ", Môi trường sống:",self.envi, ", Avg Move Speed:", self.swimspeed)

    def EnviInfo(self):
        print("Môi trường sống:",self.envi)



class Dog (Mammals):
    def __init__(self,name,envi,runspeed,species):
        super().__init__(name,envi,runspeed)
        self.species = species

    def Info(self):
        print("Chó ,","Tên:", self.name, ", Môi trường sống:", self.envi, ", Loài:",self.species, ", Avg Move Speed:", self.runspeed)

    def SpeciesInfo(self):
        print("Loài:",self.species)



class Cat (Mammals):
    def __init__(self, name, envi, runspeed, species):
        super().__init__(name, envi, runspeed)
        self.species = species

    def Info(self):
        print("Mèo ,","Tên:", self.name, ", Môi trường sống:", self.envi, ", Loài:", self.species, ", Avg Move Speed:", self.runspeed)

    def SpeciesInfo(self):
        print("Loài:", self.species)



class Shark (Fish):
    def __init__(self, name, envi, swimmspeed, species):
        super().__init__(name, envi, swimmspeed)
        self.species = species

    def Info(self):
        print("Cá mập ,","Tên:", self.name, ", Môi trường sống:", self.envi, ", Loài:", self.species, ", Avg Move Speed:", self.swimspeed)

    def SpeciesInfo(self):
        print("Loài:", self.species)



class Carp (Fish):
    def __init__(self, name, envi, swimmspeed, species):
        super().__init__(name, envi, swimmspeed)
        self.species = species

    def Info(self):
        print("Cá chép ,","Tên:", self.name, ", Môi trường sống:", self.envi, ", Loài:", self.species, ", Avg Move Speed:", self.swimspeed)

    def SpeciesInfo(self):
        print("Loài:", self.species)



# First = Animal("AAAAAAA")
# First.Info()
# First.NameInfo()

# Second = Mammals("BBBBBBB","Dưới nước",10)
# Second.Info()
# Second.NameInfo()
# Second.EnviInfo()

# Second = Fish("CCCCCCC","Trên cạn",20)
# Second.Info()
# Second.NameInfo()
# Second.EnviInfo()

# Third = Dog("DDDDDDD","Trên cạn",30,"Chó cỏ")
# Third.Info()
# Third.NameInfo()
# Third.EnviInfo()
# Third.SpeciesInfo()

# Forth = Cat("EEEEEEE","Trên cạn",40,"Mèo nhà")
# Forth.Info()
# Forth.NameInfo()
# Forth.EnviInfo()
# Forth.SpeciesInfo()

# Fifth = Shark("FFFFFFF","Dưới nước",50,"Cá mập con")
# Fifth.Info()
# Fifth.NameInfo()
# Fifth.EnviInfo()
# Fifth.SpeciesInfo()

Sixth = Carp("GGGGGGG","Dưới nước",60,"Cá chép đồng")
Sixth.Info()
Sixth.NameInfo()
Sixth.EnviInfo()
Sixth.SpeciesInfo()