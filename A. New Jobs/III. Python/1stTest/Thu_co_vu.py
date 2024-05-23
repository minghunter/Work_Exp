from Dong_vat import Dong_vat

class Thu_co_vu(Dong_vat):
    def __init__(self, name, env):
        self.name = name
        self.env = env

    def env_new(self):
        env_new = ""
        env1_new = "Trên cạn"
        env2_new = "Dưới nước"

        while (env_new != env1_new) and (env_new != env2_new):
            env_new = input("Please input environment (Trên cạn OR Dưới nước): ")
            if (env_new != env1_new) and (env_new != env2_new):
                print("Vui lòng nhập lại đúng thông tin")

        print("Its environment of that Animal is:",env_new)
