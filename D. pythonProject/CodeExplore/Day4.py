def CtoFconverter():
    while True:
        cTemp = input("Please enter C degree: ")
        if cTemp:
            #print(type(cTemp))
            cTemp = float(cTemp) # str to float
            fTemp = round((cTemp * 9/5 + 32),1)
            print(f"{cTemp}C is converted to {fTemp}F")
            break
        else:
            print("cTemp input is empty")
            continue
        
# def CtoFconverter():
#     cTemp = int(input("Please enter C degree: "))
#     if cTemp:
#         fTemp = cTemp * 9/5 + 32
#         print(f"{cTemp}C is converted to {fTemp}F")

def main():
    #print("Test_Function: Tips that should check step by step to ensure every code/function is runable & not error")
    CtoFconverter()

if __name__ == "__main__":
    main()