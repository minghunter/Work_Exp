#Basics Data Types:

'''
Các kiểu dữ liệu cơ bản trong Python: bool, None, int, float, str
'''

'''
Topic #0: bool & None
'''
# [] bool: True & False
var_bool = True

# [] type(): Dynamically typed
#print(type(var_bool))

#Numberically, they're evaluated as integers with value 1 (True), 0 (False)
a = 4 + True
#print(a)

b = False
# if b == 0:
#     print ("B is zero")

# [] None: Phần tử Không
var_none = None
# print(type(var_none))

# None is often used as a placeholder for optional or missing value.
# It evaluates as False in conditionals.
# email_address = None #False
# email_address = "codex@warhammer.40k"

# if email_address:
#     print(f"Email address is {email_address}")
# else:
#     print(f"Email address is empty or {email_address}")


'''
Topic #1: Number (int & float)
'''
# print(type(1)) # int
# print(type(0))
# print(type(-10))

# print(type(0.0)) # float
# print(type(2.3))
# print(type(4E2), 4E2) # 4*10(^2)

# print(10+3) # 13
# print(10-3) # 7
# print(10*3) # 30
# print(10**3) # 10^3 = 10*10*10 = 1000
# print(10/3) # 3.3333333
# print(10//3) # 3
# print(10%3) # 1 vì 10 chia 3 = 3 dư 1

# [] Basic Function (Hàm cơ bản)
print(pow(10,3)) # 10**3 = 1000
print(abs(-6.9)) # 6.9
print(round(6.69)) # 7
print(round(5.468,2)) # 5.47 --> round to nth digit
print(bin(512)) # '0b1000000000' --> binary format
print(hex(512)) # '0x200' -- hexadecimal format