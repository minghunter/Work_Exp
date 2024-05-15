# num1 = float(input("Enter first number: "))
# op = input("Enter operator: ")
# num2 = float(input("Enter second number: "))
#
# if op == "+":
#     print(num1 + num2)
# elif op == "-":
#     print(num1 - num2)
# elif op == "/":
#     print(num1 / num2)
# elif op == "*":
#     print (num1 * num2)
# else:
#     print("invalid operator")

# month_conversions = {
#     "Jan": "January",
#     "Feb": "February",
#     "Mar": "March",
#     "Apr": "April",
#     "May": "May",
#     "Jun": "June",
#     "Jul": "July",
#     "Aug": "August",
#     "Sep": "September",
#     "Oct": "October",
#     "Nov": "November",
#     "Dec": "December"
# }
#
# print(month_conversions.get("ad","Not a valid key"))

# i = 0
# while i <=10:
#     print(i)
#     i += 1
#
# print("Done with loop")

# secret_word = "Giraffe"
# guess = ""
# guess_count = 0
# guess_limit = 3
# out_of_guesses = False
#
# while guess != secret_word and not(out_of_guesses):
#     if guess_count < guess_limit:
#         guess = input("Enter guess: ")
#         guess_count += 1
#     else:
#         out_of_guesses = True
#
# if out_of_guesses:
#     print("Out of Guesses, you LOSE!")
# else:
#     print("You win!")

# friends = ["Jim", "Karen", "Kevin"]
# for index in range(len(friends)):
#     print(friends[index])

# for index in range(5):
#     if index == 0:
#         print ("First Iteration")
#     else:
#         print ("Not first")

# def raise_to_power (base_num, power_num):
#     result = 1
#     for index in range(power_num):
#         result = result * base_num
#     return result
#
# print (raise_to_power(3,4))

# number_grid = [
#     [1, 2, 3],
#     [4, 5, 6],
#     [7, 8, 9],
#     [0]
# ]
#
# for row in number_grid:
#     for col in row:
#         print (col)

# def translate(phrase):
#     translation = ""
#     for letter in phrase:
#         if letter.lower() in "aeiou":
#             if letter.isupper():
#                 translation = translation + "G"
#             else:
#                 translation = translation + "g"
#         else:
#             translation = translation + letter
#     return translation
#
# print(translate(input("Enter a phrase: ")))

# try:
#     value = 10/0
#     number = int(input("Enter a number: "))
#     print(number)
# except ZeroDivisionError as Err:
#     print(Err)
# except ValueError:
#     print("Invalid input")