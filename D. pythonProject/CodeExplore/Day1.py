#Day1
#name = input("Please enter NAME: ")
#print (f"Welcome to {name} Channel")

#Test Day2
from Day2 import EmailProcess, printMsg

def main():
    emails = ["mon@ster.vn", "dummy@asia-plus.net", "xxx@utube.com"]
    for email in emails:
        username, domain = EmailProcess(email)
        printMsg(username,domain)

if __name__ == "__main__":
    main()