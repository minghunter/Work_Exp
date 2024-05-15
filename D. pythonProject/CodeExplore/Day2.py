def EmailProcess(email):
    username = email[0:email.index("@")]
    domain = email[email.index("@")+1:]
    return [username, domain]

def printMsg(username, domain):
    print(f"User name is: {username}; Email domain is: {domain}")

def main():
    email = input("Please enter ur email address: ").strip()
    username, domain = EmailProcess(email)
    printMsg(username, domain)
# Add .strip() để hàm input chỉ nhận giá trị là chuỗi

if __name__ == "__main__":
    main()