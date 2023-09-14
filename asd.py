ip = "805.266.13.25"


def ip_validation(ip):
    a = ip.split(".")
    if len(a) != 4:
        res = 'invalid'
    else:
        for i in a:
            if int(i) <= 0:
                res = 'invalid'
                print("inv")
            elif int(i) > 255:
                res = 'invalid'
                print("inv")
            else:
                res = 'valid'
                print("v")

    return res


if __name__ == '__main__':
    print(ip_validation(ip))