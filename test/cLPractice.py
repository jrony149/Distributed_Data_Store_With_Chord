from CircularList import CreateList


def main():

    cl = CreateList()

    for x in range(7):
        cl.add(x)

    cl.moveHead()

    for x in range(7):
        print(cl.getCursorData())
        cl.moveNext()
    
    cl.moveHead()

    for x in range(7):
        print(cl.getCursorData())
        cl.movePrevious()







if __name__ == "__main__":
    main()
