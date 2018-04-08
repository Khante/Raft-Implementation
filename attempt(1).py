import time
import sys
import threading
import os
import zmq
from concurrent.futures import ThreadPoolExecutor
import signal
import os
import random

def setValue(value):
    global state
    global threadArray
    if (state!=value and (value==0)):
        state = value
        threadArray.append(threading.Thread(target=candidateServer))
        threadArray.append(threading.Thread(target=candidateClient))
        threadArray[len(threadArray)-2].start()
        threadArray[len(threadArray)-1].start()
    elif(state!=value and (value==1)):
        print("in Follower")
        state = value
        threadArray.append(threading.Thread(target=followerServer))
        #threadArray.append(threading.Thread(target=followerClient))
        #executor.submit(followerServer)
        #threadArray[len(threadArray)-2].start()
        threadArray[len(threadArray)-1].start()
    elif(state!=value and (value==2)):
        print("in Server")
        state = value
        #executor = ThreadPoolExecutor(max_workers=2)
        threadArray.append(threading.Thread(target=leaderServer))
        threadArray.append(threading.Thread(target=leaderClient))
        threadArray[len(threadArray)-2].start()
        threadArray[len(threadArray)-1].start()
    else:
        pass

def candidateClient():
    global identity
    global state
    global currentTerm
    global socketBindArray
    global socketSendArray
    global votedFor
    global votedDuringTerm
    global votesGotten
    global logEntryNumber
    global clientBindArray
    global socketClientSendArray
    while state == 0:
        for i in range(len(socketSendArray)):
                if (i+1)!=int(identity):
                    print("sending to all that I need votes")
                    socketSendArray[i].send_json(str(identity) + ":needvotes")
        time.sleep(1)
    

def candidateServer():
    global identity
    global state
    global currentTerm
    global socketBindArray
    global socketSendArray
    global votedFor
    global votedDuringTerm
    global votesGotten
    global logEntryNumber
    global clientBindArray
    global socketClientSendArray
    currentTerm +=1 #increase current term
    votesGotten = 1
    votedFor.append(int(identity)) #keep track of current term and your own vote for yourself
    votedDuringTerm.append(currentTerm)
    timer1 = time.time()
    while state == 0:
        message = [0,0,0,0,0]
        print("we candidate now")
        for i in range(len(socketBindArray)):
            if (i+1)!=int(identity):
                message[i] = socketBindArray[i].recv_json()
                timer2 = time.time()
                if message[i].split(':')[1] == 'becomeleader': #need to change this
                    print("I got one vote")
                    votesGotten +=1
                    if(votesGotten>=2):
                        setValue(2)
                        break
                elif message[i].split(':')[1] == 'heartbeat': #need to change this
                    print("getting heartbeats")
                    socketSendArray[int(message[i].split(':')[0])-1].send_json(str(identity)+":ack")
                    setValue(1) #leader came back online or someone else got elected so go back to being a follower
                    #signal.alarm(5)
                    break
        #if timer2 - timer1 > 3: # time for re election

#NEED FOLLOWER CLIENT FOR INITIAL STATE I GUESS
def followerServer():
    global identity
    global state
    global currentTerm
    global socketBindArray
    global socketSendArray
    global votedFor
    global votedDuringTerm
    global votesGotten
    global logEntryNumber
    global clientBindArray
    global socketClientSendArray
    while state == 1:
        print("we here bois")
        message = [0,0,0,0,0]
        timer1 = time.time()
        for i in range(len(socketBindArray)):
            if (i+1)!=int(identity):
                #print(int(port)+i+1)
                message[i] = socketBindArray[i].recv_json()
                print(message[i])
                if message[i].split(':')[1] == 'heartbeat': #need to change this
                    print("getting heartbeats")
                    timer2 = time.time()
                    #socketSendArray[int(message[i].split(':')[0])-1].send_json(str(identity)+":ack")
                    #signal.alarm(5)
                    break
                elif message[i].split(':')[1] == 'needvotes': #keep entry which says which one you voted for #you need to check for election term
                    if(len(votedDuringTerm) == 0):
                        print("sending go ahead become leader" + str(int(message[i].split(':')[0])-1))
                        socketSendArray[int(message[i].split(':')[0])-1].send_json(str(identity)+":becomeleader")
                        votedFor.append( int(message[i].split(':')[0]) )
                        votedDuringTerm.append( currentTerm )
                        break
                    elif( votedDuringTerm[-1] != currentTerm):
                        print("sending go ahead become leader" + str(int(message[i].split(':')[0])-1))
                        socketSendArray[int(message[i].split(':')[0])-1].send_json(str(identity)+":becomeleader")
                        votedFor.append( int(message[i].split(':')[0]) )
                        votedDuringTerm.append( currentTerm )
                        #signal.alarm(5)
                        break
                    else:
                        print("sending you will suck as leader" + str(int(message[i].split(':')[0])-1))
                        socketSendArray[int(message[i].split(':')[0])-1].send_json(str(identity)+":youwillsuckasleader")
                else:
                    print("on my way to becoming a candidate")
                    setValue(0) #havent seen heartbeat nor have we seen needVotes

def leaderServer():
    global identity
    global state
    global currentTerm
    global socketBindArray
    global socketSendArray
    global votedFor
    global votedDuringTerm
    global votesGotten
    global logEntryNumber
    global clientBindArray
    global socketClientSendArray
    #j = 0
    while(True):
        heartbeatMessage = str(identity) + ":heartbeat" 
        for i in range(len(socketSendArray)):
            if (i+1)!=int(identity):
                socketSendArray[i].send_json(heartbeatMessage)
        #print("lol")
        time.sleep(1) #this sleep duration should be less than others

def leaderClient():
    global identity
    global state
    global currentTerm
    global socketBindArray
    global socketSendArray
    global votesGotten
    global logEntryNumber
    global clientBindArray
    global socketClientSendArray
    #if you have failed and you get a heartbeat message from others then you should go to being a follower
    while(state==2):
        message = [0,0]
        for i in range(len(clientBindArray)):
            message[i] = clientBindArray[i].recv_json().split(':')[1]
        if message[0] == "blocking_with_left" and message[1] == "punch_with_right":
            pass
        elif message[0] == "blocking_with_left" and message[1] == "punch_with_left":
            pass
        elif message[0] == "blocking_with_left" and message[1] == "blocking_with_left":
            pass
        elif message[0] == "blocking_with_left" and message[1] == "blocking_with_right":
            pass
        elif message[0] == "blocking_with_right" and message[1] == "punch_with_right":
            pass
        elif message[0] == "blocking_with_right" and message[1] == "punch_with_left":
            pass
        elif message[0] == "blocking_with_right" and message[1] == "blocking_with_right":
            pass
        elif message[0] == "blocking_with_right" and message[1] == "blocking_with_left":
            pass
        elif message[0] == "punch_with_right" and message[1] == "punch_with_right":
            pass
        elif message[0] == "punch_with_right" and message[1] == "punch_with_leftt":
            pass
        elif message[0] == "punch_with_right" and message[1] == "blocking_with_right":
            pass
        elif message[0] == "punch_with_right" and message[1] == "blocking_with_left":
            pass
        elif message[0] == "punch_with_left" and message[1] == "punch_with_right":
            pass
        elif message[0] == "punch_with_left" and message[1] == "punch_with_left":
            pass
        elif message[0] == "punch_with_left" and message[1] == "blocking_with_right":
            pass
        elif message[0] == "punch_with_left" and message[1] == "blocking_with_left":
            pass
        else:
            pass



def clientClient():
    global identity
    global state
    global currentTerm
    global socketBindArray
    global socketSendArray
    global votedFor
    global votedDuringTerm
    global votesGotten
    global logEntryNumber
    global clientBindArray
    global socketClientSendArray
    print("Block with left by pressing q")
    print("Block with right by pressing w")
    print("Punch with left by pressing o")
    print("Punch with right by pressing p")
    while(True):
        user_input = input()
        if(user_input == 'q'): #block with left
            #setValue('blocking_with_left')
            for i in range(len(socketSendArray())):
                socketSendArray[i].send_json(str(identity)+ ":blocking_with_left")
            pass
        elif(user_input=='w'): #block with right
            #setValue('blocking_with_right')
            for i in range(len(socketSendArray())):
                socketSendArray[i].send_json(str(identity)+ ":blocking_with_right")
            pass
        elif(user_input == 'o'): #punch with left
            #setValue('non_blocking')
            socketSend.send_json('punch_with_left')
            for i in range(len(socketSendArray())):
                socketSendArray[i].send_json(str(identity)+ ":punch_with_left")
            time.sleep(1)
            pass
        elif(user_input == 'p'): #punch with right
            #setValue('non_blocking')
            socketSend.send_json('punch_with_right')
            for i in range(len(socketSendArray())):
                socketSendArray[i].send_json(str(identity)+ ":punch_with_right")
            time.sleep(1)
            pass
        else:
            print("Please follow the instructions dude")
    

def clientServer():
    global identity
    global state
    global currentTerm
    global socketBindArray
    global socketSendArray
    global votedFor
    global votedDuringTerm
    global votesGotten
    global logEntryNumber
    global clientBindArray
    global socketClientSendArray
    while(True):
        incoming = someSocketBind.recv_json()
        if(incoming == 'punch_with_left'):

if __name__ == "__main__":
    global identity
    identity = sys.argv[1]
    print(identity)
    global state
    state = 100
    global currentTerm
    currentTerm = 0
    global threadArray
    threadArray = []
    global votesGotten
    votesGotten = 0
    global votedFor
    global votedDuringTerm
    votedFor = []
    votedDuringTerm = []
    global logEntryNumber
    logEntryNumber = 0
    port = "6000"
    ipAddresses = ['10.142.0.2','10.142.0.3','10.142.0.4','10.142.0.5','10.142.0.6'] #list of 5 ip addresses of servers
    contextBindOne, contextBindTwo, contextBindThree, contextBindFour, contextBindFive = zmq.Context(), zmq.Context() , zmq.Context() , zmq.Context() ,zmq.Context()
    socketBindOne, socketBindTwo, socketBindThree, socketBindFour, socketBindFive = contextBindOne.socket(zmq.PAIR), contextBindTwo.socket(zmq.PAIR), contextBindThree.socket(zmq.PAIR), contextBindFour.socket(zmq.PAIR), contextBindFive.socket(zmq.PAIR)
    global socketBindArray
    socketBindArray = [socketBindOne, socketBindTwo, socketBindThree, socketBindFour, socketBindFive]
    for i in range(len(socketBindArray)):
        if (i+1)!=int(identity):
            print(int(port)+i+1)
            socketBindArray[i].bind("tcp://*:%s" % str(int(port)+i+1))
    ###########################################
    global clientBindArray
    ipAddresses2 = ['10.142.0.7','10.142.0.8'] of clients
    contextClientBindSix, contextClientBindSeven = zmq.Context(), zmq.Context()
    socketClientBindSix, socketClientBindSeven = contextClientBindSix(zmq.PAIR), contextClientBindSeven(zmq.PAIR)
    clientBindArray = [socketClientBindSix, socketClientBindSeven]
    for i in range(len(clientBindArray)):
        clientBindArray[i].bind("tcp://*:%s" % str(int(port)+i+5+1)) #binds to 6006, 6007
    ########################
    contextSendOne, contextSendTwo, contextSendThree, contextSendFour, contextSendFive = zmq.Context(), zmq.Context() , zmq.Context() , zmq.Context() ,zmq.Context()
    socketSendOne, socketSendTwo, socketSendThree, socketSendFour, socketSendFive = contextSendOne.socket(zmq.PAIR), contextSendTwo.socket(zmq.PAIR), contextSendThree.socket(zmq.PAIR), contextSendFour.socket(zmq.PAIR), contextSendFive.socket(zmq.PAIR)
    global socketSendArray
    socketSendArray = [socketSendOne, socketSendTwo, socketSendThree, socketSendFour, socketSendFive]
    for i in range(len(socketSendArray)):
        if (i+1)!=int(identity):
            #print(int(port)+i+1)
            socketSendArray[i].connect("tcp://" + ipAddresses[i]+ ":%s" % str(int(port)+int(identity)))

    ######################################
    contextClientSendOne, contextClientSendTwo = zmq.Context(), zmq.Context()
    socketClientSendOne, socketClientSendTwo = contextClientSendOne(zmq.PAIR), contextClientSendTwo(zmq.PAIR)
    global socketClientSendArray
    socketClientSendArray = [socketClientSendOne, socketClientSendTwo]
    for i in range(socketClientSendArray):
        socketClientSendArray.connect("tcp://" + ipAddresses2[i]+ ":%s" % str(int(port)+int(identity)))  #client identities have to be 6 and 7
    ###############################
    time.sleep(6)
    setValue(sys.argv[2]) #enter information as programName identity(from o to 5) leader etc