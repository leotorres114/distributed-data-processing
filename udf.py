if __name__ != '__lib__':
    def outputSchema(empty):
        return lambda x: x

@outputSchema("fromAverage:double")
def getAverage(passenger_count, fare):
    passengerSum = 0.0
    fareSum = 0.0
    #create a sum for passenger_count
    for eachpass in passenger_count:
        passengerSum += eachpass[0]
    #create a sum for fare
    for eachfare in fare:
        fareSum += eachfare[0]
    #return 0 if either are 0
    if passengerSum == 0:
        return 0;
    elif fareSum ==0:
        return 0;
    else:
        return fareSum / passengerSum #if not 0, returns avg. fare density
