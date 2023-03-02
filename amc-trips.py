# amc-trips.py
# March 1, 2023
# Analyze raw ActDB trip listing output from the Activities Database.
# Cross-reference trip listing data with a data listing all approved activity leaders.
# Input data provided in CSV format.
# Operations:
# - Gathers leaders and coleaders separately from each trip
# - Separately tracks leads and coleads for each unique individual
# - Counts the number of trips with status O, F, W, C
# - Counts trips per committee
# - Identifies the earliest and latest trip start date in the data
# - For each leader, counts the number of trips as leader, coleader, and cancelled.

import csv
import datetime as dt

# program constants
# This program expects to find two input files in the 'tripdata' subfolder.
# modify INPUTFILE to contain the base name of the CSV file to be analyzed, leaving off the ".csv" part
TRIPDATA_LOC = 'tripdata'
PATH_SEP = '/'
TRIPFILE = TRIPDATA_LOC+PATH_SEP+'trips'
LEADERFILE = TRIPDATA_LOC+PATH_SEP+'leaders'
LEADER_FIELDS = ['TripLeader1','TripLeader2','TripLeader3','TripLeader4']
COLEADER_FIELDS = ['TripCoLeader1','TripCoLeader2']

# Global Variables
Active_Committees = dict()
Leaders_By_ID = dict()
Leaders_By_Name = dict()
Trips = list()
Open_Trips = 0
Cancelled_Trips = 0
Waitlisted_Trips = 0
Full_Trips = 0
Earliest_Trip = dt.datetime(dt.MAXYEAR,12,31)
Latest_Trip = dt.datetime(dt.MINYEAR,1,1)
Next_Fake_ID = -1

def normalized_name(name_string):
    # convert name_string to lower case and replace whitespaces with underscores
    name_string = name_string.lower().replace(' ','_')
    return name_string

# Leader class
# This class is used to marshal information about leaders.
# We track their unique ID number, email address, and the various normalized names that might be associated with their trips.
class Leader:
    fieldnames = ['ID','PrimaryName','FirstName','LastName','Known','Email','Active','Leads','CoLeads','Cancels','TripDates']

    @staticmethod
    def split_name(name_string):
        # this is an internal utility function that takes a name string with internal underscore separators.
        # it splits the string at the first and (if found) second underscore, returning a list of up to three substrings.
        # we return the list with the individual parts capitalized.
        if '_' not in name_string:
            # probably should raise an exception here
            return name_string
        # break the name_string into a list, using underscore as delimiter. 2 splits at most.
        parts = name_string.split('_',2)
        # return the list of the name parts with each part capitalized
        return [n.capitalize() for n in parts]

    def __init__(self, id_num, email):
        # initialize the state variables of a new Leader instance
        self.id_num = id_num
        self.known = True
        self.email = email
        self.names = list()
        if len(email) > 0:
            self.email = email.lower()
        self.trip_dates = set()
        self.trips_as_leader = 0
        self.trips_as_coleader = 0
        self.trips_cancelled = 0

    def primary_name(self):
        return self.names[0] if len(self.names) > 0 else ''

    def __repr__(self) -> str:
        return f'Leader({self.id_num}, {self.primary_name()})'

    def as_dict(self):
        return { 'ID' : self.id_num,
            'PrimaryName' : self.primary_name(),
            'FirstName' : self.fname(),
            'LastName' : self.lname(),
            'Known' : (1 if self.known else 0),
            'Email' : self.email,
            'Active' : self.active(),
            'Leads' : self.trips_as_leader,
            'CoLeads' : self.trips_as_coleader,
            'Cancels' : self.trips_cancelled,
            'TripDates' : f'"{self.trip_dates}"' if len(self.trip_dates) > 0 else ''}

    def add_name(self, fname, lname, mi):
        if len(fname) == 0 and len(lname) == 0:
            # empty first ane last name, so no op
            return ''
        if len(mi) > 0:
            fullname = normalized_name(fname+' '+mi+' '+lname)
        else:
            fullname = normalized_name(fname+' '+lname)
        if fullname not in self.names:
            self.names += [fullname]
        return fullname

    def add_leader_credit(self,tripdate):
        if tripdate in self.trip_dates:
            print('not giving',self.primary_name(),'credit for duplicate lead on',tripdate)
            return self
        else:
            self.trips_as_leader += 1
            self.trip_dates.add(tripdate)
            return self

    def add_coleader_credit(self,tripdate):
        if tripdate in self.trip_dates:
            print('not giving',self.primary_name(),'credit for duplicate colead on',tripdate)
            return self
        else:
            self.trips_as_coleader += 1
            self.trip_dates.add(tripdate)
            return self

    def active(self):
        # Returns a measure of how "active" this leader is.
        # Represented as the sum of trips_as_leader and trips_as_coleader.
        # No credit for posted but cancelled trips.
        return self.trips_as_leader + self.trips_as_coleader
    
    def fname(self):
        # pull the first element of self.names, find the first underscore, return the string up to it.
        if len(self.names) == 0:
            # no known names so return the empty string
            return ''
        # we get the first name from the first name string contained in the list of names
        name = self.names[0]
        return self.split_name(name)[0]

    def lname(self):
    # pull the first element of self.names, find the first underscore, return the string up to it.
        if len(self.names) == 0:
            # no known names so return the empty string
            return ''
        # we get the first name from the first name string contained in the list of names
        name = self.names[0]
        parts = self.split_name(name)
        if len(parts) == 2:
            # no middle name, so capitalize and return the second element of the split_name list
            return parts[1]
        else:
            return parts[2]

# End of Leader class definition

def dictify(field_names, field_values):
    # field_names is a list of string labels of the field values
    # field_values is a list of values
    # create and return a dictionary that associates field names with field values
    # length of field_values must be <= length of field_names
    if len(field_values) > len(field_names):
        print('in dictify, too many field values!')
        return {}
    new_dict = {}
    for field_num in range(0,len(field_names)):
        this_field = field_names[field_num]
        this_value = field_values[field_num]
        new_dict[this_field] = this_value
    return new_dict

# main program
# Part 1: Load the file of known activity leaders.
# We keep track of all the leaders we've seen, using their unique ID as supplied in the ActDB data
# Note that we have to be watchful for duplicate IDs, which could actually contain new names!
with open(LEADERFILE+'.csv','r') as csvfile:
    print('loading leader list from ',LEADERFILE)
    csvreader = csv.reader(csvfile)
    # load the first row which contains a comma-separated list of field names
    fields = csvreader.__next__()
    # iterate over the remaining rows
    for row in csvreader:
        row_dict = dictify(fields,row)
        # each row contains a ConstituentID which is unique for each unique person
        this_id = row_dict['ConstituentID']
        if this_id in Leaders_By_ID:
            # we have already processed a row with this ID, so grab the existing Leader instance
            leader = Leaders_By_ID[this_id]
        else:
            # first time we've seen this ID so create a new Leader instance
            this_email = row_dict['Email']
            leader = Leader(this_id,this_email)
            Leaders_By_ID[this_id] = leader
        # add names from this record. Two versions are supplied in the record.
        leader.add_name(row_dict['FirstName'],row_dict['LastName'],row_dict['MiddleInitial'])
        leader.add_name(row_dict['REFirstName'],row_dict['RELastName'],row_dict['REMiddleInitial'])
        # associate each name variant found in the data with the same leader record
        for name in leader.names:
            Leaders_By_Name[name] = leader

# at this point, the Leaders_By_ID dictionary should contain a list of unique constitutent IDs associated with a unique Leader instance.
print('loaded data on',len(Leaders_By_ID),'leaders')

# Part 2: Load all the trip listing data.
with open(TRIPFILE+'.csv','r') as csvfile:
    print('analyzing trip listings in',TRIPFILE)
    csvreader = csv.reader(csvfile)
    fields = csvreader.__next__()
    for row in csvreader:
        # parse the row into a dictionary matching field names to field values
        this_trip = dictify(fields,row)
        # save this dictionary in our list of trips
        Trips += [this_trip]
        # Inspect the committee, and update the dictionary mapping committees to trip counts
        this_committee = this_trip['Committee']
        if this_committee in Active_Committees:
            Active_Committees[this_committee] += 1
        else:
            Active_Committees[this_committee] = 1
        this_date = dt.datetime.strptime(this_trip['TripStartDate'],'%m/%d/%Y')
        # update values of Earliest_Trip and Latest_Trip if appropriate
        if this_date < Earliest_Trip:
            Earliest_Trip = this_date
        if this_date > Latest_Trip:
            Latest_Trip = this_date
        this_status = this_trip['TripStatus']
        # update the count of trips with each of four different status values
        if this_status == 'O':
            Open_Trips += 1
        elif this_status == 'C':
            Cancelled_Trips += 1
        elif this_status == 'W':
            Waitlisted_Trips += 1
        elif this_status == 'F':
            Full_Trips += 1

print('statistics on',len(Trips),'trips:')
print('trip data spans',Earliest_Trip.strftime("%m/%d/%Y"),'to',Latest_Trip.strftime("%m/%d/%Y"))
print(Open_Trips,'Open,',Full_Trips,'Full,',Waitlisted_Trips,'Waitlisted,',Cancelled_Trips,'cancelled')

# now let's iterate over all the trips and separately process leaders and coleaders
for trip in Trips:
    status = trip['TripStatus']
    trip_date = trip['TripStartDate']
    # extract the lists of leaders and coleaders. These typically are name strings like 'Jane Q Public'.
    leaders = [trip[leader] for leader in LEADER_FIELDS]
    coleaders = [trip[coleader] for coleader in COLEADER_FIELDS]
    # iterate over the leader names
    for leader_name in leaders:
        if len(leader_name) > 0:
            # non-blank leader name
            normalized = normalized_name(leader_name)
            if normalized not in Leaders_By_Name:
                # this leader name was not previously found in the leader data
                # create a new Leader instance, mark it as not known, set the name, add to Leaders_By_Name
                leader = Leader(Next_Fake_ID,'')
                parts = normalized.split('_',2)
                if len(parts) == 2:
                    leader.add_name(parts[0],parts[1],'')
                else:
                    leader.add_name(parts[0],parts[2],parts[1])
                leader.known = False
                Leaders_By_Name[normalized] = leader
                Leaders_By_ID[Next_Fake_ID] = leader
                Next_Fake_ID -= 1
            else:
                leader = Leaders_By_Name[normalized]
            if status == 'C':
                leader.trips_cancelled += 1
            else:
                leader.add_leader_credit(trip_date)
    for coleader_name in coleaders:
       if len(coleader_name) > 0:
            # non-blank leader name
            normalized = normalized_name(coleader_name)
            if normalized not in Leaders_By_Name:
                # this coleader name was not previously found in the leader data
                # these unapproved leaders are assigned negative 'fake' id numbers 
                leader = Leader(Next_Fake_ID,'')
                parts = normalized.split('_',2)
                if len(parts) == 2:
                    leader.add_name(parts[0],parts[1],'')
                else:
                    leader.add_name(parts[0],parts[2],parts[1])
                leader.known = False
                Leaders_By_Name[normalized] = leader
                Leaders_By_ID[Next_Fake_ID] = leader
                Next_Fake_ID -= 1
            else:
                leader = Leaders_By_Name[normalized]
            if status == 'C':
                leader.trips_cancelled += 1
            else:
                leader.add_coleader_credit(trip_date)

# count all the active leaders in Leaders_By_ID
print('finished processing all the trips, and analyzing leaders and coleaders')

print('writing leader data to',TRIPFILE+'-leaderdata.csv')
with open(TRIPFILE+'-leaderdata.csv', 'w', newline='') as csvfile:
    writer = csv.DictWriter(csvfile,fieldnames=Leader.fieldnames)
    writer.writeheader()
    for leader in Leaders_By_ID:
        writer.writerow(Leaders_By_ID[leader].as_dict())

print('writing committee data to',TRIPFILE+'-committeedata.csv')
with open(TRIPFILE+'-committeedata.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    for c in Active_Committees:
        writer.writerow([c,Active_Committees[c]])

print('done!')
# write it all out to CSV files - statistics, per committee, per leader
