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
import sys
import os
import datetime as dt

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
        self.committees = list()
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
    
    def add_committee(self,committee_name):
        if not(committee_name in self.committees):
            self.committees += [committee_name]
        return self.committees

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
    if len(field_values) != len(field_names):
        # something isn't right with the input data, just ignore it
        return {}
    new_dict = {}
    for field_num in range(0,len(field_names)):
        this_field = field_names[field_num]
        this_value = field_values[field_num]
        new_dict[this_field] = this_value
    return new_dict

# load leaders from the leaderfile
def load_leaders(leaderfile):
    # Load and process the file of known activity leaders.
    # We keep track of all the leaders we've seen, using their unique ID as supplied in the ActDB data
    # Note that we have to be watchful for duplicate IDs, which could actually contain new names!
    leaders_by_id = dict()
    leaders_by_name = dict()
    with open(leaderfile,'r') as csvfile:
        print('Loading leaders from',leaderfile)
        csvreader = csv.reader(csvfile)
        # load the first row which contains a comma-separated list of field names
        fields = csvreader.__next__()
        # iterate over the remaining rows
        for row in csvreader:
            # 'dictify' this row of data, associating each field in the header with a value
            row_dict = dictify(fields,row)
            if len(row_dict) != 0:
                # each row will contain a ConstituentID that is unique for each unique person
                this_id = row_dict['ConstituentID']
                print("processing ConstituentID", this_id)
                if this_id in leaders_by_id:
                    # we have already processed a row with this ID, so grab the existing Leader instance
                    leader = leaders_by_id[this_id]
                else:
                    # first time we've seen this ID so create a new Leader instance
                    this_email = row_dict['Email']
                    leader = Leader(this_id,this_email)
                    leaders_by_id[this_id] = leader
                # add names from this record. Two versions are supplied in the record.
                leader.add_name(row_dict['FirstName'],row_dict['LastName'],row_dict['MiddleInitial'])
                leader.add_name(row_dict['REFirstName'],row_dict['RELastName'],row_dict['REMiddleInitial'])
                # associate each name variant found in the data with the same leader record
                for name in leader.names:
                    leaders_by_name[name] = leader
                # add the committee affiliation
                leader.add_committee(row_dict['Committee'])

    # at this point, the Leaders_By_ID dictionary should contain a list of unique constitutent IDs associated with a unique Leader instance.
    print('loaded data on',len(leaders_by_id),'leaders')
    # return the two dictionaries
    return [leaders_by_id, leaders_by_name]

def load_trips(tripfile):
    # local variables
    active_committees = dict()
    trips = list()
    open_trips = 0
    cancelled_trips = 0
    waitlisted_trips = 0
    full_trips = 0
    earliest_trip = dt.datetime(dt.MAXYEAR,12,31)
    latest_trip = dt.datetime(dt.MINYEAR,1,1)

    # Load all the trip listing data.
    with open(tripfile,'r') as csvfile:
        print('analyzing trip listings in',tripfile)
        csvreader = csv.reader(csvfile)
        fields = csvreader.__next__()
        for row in csvreader:
            # parse the row into a dictionary matching field names to field values
            this_trip = dictify(fields,row)
            # save this dictionary in our list of trips
            trips += [this_trip]
            # Inspect the committee, and update the dictionary mapping committees to trip counts
            this_committee = this_trip['Committee']
            if this_committee in active_committees:
                active_committees[this_committee] += 1
            else:
                active_committees[this_committee] = 1
            this_date = dt.datetime.strptime(this_trip['TripStartDate'],'%m/%d/%Y')
            # update values of Earliest_Trip and Latest_Trip if appropriate
            if this_date < earliest_trip:
                earliest_trip = this_date
            if this_date > latest_trip:
                latest_trip = this_date
            this_status = this_trip['TripStatus']
            # update the count of trips with each of four different status values
            if this_status == 'O':
                open_trips += 1
            elif this_status == 'C':
                cancelled_trips += 1
            elif this_status == 'W':
                waitlisted_trips += 1
            elif this_status == 'F':
                full_trips += 1

    print('statistics on',len(trips),'trips:')
    print('trip data spans',earliest_trip.strftime("%m/%d/%Y"),'to',latest_trip.strftime("%m/%d/%Y"))
    print(open_trips,'Open,',full_trips,'Full,',waitlisted_trips,'Waitlisted,',cancelled_trips,'cancelled')
    return trips

# program constants
LEADER_FIELDS = ['TripLeader1','TripLeader2','TripLeader3','TripLeader4']
COLEADER_FIELDS = ['TripCoLeader1','TripCoLeader2']

def analyze_trips(trips, leaders_by_name, leaders_by_id):
    # local variables
    next_fake_ID = -1
    # now let's iterate over all the trips and separately process leaders and coleaders
    for trip in trips:
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
                if normalized not in leaders_by_name:
                    # this leader name was not previously found in the leader data
                    # create a new Leader instance, mark it as not known, set the name, add to leaders_by_name
                    leader = Leader(next_fake_ID,'')
                    parts = normalized.split('_',2)
                    if len(parts) == 2:
                        leader.add_name(parts[0],parts[1],'')
                    else:
                        leader.add_name(parts[0],parts[2],parts[1])
                    leader.known = False
                    leaders_by_name[normalized] = leader
                    leaders_by_id[next_fake_ID] = leader
                    next_fake_ID -= 1
                else:
                    leader = leaders_by_name[normalized]
                if status == 'C':
                    leader.trips_cancelled += 1
                else:
                    leader.add_leader_credit(trip_date)
        for coleader_name in coleaders:
            if len(coleader_name) > 0:
                    # non-blank leader name
                    normalized = normalized_name(coleader_name)
                    if normalized not in leaders_by_name:
                        # this coleader name was not previously found in the leader data
                        # these unapproved leaders are assigned negative 'fake' id numbers 
                        leader = Leader(next_fake_ID,'')
                        parts = normalized.split('_',2)
                        if len(parts) == 2:
                            leader.add_name(parts[0],parts[1],'')
                        else:
                            leader.add_name(parts[0],parts[2],parts[1])
                        leader.known = False
                        leaders_by_name[normalized] = leader
                        leaders_by_id[next_fake_ID] = leader
                        next_fake_ID -= 1
                    else:
                        leader = leaders_by_name[normalized]
                    if status == 'C':
                        leader.trips_cancelled += 1
                    else:
                        leader.add_coleader_credit(trip_date)

if __name__ == "__main__":
    # main body of program
    print("Preparing to analyze data from AMC Activities Database")
    leader_file = sys.argv[1]
    if len(sys.argv) != 3:
        print("Missing expected arguments: leaderfile tripfile")
        exit()
    if not(os.path.isfile(leader_file)):
        print(leader_file,"does not exist")
        exit()
    trip_file = sys.argv[2]
    if not(os.path.isfile(trip_file)):
        print(trip_file,"does not exist")
        exit()
   
    # load the leaders
    print("Step 1: loading leaders")
    result = load_leaders(leader_file)
    leaders_by_id = result[0]
    leaders_by_name = result[1]
    
    # load the trips
    print("Step 2: loading trips")
    trips = load_trips(trip_file)
    exit()

    print("Step 3: Analyze trips")
    analyze_trips(trips, leaders_by_name, leaders_by_id)
    print("Done with analysis, writing output files")

    # FIX THIS
    output_file_1 = trip_file_base+'-leaderdata.csv'
    print('Writing leader data to',output_file_1)
    with open(output_file_1, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile,fieldnames=Leader.fieldnames)
        writer.writeheader()
        for leader in leaders_by_id:
            writer.writerow(leaders_by_id[leader].as_dict())

    # FIX THIS
    output_file_2 = trip_file_base+'-committeedata.csv'
    print('Writing committee data to',output_file_2)
    with open(output_file_2, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # FIX THIS
        for c in Active_Committees:
            writer.writerow([c,Active_Committees[c]])

    print('Done!')
