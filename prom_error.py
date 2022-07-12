from ast import Continue
from csv import list_dialects
from enum import unique
from re import S
import os
import datetime
from time import time

import time
import json

data_path = '/root/prometheus/data/'

class my_dictionary(dict):
    def __init__(self):
        self = dict()
    def add(self,key,value):
        self[key]= value


def prev_stored_counter(filename,currentdate,channeltype):
    if(os.path.exists(filename)):
        file = open(filename,'r')
        filedata = file.read()
        filedata = filedata.replace(" ","")
        s1 = filedata.split(currentdate)
        s1 = s1[1].split(channeltype)
        file.close()
        return int(float((s1[1])))
    return 0


def overall_channel_counter(value, channel, subchannel):
    name = data_path + "overall_error_count_metric"
    stringall = ""
    current_metric_overall_count = 0.0
    if(os.path.exists(name)):
        overallfile = open(name, 'r')
        for x in overallfile:
            if channel not in x:
                stringall += x
            else:
                current_metric_overall_count = float(x.split('=')[-1])
        overallfile.close()
    overallfile = open(name, 'w')
    data = "metric=http_requests_count;channel=" + channel + ";subchannel=" + subchannel + ";action=createbooking;outcome=Failure;count=" + str(1 + current_metric_overall_count) + "\n"
    stringall += data
    overallfile.write(stringall)
    overallfile.close()


def update_current_date_counter(channeltype, value, channel, subchannel):
    current_time = str(datetime.datetime.now())
    currentdate = current_time[0:10]
    filename = data_path + currentdate + "_" + channeltype
    previosvalue = prev_stored_counter(filename, currentdate, channeltype)

    if previosvalue < float(value):
        data = currentdate + "     " + channeltype + "     " + str(float(value))
        outfile = open(filename,'w')
        outfile.write(data)
        outfile.close()
        overall_channel_counter(value, channel, subchannel)

def unique_booking_counter(id, channeltype, channel, subchannel):
    if channeltype == 'bookingdotcom':
        if id not in distinct_tsv_bookingdotcom_map:
            count_map[channeltype] += 1
            distinct_tsv_bookingdotcom_map[id] = 1
            update_current_date_counter("bookingdotcom", count_map[channeltype], channel, subchannel)
            return count_map[channeltype]
    if channeltype == 'airbnb':
        if id not in distinct_tsv_airbnb_map:
            count_map[channeltype] += 1
            distinct_tsv_airbnb_map[id] = 1
            update_current_date_counter("airbnb", count_map[channeltype], channel, subchannel)
            return count_map[channeltype]
    if(channeltype == 'website'):
        if id not in distinct_json_website_map:
            count_map[channeltype] += 1
            distinct_json_website_map[id] = 1
            update_current_date_counter("website", count_map[channeltype], channel, subchannel)
            return count_map[channeltype]
    if channeltype == 'longtail':
        if id not in distinct_json_longtail_map:
            count_map[channeltype] += 1
            distinct_json_longtail_map[id] = 1
            update_current_date_counter("longtail", count_map[channeltype], channel, subchannel)
            return count_map[channeltype]
    if channeltype == 'homeaway':
        if id not in distinct_xml_homeaway_map:
            count_map[channeltype] += 1
            distinct_xml_homeaway_map[id] = 1
            update_current_date_counter("homeaway", count_map[channeltype], channel, subchannel)
            return count_map[channeltype]
    # if id not in list_id:
    #     list_id.append(id)
    #     list_id[0] = list_id[0]+1
    # return list_id[0]

        


def dynamic_file_name(channeltype):
    tm = datetime.datetime.now()
    tm = str(tm)
    name = ""
    if(channeltype == 'website' or channeltype == 'longtail' or channeltype == 'homeaway'):
        currentdate = tm[2:4] + tm[5:7] + tm[8:10]
    else:
        currentdate = tm[0:10]
    if channeltype == 'bookingdotcom':
        name = '/appl/commwr/BookingReport/' + currentdate +'_booking.tsv'
    elif channeltype == 'airbnb':
        name = '/appl/commwr/BookingReport/' + currentdate +'_airbnb.tsv'
    elif channeltype == 'website':
        name = '/appl/log/jsonrpc-intern/' + currentdate + '.jsonrpc-intern.insertrentalcontractv2'
    elif channeltype == 'longtail':
        name = '/appl/log/jsonrpc-partner/' + currentdate + '.jsonrpc-partner.placebookingv1'
    elif channeltype == 'homeaway':
        name = '/appl/log/homeaway/' + currentdate + '.prod.xml.homeaway.booking'

    #print("File name path.............." + name)
    return name


def open_new_file(channeltype, name):
    count_map[channeltype] = 0
    if channeltype == 'bookingdotcom':
        distinct_tsv_bookingdotcom_map.clear()
    if channeltype == 'airbnb':
        distinct_tsv_airbnb_map.clear()
    if channeltype == 'website':
        distinct_json_website_map.clear()
    if channeltype == 'longtail':
        distinct_json_longtail_map.clear()
    if channeltype == 'homeaway':
        distinct_xml_homeaway_map.clear()
    
    file_node = open(name)
    file_map[channeltype] = file_node
    # if(channeltype=='d'):


def tail_logs():
    while True:
        if len(file_map) < 5:
            time.sleep(0.1)
            list = ['bookingdotcom','airbnb','website','longtail','homeaway']
            for x in list:
                if x not in file_map:
                    name = dynamic_file_name(x)
                    if name and os.path.exists(name):
                        #print("Opening file................" + name)
                        open_new_file(x,name)


        for channeltype, file_type in file_map.items():
            #print(channeltype + " ---------------------  ")
            try:
                req_line = file_type.readline()
            except Exception as e:
                req_line = None

            if not req_line:
                #print("Not line................")
                time.sleep(0.1)
                name = dynamic_file_name(channeltype)      
                if name and os.path.exists(name) and os.path.basename(file_map[channeltype].name) != name:
                    #print("Opening New Date file................" + name)
                    open_new_file(channeltype, name)
            else:
                if channeltype =='bookingdotcom':
                    metric = process_tsv_bookingdotcom(req_line)
                elif channeltype == 'airbnb':
                    metric = process_tsv_airbnb(req_line)
                elif channeltype == 'website':
                    metric = process_json_website(req_line)
                elif channeltype == 'longtail':
                    metric = process_json_longtail(req_line)
                elif channeltype == 'homeaway':
                    metric = process_xml_homeaway(req_line)
                
                yield(channeltype, metric)




def process_json_website(line): #line as string
        
    global json_elements

    json_c = 0
    
    if 'params' in line:
        try:
            json_elements = json.loads(line)
        except Exception as e:
            return
    
    if 'ERROR' in line:
        try:
            booking_details = json_elements['params']['HouseCode'] + json_elements['params']['ArrivalDate'] + json_elements['params']['DepartureDate'] + json_elements['params']['CustomerEmail']
        except Exception as e:
            return
        
        json_c = unique_booking_counter(booking_details, 'website', 'website', 'website')
        return json_c




def process_json_longtail(line): #line as string
  
    global json_elements,booking_details,multiple_line_objectdetails
    global objectbool
    json_d = 0 
    if line[0]=='{':
        try :
            json_elements = json.loads(line)
            booking_details = json_elements['params']['HouseCode'] + json_elements['params']['ArrivalDate'] + json_elements['params']['DepartureDate'] + json_elements['params']['CustomerEmail']
            objectbool=False
        except:
            objectbool=True
    if objectbool:
        line=line.replace(" ","")
        if 'HouseCode' in line:
            multiple_line_objectdetails = ""
            housecode = line[13:]
            housecode = housecode[:-2]
            multiple_line_objectdetails = housecode
        if 'ArrivalDate' in line:
            arrivaldate = line[15:]
            arrivaldate = arrivaldate[:-2]
            multiple_line_objectdetails += arrivaldate
        if 'DepartureDate' in line:
            departuredate = line[17:]
            departuredate = departuredate[:-2]
            multiple_line_objectdetails += departuredate
        if 'CustomerEmail' in line:
            customeremail = line[17:]
            customeremail = customeremail[:-2]
            multiple_line_objectdetails += customeremail
    if 'ERROR' in line:
        if objectbool:
            booking_details = multiple_line_objectdetails
            objectbool = False
       
        json_d = unique_booking_counter(booking_details,'longtail', 'longtail', 'longtail')
    
        return json_d




def process_tsv_bookingdotcom(line):
    splitted_logs = line.split()
    #checkin_checkout_housecode_email
    booking_details = splitted_logs[7] + "_" + splitted_logs[4] + "_"  + splitted_logs[5] + "_"  + splitted_logs[17]    
    tsv_count = unique_booking_counter(booking_details, 'bookingdotcom', 'bdc', 'bdc')
    return tsv_count


def process_tsv_airbnb(line):
    splitted_logs = line.split()
    # HuisCode=splitted_logs[2]
    # ArrivalDate=splitted_logs[4]
    # DepartureDate=splitted_logs[5]
    # CustomerEmail=splitted_logs[10]
    booking_details = splitted_logs[2]+ splitted_logs[4] + splitted_logs[5] + splitted_logs[10]
    tsv_c = unique_booking_counter(booking_details,'airbnb','airbnb','airbnb')
    return tsv_c



def process_xml_homeaway(line):
    global booking_details_xml
    log = line.replace(" ","")
    if 'unitExternalId' in log:
        booking_details_xml=""
        houseid = log.split('<unitExternalId>')
        houseid = houseid[1]
        houseid=houseid.split('</unitExternalId>')
        booking_details_xml+=houseid[0]
    if 'beginDate' in log:
        arrival_date = log[11:21]
        booking_details_xml+=arrival_date
    if 'endDate' in log:
        dep_date = log[9:19]
        booking_details_xml+=dep_date
    if 'emailAddress' in log:
        emailaddress = log.split('<emailAddress>')
        emailaddress = emailaddress[1]
        emailaddress=emailaddress.split('</emailAddress>')
        booking_details_xml+=emailaddress[0]
    if 'ERROR:PROPERTY' in log:
        count=unique_booking_counter(booking_details_xml,'homeaway','homeaway','homeaway')
        return count



if __name__=="__main__":
    global count_map
    global distinct_xml_homeaway_map
    global distinct_tsv_bookingdotcom_map
    global distinct_json_website_map
    global distinct_tsv_airbnb_map
    global distinct_json_longtail_map
    distinct_xml_homeaway_map = {}
    distinct_tsv_bookingdotcom_map = {}
    distinct_json_website_map = {}
    distinct_json_longtail_map = {}
    distinct_tsv_airbnb_map = {}
    count_map = {}
    count_map['bookingdotcom'] = 0
    count_map['airbnb'] = 0
    count_map['website'] = 0
    count_map['longtail'] = 0
    count_map['homeaway'] = 0
    global file_map
    file_map = {}
    global objectbool
    objectbool=False
    for data in tail_logs():
        # if data[0]=='b' and str(data[1])!='None':
        # #  if(data[-1]!=0):
        # if data[0]=='c' and str(data[1])!='None':
        # if data[0]=='a' and str(data[1])!='None':
            # z=5
        # z=5
        pass
    

