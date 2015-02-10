import requests
from datetime import datetime
import time
from bs4 import BeautifulSoup
import pymysql

url = 'http://cad.chp.ca.gov/Traffic.aspx'

wuk = '581d91007fdb4ee7'
# http://www.wunderground.com/weather/api/d/581d91007fdb4ee7/edit.html
# http://api.wunderground.com/api/581d91007fdb4ee7/conditions/q/CA/San_Francisco.json

dispatchCenters = [
    'BFCC',  # Bakersfield
    'BSCC',  # Barstow
    'BICC',  # Bishop
    'BCCC',  # Border
    'CHCC',  # Chico
    'ECCC',  # El Centro
    'FRCC',  # Fresno
    'GGCC',  # Golden Gate
    'HMCC',  # Humboldt
    'ICCC',  # Indio
    'INCC',  # Inland
    'LACC',  # Los Angeles
    'MRCC',  # Merced
    'MYCC',  # Monterey
    'OCCC',  # Orange
    'RDCC',  # Redding
    'SACC',  # Sacramento
    'SLCC',  # San Luis Obispo
    'SKCCSTCC',  # Stockton
    'SUCC',  # Susanville
    'TKCC',  # Truckee
    'UKCC',  # Ukiah
    'VTCC',  # Ventura
    'YKCC',  # Yreka
]

# Events we want to record (ignore construction, weather, etc events)
#
recordableEvents = [
    'Trfc Collision',
    'Hit and Run',
    'Fatality',
    'Fire',
    'Hazard',
]

# Form data for submission
#
payload = {
    'ddlcomcenter': 'none',
    'ListMap': 'radList',
    'ddlResources': 'Choose One',
    'ddlSearches': 'Choose One',
    '__EVENTTARGET': '',
    '__EVENTARGUMENT': '',
    '__LASTFOCUS': '',
}

# This will be used to gather secondary data from incidents. This data may or may not be present on the CHP server.
#
levelTwoLinks = []
levelTwoID = {}

# For debugging - save a buffer to disk for examination
#
def saveToFile(doc):
    file_ = open('debug.txt', 'w')
    file_.write(doc)
    file_.close()


# Only record events in qualified list
#
def qualifies(data):
    for strevent in recordableEvents:
        if strevent in data:
            return True
    return False


# fixup orphaned end time where incident p
#
def fixupTime():
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='pass', db='chplog_db', autocommit=True)
    cur = conn.cursor()
    sql = "UPDATE TBL_Incidents set endtime = NOW() where startime = endtime;"
    cur.execute(sql)
    cur.close()
    conn.close()

# store data into MySQL database
#
def storeDetails(data, idx, k):
    day_of_year = str(datetime.now().timetuple().tm_yday)
    callCenter = dispatchCenters[idx]
    incidentID = day_of_year + levelTwoID[levelTwoLinks[k]]

    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='pass', db='chplog_db', autocommit=True)
    cur = conn.cursor()

    data2 = conn.escape(data)
    callCenter2 = conn.escape(callCenter)
    incidentID2 = conn.escape(incidentID)

    sql = "UPDATE TBL_Incidents set DetailText = " + data2 + "where dispatchcenter = " + callCenter2 + " and CHPIncidentID = " + incidentID2 + ";"
    cur.execute(sql)

    cur.close()
    conn.close()


# store data into MySQL database
#
def storeRecord(data, rowIndex):
    itype = data[4]

    if qualifies(itype):

        conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='pass', db='chplog_db', autocommit=True)
        cur = conn.cursor()

        callCenter = data[0]
        incidentID = data[2]

        # used in the form as a parameter to obtain more detail about this incident
        #
        rtext = 'Select$' + str(rowIndex)
        global levelTwoID
        global levelTwoLinks

        levelTwoLinks.append(rtext)
        levelTwoID[rtext] = incidentID

        loc1 = data[5]
        loc2 = data[6]
        loc3 = data[7]
        location = loc1 + ' ' + loc2 + ' - ' + loc3
        loctext = conn.escape(location)
        loctext.encode()

        # strip junk characters coming from chp server
        #
        loctext = str.replace(loctext, "�", " ")

        # CHP incident ids roll over at midnight, so we are prepending the
        # julian date as part of the iid for the database
        # This eliminates incorrect updates going to an older record.
        #
        idoy = datetime.now().timetuple().tm_yday
        day_of_year = str(idoy)
        iid = day_of_year + incidentID

        # Check if this event is already in the database
        #
        sql = "SELECT COUNT(*) FROM TBL_Incidents where dispatchcenter = \'" + callCenter + "\' and CHPIncidentID = \'" + iid + "\';"

        cur.execute(sql)
        result = 0

        for row in cur:
            result = row[0]

        doInsert = False

        # if not in the database, insert it
        if result == 0:
            doInsert = True
            #
            # Check if this event is a day overlap event. Check incident id, location, dispatch center match for yesterday
            #
            if idoy == 0:
                doy = '365'
            else:
                doy = str(idoy - 1)cd de
            iid2 = doy + incidentID
            sql = "SELECT COUNT(*) FROM TBL_Incidents where dispatchcenter = \'" + callCenter + "\' and CHPIncidentID = \'" + iid2 + "\' and location = " + loctext + ";"
            cur.execute(sql)
            result = 0
            for row in cur:
                result = row[0]
            #
            # Found matching incdent from prior day. Use data to update only
            #
            if result == 1:
                doInsert = False
                iid = iid2

        # if not in the database, insert it
        if doInsert:
            sql = "insert into TBL_Incidents(startime,endtime,dispatchcenter,CHPIncidentID,type,location) values (NOW(), NOW(),\'" + callCenter + "\',\'" + iid + "\',\'" + itype + "\'," + loctext + ");"
        else:
            sql = "update TBL_Incidents set endtime = NOW(), type = \'" + itype + "\' where dispatchCenter = \'" + callCenter + "\' and CHPIncidentID = \'" + iid + "\'"

        saveToFile(sql)
        cur.execute(sql)

        cur.close()
        conn.close()


# parse the main html document
#
def parseDom(doc, index):
    # Extract the data from html, if present
    #
    bs = BeautifulSoup(doc.encode('utf-8', 'ignore'))

    # Look for the gvIncidents table
    #
    table = bs.find(lambda tag: tag.name == 'table' and tag.has_key('id') and tag['id'] == "gvIncidents")

    if table is not None:
        cnt = 0
        rowIndex = -1  # first row is the header
        rows = table.find_all(lambda tag: tag.name == 'tr')
        for tr in rows:
            cols = tr.findAll('td')
            list = []
            list.append(dispatchCenters[index])
            for td in cols:
                text = ''.join(td.find(text=True))
                list.append(text)
                cnt += 1
            if cnt:
                storeRecord(list, rowIndex)
            rowIndex += 1


# parse the main html documention
#
def parseDetails(doc, index, k):
    details = ''
    # Extract the data from html, if present
    #
    bs = BeautifulSoup(doc)

    # Look for the gvIncidents table
    #
    table = bs.find(lambda tag: tag.name == 'table' and tag.has_key('id') and tag['id'] == "tblDetails")

    if table is not None:
        rows = table.find_all(lambda tag: tag.name == 'tr')
        for tr in rows:
            cols = tr.findAll('td')
            for td in cols:
                text = ''.join(td.find(text=True))
                details += text
            details += '\n'
    storeDetails(details, index, k)


# main loop
#
def extractData():
    for index in range(len(dispatchCenters)):

        payload['ddlcomcenter'] = dispatchCenters[index]
        payload['__EVENTTARGET'] = ''
        payload['__EVENTARGUMENT'] = ''

        # reference the shared array here, clear it
        #
        global levelTwoLinks
        levelTwoLinks = []

        # Send a POST request to the url with the form data
        #
        response = requests.post(url, payload)
        doc = response.text

        parseDom(doc, index)

        # gather the secondary incident data, if any
        #
        payload['__EVENTTARGET'] = 'gvIncidents'

        for k in range(len(levelTwoLinks)):
            payload['__EVENTARGUMENT'] = levelTwoLinks[k]

            # secondary post
            #
            s = requests.Session()
            s.headers.update({'Referer': 'http://cad.chp.ca.gov/traffic.aspx'})
            s.headers.update(
                {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:32.0) Gecko/20100101 Firefox/32.0`'})
            response = requests.post(url, payload)
            doc = response.text

            parseDetails(doc, index, k)

# early am
#
def isEarlyAm():
    now = datetime.now()
    seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()

    # midnight to 12:15
    #
    if seconds_since_midnight < 1200:
        return True
    return False


# IsNightTime
#
def isNightTime():
    now = datetime.now()
    seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()

    # midnight to 6am
    #
    if seconds_since_midnight < 21600:
        return True
    return False

# main entry
#
if __name__ == '__main__':
    # loop forever
    idx = 1
    while 1:
        print("Getting data for iteration " + str(idx))
        extractData()
        print("Start Delay")
        if isNightTime():
            time.sleep(900)  # wait 15 minutes
        else:
            time.sleep(600)  # wait 10 minutes
        fixupTime()
        idx += 1

