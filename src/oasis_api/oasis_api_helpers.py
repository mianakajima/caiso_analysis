import requests
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import re
import os
import mysql.connector

def test_func(x):
    return x*3

def get_LMP(start_date, end_date, market_run_id = "DAM", node = "DLAP_PGAE-APND"):
    #start_date and end_date of format YYYYMMDD as a string
    LMP_url = f"http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&" \
              f"startdatetime={start_date}T07:00-0000&" \
              f"enddatetime={end_date}T07:00-0000&version=1&" \
              f"market_run_id={market_run_id}&node={node}"

    r = requests.get(LMP_url)

    #change working directory to folder the script is in
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    # print(os.getcwd())
    # os.chdir('/Users/mnakajim/Documents/Github/caiso_analysis/src/oasis_api')

    #extract zipfile
    with open('./temp_zip.zip', 'wb') as code:
        code.write(r.content)

    with zipfile.ZipFile('./temp_zip.zip', 'r') as zip_ref:
        zip_ref.extractall('./download_data')


    #get name of XML file
    header_filename = r.headers['Content-Disposition']
    zip_file = re.sub('inline; filename=', '', header_filename)
    xml_file = re.sub('.zip;', '.xml', zip_file)
    os.remove('temp_zip.zip')
    #parse XML file

    xml_data = ET.parse(f"download_data/{xml_file}")
    root = xml_data.getroot()

    #extract values of interest
    values = [repdata.text for repdata in root.iter('{http://www.caiso.com/soa/OASISReport_v1.xsd}VALUE')]
    interval_start = [repdata.text for repdata in
                      root.iter('{http://www.caiso.com/soa/OASISReport_v1.xsd}INTERVAL_START_GMT')]
    interval_end = [repdata.text for repdata in
                    root.iter('{http://www.caiso.com/soa/OASISReport_v1.xsd}INTERVAL_END_GMT')]
    interval_num = [repdata.text for repdata in root.iter('{http://www.caiso.com/soa/OASISReport_v1.xsd}INTERVAL_NUM')]
    opr_date = [repdata.text for repdata in root.iter('{http://www.caiso.com/soa/OASISReport_v1.xsd}OPR_DATE')]
    resource_name = [repdata.text for repdata in
                     root.iter('{http://www.caiso.com/soa/OASISReport_v1.xsd}RESOURCE_NAME')]

    lmp_df = pd.DataFrame({'opr_date': opr_date,
                        'interval_start': interval_start,
                        'interval_end': interval_end,
                        'interval_num': interval_num,
                        'resource_name': resource_name,
                        'value': values})

    #convert to correct data types
    lmp_df[['interval_num', 'value']] = lmp_df[['interval_num', 'value']].apply(pd.to_numeric)
    lmp_df[['opr_date', 'interval_start', 'interval_end']] = lmp_df[
        ['opr_date', 'interval_start', 'interval_end']].apply(pd.to_datetime)

    return(lmp_df)


#TODO: make this an object so I can close it?
def import_to_mysql(lmp_df, user, password, host, database):
    # connect to database
    cnx = mysql.connector.connect(user= user , password= password,
                                  host= host,
                                  database= database,
                                  auth_plugin='mysql_native_password')

    cursor = cnx.cursor()

    add_query = ("INSERT INTO lmp"
                 "(interval_start, interval_end, interval_num, resource_name, value)"
                 "VALUES (%s, %s, %s, %s, %s)")

    data = lmp_df[['interval_start', 'interval_end', 'interval_num',
                     'resource_name', 'value']].values.tolist()

    print('Loading into SQL 1000 rows at a time... ')

    for i in range(0, len(data), 1000):
        print(str(i) + "/" + str(len(data)))
        cursor.executemany(add_query, data[i: i + 1000])

    cnx.commit()
    cursor.close()
    cnx.close()
