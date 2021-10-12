import requests
import zipfile
import xml.etree.ElementTree as ET
import pandas as pd
import re

def test_func(x):
    return x

def get_LMP(start_date, end_date, market_run_id = "DAM", node = "DLAP_PGAE-APND"):
    #start_date and end_date of format YYYYMMDD as a string
    LMP_url = f"http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&" \
              f"startdatetime={start_date}T07:00-0000&" \
              f"enddatetime={end_date}T07:00-0000&version=1&" \
              f"market_run_id={market_run_id}&node={node}"

    r = requests.get(LMP_url)

    #extract zipfile
    with open('temp_zip.zip', 'wb') as code:
        code.write(r.content)

    with zipfile.ZipFile('temp_zip', 'r') as zip_ref:
        zip_ref.extractall('data') ##TODO: creating a data folder

    #get name of XML file
    header_filename = r.headers['Content-Disposition']
    zip_file = re.sub('inline; filename=', '', header_filename)
    xml_file = re.sub('.zip;', '.xml', zip_file)

    #parse XML file

    xml_data = ET.parse(f"data/{xml_file}")
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

    return(lmp_df)
