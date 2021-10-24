
start_date = '20211001'
end_date = '20211010'
market_run_id = "DAM"
node = "DLAP_PGAE-APND"

LMP_url = f"http://oasis.caiso.com/oasisapi/SingleZip?queryname=PRC_LMP&" \
          f"startdatetime={start_date}T07:00-0000&" \
          f"enddatetime={end_date}T07:00-0000&version=1&" \
          f"market_run_id={market_run_id}&node={node}"

r = requests.get(LMP_url)