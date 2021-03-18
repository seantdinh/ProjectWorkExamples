import tabula
import glob
import pandas as pd
from pathlib import Path
import re
import pdfplumber
import numpy as np

pd.set_option('display.max_columns', 5000)
pd.set_option('display.width', 1000000)

path = r'Statements\\202010'  # use your path
all_files = glob.glob(path + "/*.pdf")
processor = ['PBB','PWF']
f1 = 'F1'
li = []
listforfeesquantum = []
listforsynovous = []
listforhumbdolt = []
sales_humbdolt = []
discount_humboldt = []
monthend_humbdolt = []
combine_sales_refund = []
refund_humboldt = []
for paysafe in processor:
    for filename in all_files:
        if paysafe in filename:
            tables = tabula.read_pdf(filename, pages='all')
            tablecount = len(tables)
            summary = tables[0]
            s = summary['(Total Sales You Submitted - Refunds = Total Amount You Submitted)'].str.split(expand=True)
            s = s.iloc[3:]
            s = s[[0, 1, 3, 4]]
            s = s.iloc[[-1]].reset_index(drop=True)
            s['Corporation'] = Path(filename).resolve().stem
            match = re.search("([0-9]{4}[0-9]{2})", Path(filename).resolve().stem)
            if match is not None:
                s['Date'] = match.group()
            else:
                pass
            #reserve
            for i in range(0, tablecount):
                reserve = tables[i]
                if (reserve.iloc[:,0].str.contains('Reserve Amount')).any():
                    reserve = reserve[reserve['ADJUSTMENTS/CHARGEBACKS'].str.contains('Reserve Amount')]
                    reserve = reserve.iloc[:, 1].reset_index(drop=True)
                    s = s.assign(**{'Reserve Amount': reserve})
            #Chargeback Count Number
            for i in range(0, tablecount):
                chargebackscount = tables[i]
                if (chargebackscount.iloc[:,1].str.contains('CHARGEBACKS')).any():
                    chargebackscount = chargebackscount[chargebackscount.iloc[:,1].str.contains('CHARGEBACKS')]
                    chargebackscount = chargebackscount.iloc[:, 2].reset_index(drop=True)
                    s = s.assign(**{'Chargeback Count': chargebackscount})
            # pdfplumber - card fees discount and misc fees
            pdf = pdfplumber.open(filename)
            table = pdf.pages[0].extract_table()
            cardfee = pd.DataFrame(table)
            cardfee = cardfee[0].str.split('-', expand=True)
            cardfee = cardfee[cardfee[0].str.contains('Month End Charge|Less Discount Paid')]
            cardfee[1] = cardfee[1].str.replace('$', '')
            cardfee[1] = cardfee[1].str.replace(',', '').astype(float)
            cardfee = cardfee.T
            new_header = cardfee.iloc[0]  # grab the first row for the header
            cardfee = cardfee[1:]  # take the data less the header row
            cardfee.columns = new_header  # set the header row as the df header
            discountfee = cardfee.iloc[:,0].reset_index(drop=True)
            monthendfee = cardfee.iloc[:,1].reset_index(drop=True)
            s = s.assign(**{'Fee - Discount': discountfee, 'Fee - Month End Misc': monthendfee})

            # print(s)
            pdf.close()
            li.append(s)

for filename in all_files:
    if f1 in filename:
        # gross sales, refunds, count
        tables = tabula.read_pdf(filename, pages='2', area=[[258, 34, 400, 590]], multiple_tables=False, guess=False)
        tables = pd.DataFrame(np.concatenate(tables))
        tables = tables[[1, 3, 5]]
        tables = tables.iloc[[-1]].reset_index(drop=True)
        tables.rename(columns={1: 0, 3: 1, 5: 3}, inplace=True)
        tables['Corporation'] = Path(filename).resolve().stem
        match = re.search("([0-9]{4}[0-9]{2})", Path(filename).resolve().stem)
        tables[3] = tables[3].replace('-', '', regex=True)
        if match is not None:
            tables['Date'] = match.group()
        else:
            pass

        # fees f1
        fees_table = tabula.read_pdf(filename, pages='all', stream=True, guess=False)
        tablecount = len(fees_table)
        for i in range(0, tablecount):
            fees = fees_table[i]
            if (fees.iloc[:, 0].str.contains('Total Fees')).any():
                positioncount = len(fees.columns) - 1
                fees = fees[fees.iloc[:, 0].str.contains('Fee', na=False)].reset_index(drop=True)
                try:
                    fees = fees.iloc[1:, [0, positioncount]].T
                except:
                    pass
                new_header = fees.iloc[0]  # grab the first row for the header
                fees = fees[1:]  # take the data less the header row
                fees.columns = new_header # set the header row as the df header
                fees = fees.reset_index(drop=True)
                fees["Less Daily Fees"] = fees['Less Daily Fees'].replace('-', '', regex=True)
                fees.rename(columns={'Less Daily Fees': "Fee - Discount", 'Total Merchant Processing Fees': "Fee - Month End Misc"}, inplace=True)

        # reserve f1
        for i in range(0, tablecount):
            reservef1 = fees_table[i]
            positioncount = len(reservef1.columns)
            if positioncount > 4:
                reservef1 = reservef1.fillna(0)
                reservef1.iloc[:,4] = reservef1.iloc[:,4].astype(str)
                if (reservef1.iloc[:, 4].str.contains('Reserve')).any():
                    reservef1 = reservef1[reservef1.iloc[:, 0].str.contains('Total', na=False)]
                    reservef1 = reservef1.iloc[:, [4]]
                    reservef1.columns.values[0] = 'Reserve Amount'
                    reservef1finished = reservef1.iloc[[-1],:].reset_index(drop=True)
                    reservef1finished["Reserve Amount"] = reservef1finished['Reserve Amount'].replace('0.00', '', regex=True)
                    reservef1finished["Reserve Amount"] = reservef1finished["Reserve Amount"].str.replace(',', '').astype(float)

                    # print(reservef1finished)


        whole = [tables, fees, reservef1finished]
        tablesinside = pd.concat(whole, axis=1, ignore_index=False)
        li.append(tablesinside)

for filename in all_files:
    if '- Q -' in filename:
        quantum_tables = tabula.read_pdf(filename, pages='all', stream=True)
        quantum_table_fees = tabula.read_pdf(filename, pages='all', stream=True, guess=False)
        quantum_tables = quantum_tables[1]
        quantum_tables = quantum_tables.iloc[[-1]].reset_index(drop=True)
        quantum_tables.rename(columns={quantum_tables.columns[1]: 0,
                                       quantum_tables.columns[2]: 1,
                                       quantum_tables.columns[4]: 3,
                                       quantum_tables.columns[8]: 'Fee - Discount'}, inplace=True)
        quantum_tables = quantum_tables.iloc[:,[1,2,4,8]]
        quantum_tables['Corporation'] = Path(filename).resolve().stem
        match = re.search("([0-9]{4}[0-9]{2})", Path(filename).resolve().stem)
        if match is not None:
            quantum_tables['Date'] = match.group()
        else:
            pass
        # processing fees Quantum
        tablecount = len(quantum_table_fees)
        for i in range(0, tablecount):
            quantum_table_fees_single = quantum_table_fees[i]
            positioncount = len(quantum_table_fees_single.columns)
            # print(quantum_table_fees_single, quantum_table_fees_single.shape)
            if positioncount == 9:
                if (quantum_table_fees_single.iloc[:, 6].str.contains('Total Fees Due:')).any():
                    feesquantum = quantum_table_fees_single[quantum_table_fees_single.iloc[:, 6].str.contains('Total Fees Due:', na=False)]
                    feesquantum = feesquantum.iloc[:, [8]].reset_index(drop=True)
                    feesquantum.rename(columns={feesquantum.columns[0]: "Fee - Month End Misc",}, inplace=True)
                    combine1 = [quantum_tables, feesquantum]
                    tablesinside1 = pd.concat(combine1, axis=1, ignore_index=False)
                    listforfeesquantum.append(tablesinside1)
                    # print('nine')
                quantum_table_fees_single.iloc[:,7] = quantum_table_fees_single.iloc[:,7].astype(str)
                if (quantum_table_fees_single.iloc[:, 7].str.contains('Total Fees Due:')).any():
                    feesquantum7 = quantum_table_fees_single[quantum_table_fees_single.iloc[:, 7].str.contains('Total Fees Due:', na=False)]
                    feesquantum7 = feesquantum7.iloc[:, [8]].reset_index(drop=True)
                    feesquantum7.rename(columns={feesquantum7.columns[0]: "Fee - Month End Misc",}, inplace=True)
                    combine2 = [quantum_tables, feesquantum7]
                    tablesinside2 = pd.concat(combine2, axis=1, ignore_index=False)
                    listforfeesquantum.append(tablesinside2)
                    # print('seven')
            if positioncount == 3:
                if (quantum_table_fees_single.iloc[:, 1].str.contains('Total Fees Due:')).any():
                    feesquantum3 = quantum_table_fees_single[quantum_table_fees_single.iloc[:, 1].str.contains('Total Fees Due:', na=False)]
                    feesquantum3 = feesquantum3.iloc[:, [2]].reset_index(drop=True)
                    feesquantum3.rename(columns={feesquantum3.columns[0]: "Fee - Month End Misc", }, inplace=True)
                    combine3 = [quantum_tables, feesquantum3]
                    tablesinside3 = pd.concat(combine3, axis=1, ignore_index=False)
                    listforfeesquantum.append(tablesinside3)

listforfeesquantum = pd.concat(listforfeesquantum, axis=0, ignore_index=False)
li.append(listforfeesquantum)

for filename in all_files:
    if '- SY -' in filename:
        tables = tabula.read_pdf(filename, pages='all', stream=True,)
        ghost = tables[1]
        ghost = ghost.iloc[[5], [1, 3, 8]]
        ghost.rename(columns={ghost.columns[1]: 3, ghost.columns[2]: 'Fee - Discount'}, inplace=True)
        sales = ghost['# SALE'].str.split(expand=True).reset_index(drop=True)
        refunds = ghost.iloc[:,[1]].reset_index(drop=True)
        discountfee = ghost.iloc[:,[2]].reset_index(drop=True)

        sales['Corporation'] = Path(filename).resolve().stem
        match = re.search("([0-9]{4}[0-9]{2})", Path(filename).resolve().stem)
        if match is not None:
            sales['Date'] = match.group()
        else:
            pass
        # synovous month end fees
        tables1 = tabula.read_pdf(filename, pages='all', stream=True, guess=False)
        tablecount = len(tables1)
        for i in range(0, tablecount):
            fees = tables1[i]
            fees.iloc[:, 0] = fees.iloc[:, 0].astype(str)
            if (fees.iloc[:, 0].str.contains('NET DISCOUNT DUE')).any():
                monthendsynovous1 = fees[fees.iloc[:, 0]=='FEES DUE'].copy().dropna(axis=1, how='all')
                monthendsynovous1.iloc[:, 1] = monthendsynovous1.iloc[:, 1].astype(str)
                monthendsynovous = monthendsynovous1.iloc[:, [1]].reset_index(drop=True)
                monthendsynovous.columns.values[0] = 'Fee - Month End Misc'
                if monthendsynovous.iloc[0,0] == 'nan':
                    monthendsynovous = monthendsynovous1.iloc[:, [3]].reset_index(drop=True)
                    monthendsynovous.columns.values[0] = 'Fee - Month End Misc'

                concat = [sales, refunds, discountfee, monthendsynovous]
                numbers1 = pd.concat(concat, axis=1, ignore_index=False)
                listforsynovous.append(numbers1)

synovousbringtogether = pd.concat(listforsynovous, axis=0, ignore_index=False)
li.append(synovousbringtogether)


# Humboldt
for filename in all_files:
    if '- HB -' in filename:
        tables1 = tabula.read_pdf(filename, pages='all', stream=True, guess=False)
        a = []
        for tables in tables1:
            check = tables.columns[(tables.values == 'Period Total:').any(0)].tolist()
            check_description= tables.columns[(tables.values == 'Description').any(0)].tolist()
            check_deposit_detail_summary= tables.columns[(tables.values == 'Deposit Detail Summary').any(0)].tolist()
            if not a == check:
                if not a == check_deposit_detail_summary:
                    column_name = check[0]
                    sales_humbdolt = tables[tables[column_name] == 'Period Total:'].copy()
                    sales_humbdolt = sales_humbdolt.iloc[[0], :].reset_index(drop=True)
                    sales_humbdolt = sales_humbdolt.iloc[:, [1, 2, 3]]
                    sales_humbdolt.iloc[:, 2] = sales_humbdolt.iloc[:, 2].str.extract(r'\(([^\)]+)\)', expand=True)
                    # getting file name
                    sales_humbdolt['Corporation'] = Path(filename).resolve().stem
                    match = re.search("([0-9]{4}[0-9]{2})", Path(filename).resolve().stem)
                    sales_humbdolt = sales_humbdolt.drop(sales_humbdolt.columns[2], axis=1)
                    if match is not None:
                        sales_humbdolt['Date'] = match.group()
                    else:
                        pass

                if not a == check_description:
                    column_name = check_description[0]
                    refund_humboldt = tables[tables[column_name].isin(['Merchant Deposit MC','Merchant Deposit VISA','Chg Bk VISA','Chg Bk MC'])].copy()
                    refund_humboldt.iloc[:, 3] = refund_humboldt.iloc[:, 3].str.extract(r'\(([^\)]+)\)', expand=True)
                    refund_humboldt.iloc[:, 3] = refund_humboldt.iloc[:, 3].str.replace(',', '').astype(float)
                    refund_humboldt.loc['Total', :] = refund_humboldt.sum(numeric_only=True, axis=0)
                    refund_humboldt = refund_humboldt.iloc[[-1], 3].reset_index(drop=True)
                    if not (refund_humboldt == 0).values.any():
                        combine_sales_refund = [sales_humbdolt, refund_humboldt]
                        combine_sales_refund = pd.concat(combine_sales_refund, axis=1, ignore_index=False)
        for tables_discount in tables1:
            check_discount_humboldt = tables_discount.columns[(tables_discount.values == 'Total Discount All Card Types').any(0)].tolist()
            if not a == check_discount_humboldt:
                column_name_discount = check_discount_humboldt[0]
            # if (tables_discount.iloc[:, 0].str.contains('Total Discount All Card Types')).any():
                discount_humboldt = tables_discount[tables_discount[column_name_discount]  == 'Total Discount All Card Types'].copy()
                positioncount_discount_hb = len(discount_humboldt.columns)
                if positioncount_discount_hb < 5:
                    discount_humboldt = discount_humboldt.iloc[:, 2].reset_index(drop=True)
                    discount_humboldt = pd.DataFrame({'Fee - Discount': discount_humboldt})
                    concat_discount = [combine_sales_refund, discount_humboldt]
                    finished = pd.concat(concat_discount, axis=1, ignore_index=False)
                    # listforhumbdolt.append(finished)
                if positioncount_discount_hb > 5:
                    discount_humboldt = discount_humboldt.iloc[:, 6].reset_index(drop=True)
                    discount_humboldt = pd.DataFrame({'Fee - Discount': discount_humboldt})
                    concat_discount = [combine_sales_refund, discount_humboldt]
                    finished = pd.concat(concat_discount, axis=1, ignore_index=False)
                    # listforhumbdolt.append(finished)

        for tables_monthend in tables1:
            check_month_end = tables_monthend.columns[(tables_monthend.values == 'Total Charges').any(0)].tolist()
            if not a == check_month_end:
                column_name = check_month_end[0]
                monthend_humbdolt_original = tables_monthend[tables_monthend[column_name] == 'Total Charges'].copy()
                monthend_humbdolt= monthend_humbdolt_original.iloc[:,-1].reset_index(drop=True)
                if monthend_humbdolt.isnull().values.any():
                    monthend_humbdolt = monthend_humbdolt_original.iloc[:, 2].reset_index(drop=True)
                if monthend_humbdolt.notnull().any():
                    monthend_humbdolt= monthend_humbdolt.str.extract(r'\(([^\)]+)\)', expand=True)
                    monthend_humbdolt= monthend_humbdolt.rename(columns={0: 'Fee - Month End Misc'})
                    if len(combine_sales_refund) != 0:
                        if len(discount_humboldt) != 0:
                            concat_monthend = [combine_sales_refund, discount_humboldt, monthend_humbdolt]
                            finished_monthend = pd.concat(concat_monthend, axis=1, ignore_index=False)
                            listforhumbdolt.append(finished_monthend)

finished1 = pd.concat(listforhumbdolt, axis= 0, ignore_index=False)
finished1.rename(columns={'Unnamed: 1': 0, 'Unnamed: 2': 1, 'Deposit Detail': 3}, inplace=True)
finished1.drop_duplicates(subset=[0, 1, 'Corporation'], keep='first', inplace=True)
li.append(finished1)

frame = pd.concat(li, axis=0, ignore_index=True)
print(frame)
frame.to_csv('data/processors.csv')