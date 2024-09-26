import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import logging
from logs.Logging_Config import setup_logging
from SlackBot.Static.Lists import *

setup_logging()
logger = logging.getLogger(__name__)

scopes = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(r"SlackBot\External\Credentials.json", scopes=scopes)
client = gspread.authorize(creds)

class Initialization:
    def grab_impvol(external_params):
        output_dict = {}

        for task in external_params:
            workbook = client.open_by_key(task["sheet_id"])
            sheet = workbook.worksheet(task["sheet_name"])
            cell_value = sheet.cell(task["row_number"], task["col_number"]).value
            
            if "ES" in task["sheet_name"]:
                output_dict['es_impvol'] = cell_value
            elif "NQ" in task["sheet_name"]:
                output_dict['nq_impvol'] = cell_value
            elif "RTY" in task["sheet_name"]:
                output_dict['rty_impvol'] = cell_value
            elif "CL" in task["sheet_name"]:
                output_dict['cl_impvol'] = cell_value
                
        es_impvol = float(output_dict['es_impvol'].strip('%'))
        nq_impvol = float(output_dict['nq_impvol'].strip('%'))
        rty_impvol = float(output_dict['rty_impvol'].strip('%'))
        cl_impvol = float(output_dict['cl_impvol'].strip('%'))

        return es_impvol, nq_impvol, rty_impvol, cl_impvol

    def prep_data(files):
        all_variables = {}

        for task in files:  
            if task["header_row"] == 1:
                data = pd.read_csv(task["filepath"], delimiter='\t', header=None)
            elif task["header_row"] == 0:
                data = pd.read_csv(task["filepath"], delimiter='\t')
                data = data.reset_index()
            else:
                raise ValueError("header_row should be either 0 or 1")

            # Do this Data Processing for Every File 
            pd.options.mode.copy_on_write = True
            data.to_string()
            data.columns = data.iloc[task["iloc1"]]
            data = data[task["iloc2"]:]
            existing_columns_to_drop = [col for col in columns_to_drop if col in data.columns]
            data = data.drop(columns=existing_columns_to_drop)
            df_cleaned = data.loc[:, data.columns.notna()]
            data = df_cleaned
            data = data.dropna()
            for columns in task["columns"]:
                data[columns] = data[columns].str.replace(',', '.').astype(float)

            # Get Necessary Variables  
            variables = {}

            match task["name"]:
                case "ES_1":
                    variables['ES_D_OPEN'] = float(data.loc[2, '[ID2.SG1] D_Open'])
                    variables['ES_D_HIGH'] = float(data.loc[2, '[ID2.SG2] D_High'])
                    variables['ES_D_LOW'] = float(data.loc[2, '[ID2.SG3] D_Low'])
                    variables['ES_D_CLOSE'] = float(data.loc[2, '[ID2.SG4] D_Close'])
                    variables['ES_VPOC'] = float(data.loc[2, '[ID1.SG1] VPOC'])
                    variables['ES_PVPOC'] = float(data.loc[2, '[ID9.SG1] P_VPOC'])
                    variables['ES_PRIOR_HIGH'] = float(data.loc[2, '[ID8.SG2] Prior High'])
                    variables['ES_PRIOR_LOW'] = float(data.loc[2, '[ID8.SG3] Prior Low'])
                    variables['ES_PRIOR_CLOSE'] = float(data.loc[2, '[ID8.SG4] Prior_Close'])
                    variables['ES_PERIODIC_RVOL'] = float(data.loc[2, '[ID6.SG1] 30 MIN RELATIVITY'])
                    variables['ES_CUMULATIVE_RVOL'] = float(data.loc[2, '[ID6.SG2] OVERALL RELATIVITY'])
                    variables['ES_TOTAL_RTH_DELTA'] = float(data.loc[2, '[ID4.SG4] Total'])
                case "ES_2":
                    variables['ES_ETH_VWAP'] = float(data.loc[2, '[ID7.SG1] ETH VWAP'])
                    variables['ES_CPL'] = float(data.loc[2, '[ID2.SG1] CPL'])
                    variables['ES_5D_VPOC'] = float(data.loc[2, '[ID4.SG2] 5DVPOC'])
                    variables['ES_20D_VPOC'] = float(data.loc[2, '[ID3.SG2] 20DVPOC'])
                    variables['ES_P_WOPEN'] = float(data.loc[2, '[ID5.SG1] P_WOPEN'])
                    variables['ES_P_WHIGH'] = float(data.loc[2, '[ID5.SG2] P_WHIGH'])
                    variables['ES_P_WLO'] = float(data.loc[2, '[ID5.SG3] P_WLO'])
                    variables['ES_P_WCLOSE'] = float(data.loc[2, '[ID5.SG4] P_WCLOSE'])
                    variables['ES_P_WVPOC'] = float(data.loc[2, '[ID11.SG1] P_WVPOC'])
                    variables['ES_WVWAP'] = float(data.loc[2, '[ID10.SG1] WVWAP'])
                    variables['ES_P_MOPEN'] = float(data.loc[2, '[ID8.SG1] P_MOPEN'])
                    variables['ES_P_MHIGH'] = float(data.loc[2, '[ID8.SG2] P_MHIGH'])
                    variables['ES_P_MLO'] = float(data.loc[2, '[ID8.SG3] P_MLO'])
                    variables['ES_P_MCLOSE'] = float(data.loc[2, '[ID8.SG4] P_MCLOSE'])
                    variables['ES_P_MVPOC'] = float(data.loc[2, '[ID12.SG1] P_MVPOC'])
                    variables['ES_MVWAP'] = float(data.loc[2, '[ID1.SG1] MVWAP'])
                case "ES_3":
                    variables['ES_IB_ATR'] = float(data.loc[2, '[ID2.SG1] IB ATR'])
                    variables['ES_IB_HIGH'] = float(data.loc[1, '[ID1.SG2] IBH'])
                    variables['ES_IB_LOW'] = float(data.loc[1, '[ID1.SG3] IBL'])
                case "ES_4":
                    variables['ES_OVNH'] = float(data.loc[2, '[ID1.SG2] OVN H'])
                    variables['ES_OVNL'] = float(data.loc[2, '[ID1.SG3] OVN L'])
                    variables['ES_TOTAL_OVN_DELTA'] = float(data.loc[2, '[ID3.SG4] OVN Total'])
                case "ES_5":
                    variables['ES_OVNTOIB_HI'] = float(data.loc[1, '[ID1.SG2] OVNTOIB_HI'])
                    variables['ES_OVNTOIB_LO'] = float(data.loc[1, '[ID1.SG3] OVNTOIB_LO'])
                case "ES_6":
                    variables['ES_EURO_IBH'] = float(data.loc[1, '[ID1.SG2] EURO IBH'])
                    variables['ES_EURO_IBL'] = float(data.loc[1, '[ID1.SG3] EURO IBL'])
                case "ES_8":
                    variables['ES_ORH'] = float(data.loc[1, '[ID1.SG2] ORH'])
                    variables['ES_ORL'] = float(data.loc[1, '[ID1.SG3] ORL'])
                case "NQ_1":
                    variables['NQ_D_OPEN'] = float(data.loc[2, '[ID2.SG1] D_Open'])
                    variables['NQ_D_HIGH'] = float(data.loc[2, '[ID2.SG2] D_High'])
                    variables['NQ_D_LOW'] = float(data.loc[2, '[ID2.SG3] D_Low'])
                    variables['NQ_D_CLOSE'] = float(data.loc[2, '[ID2.SG4] D_Close'])
                    variables['NQ_VPOC'] = float(data.loc[2, '[ID1.SG1] VPOC'])
                    variables['NQ_PVPOC'] = float(data.loc[2, '[ID9.SG1] P_VPOC'])
                    variables['NQ_PRIOR_HIGH'] = float(data.loc[2, '[ID8.SG2] Prior High'])
                    variables['NQ_PRIOR_LOW'] = float(data.loc[2, '[ID8.SG3] Prior Low'])
                    variables['NQ_PRIOR_CLOSE'] = float(data.loc[2, '[ID8.SG4] Prior_Close'])
                    variables['NQ_PERIODIC_RVOL'] = float(data.loc[2, '[ID6.SG1] 30 MIN RELATIVITY'])
                    variables['NQ_CUMULATIVE_RVOL'] = float(data.loc[2, '[ID6.SG2] OVERALL RELATIVITY'])
                    variables['NQ_TOTAL_RTH_DELTA'] = float(data.loc[2, '[ID4.SG4] Total'])
                
                case "NQ_2":
                    variables['NQ_ETH_VWAP'] = float(data.loc[2, '[ID7.SG1] ETH VWAP'])
                    variables['NQ_CPL'] = float(data.loc[2, '[ID2.SG1] CPL'])
                    variables['NQ_5D_VPOC'] = float(data.loc[2, '[ID4.SG2] 5DVPOC'])
                    variables['NQ_20D_VPOC'] = float(data.loc[2, '[ID3.SG2] 20DVPOC'])
                    variables['NQ_P_WOPEN'] = float(data.loc[2, '[ID5.SG1] P_WOPEN'])
                    variables['NQ_P_WHIGH'] = float(data.loc[2, '[ID5.SG2] P_WHIGH'])
                    variables['NQ_P_WLO'] = float(data.loc[2, '[ID5.SG3] P_WLO'])
                    variables['NQ_P_WCLOSE'] = float(data.loc[2, '[ID5.SG4] P_WCLOSE'])
                    variables['NQ_P_WVPOC'] = float(data.loc[2, '[ID11.SG1] P_WVPOC'])
                    variables['NQ_WVWAP'] = float(data.loc[2, '[ID10.SG1] WVWAP'])
                    variables['NQ_P_MOPEN'] = float(data.loc[2, '[ID8.SG1] P_MOPEN'])
                    variables['NQ_P_MHIGH'] = float(data.loc[2, '[ID8.SG2] P_MHIGH'])
                    variables['NQ_P_MLO'] = float(data.loc[2, '[ID8.SG3] P_MLO'])
                    variables['NQ_P_MCLOSE'] = float(data.loc[2, '[ID8.SG4] P_MCLOSE'])
                    variables['NQ_P_MVPOC'] = float(data.loc[2, '[ID12.SG1] P_MVPOC'])
                    variables['NQ_MVWAP'] = float(data.loc[2, '[ID1.SG1] MVWAP'])
               
                case "NQ_3":
                    variables['NQ_IB_ATR'] = float(data.loc[2, '[ID2.SG1] IB ATR'])
                    variables['NQ_IB_HIGH'] = float(data.loc[1, '[ID1.SG2] IBH'])
                    variables['NQ_IB_LOW'] = float(data.loc[1, '[ID1.SG3] IBL'])
                 
                case "NQ_4":
                    variables['NQ_OVNH'] = float(data.loc[2, '[ID1.SG2] OVN H'])
                    variables['NQ_OVNL'] = float(data.loc[2, '[ID1.SG3] OVN L'])
                    variables['NQ_TOTAL_OVN_DELTA'] = float(data.loc[2, '[ID3.SG4] OVN Total'])
                
                case "NQ_5":
                    variables['NQ_OVNTOIB_HI'] = float(data.loc[1, '[ID1.SG2] OVNTOIB_HI'])
                    variables['NQ_OVNTOIB_LO'] = float(data.loc[1, '[ID1.SG3] OVNTOIB_LO'])
                
                case "NQ_6":
                    variables['NQ_EURO_IBH'] = float(data.loc[1, '[ID1.SG2] EURO IBH'])
                    variables['NQ_EURO_IBL'] = float(data.loc[1, '[ID1.SG3] EURO IBL'])
                
                case "NQ_8":
                    variables['NQ_ORH'] = float(data.loc[1, '[ID1.SG2] ORH'])
                    variables['NQ_ORL'] = float(data.loc[1, '[ID1.SG3] ORL'])
                
                case "RTY_1":
                    variables['RTY_D_OPEN'] = float(data.loc[2, '[ID2.SG1] D_Open'])
                    variables['RTY_D_HIGH'] = float(data.loc[2, '[ID2.SG2] D_High'])
                    variables['RTY_D_LOW'] = float(data.loc[2, '[ID2.SG3] D_Low'])
                    variables['RTY_D_CLOSE'] = float(data.loc[2, '[ID2.SG4] D_Close'])
                    variables['RTY_VPOC'] = float(data.loc[2, '[ID1.SG1] VPOC'])
                    variables['RTY_PVPOC'] = float(data.loc[2, '[ID9.SG1] P_VPOC'])
                    variables['RTY_PRIOR_HIGH'] = float(data.loc[2, '[ID8.SG2] Prior High'])
                    variables['RTY_PRIOR_LOW'] = float(data.loc[2, '[ID8.SG3] Prior Low'])
                    variables['RTY_PRIOR_CLOSE'] = float(data.loc[2, '[ID8.SG4] Prior_Close'])
                    variables['RTY_PERIODIC_RVOL'] = float(data.loc[2, '[ID6.SG1] 30 MIN RELATIVITY'])
                    variables['RTY_CUMULATIVE_RVOL'] = float(data.loc[2, '[ID6.SG2] OVERALL RELATIVITY'])
                    variables['RTY_TOTAL_RTH_DELTA'] = float(data.loc[2, '[ID4.SG4] Total'])
                
                case "RTY_2":
                    variables['RTY_ETH_VWAP'] = float(data.loc[2, '[ID7.SG1] ETH VWAP'])
                    variables['RTY_CPL'] = float(data.loc[2, '[ID2.SG1] CPL'])
                    variables['RTY_5D_VPOC'] = float(data.loc[2, '[ID4.SG2] 5DVPOC'])
                    variables['RTY_20D_VPOC'] = float(data.loc[2, '[ID3.SG2] 20DVPOC'])
                    variables['RTY_P_WOPEN'] = float(data.loc[2, '[ID5.SG1] P_WOPEN'])
                    variables['RTY_P_WHIGH'] = float(data.loc[2, '[ID5.SG2] P_WHIGH'])
                    variables['RTY_P_WLO'] = float(data.loc[2, '[ID5.SG3] P_WLO'])
                    variables['RTY_P_WCLOSE'] = float(data.loc[2, '[ID5.SG4] P_WCLOSE'])
                    variables['RTY_P_WVPOC'] = float(data.loc[2, '[ID11.SG1] P_WVPOC'])
                    variables['RTY_WVWAP'] = float(data.loc[2, '[ID10.SG1] WVWAP'])
                    variables['RTY_P_MOPEN'] = float(data.loc[2, '[ID8.SG1] P_MOPEN'])
                    variables['RTY_P_MHIGH'] = float(data.loc[2, '[ID8.SG2] P_MHIGH'])
                    variables['RTY_P_MLO'] = float(data.loc[2, '[ID8.SG3] P_MLO'])
                    variables['RTY_P_MCLOSE'] = float(data.loc[2, '[ID8.SG4] P_MCLOSE'])
                    variables['RTY_P_MVPOC'] = float(data.loc[2, '[ID12.SG1] P_MVPOC'])
                    variables['RTY_MVWAP'] = float(data.loc[2, '[ID1.SG1] MVWAP'])
               
                case "RTY_3":
                    variables['RTY_IB_ATR'] = float(data.loc[2, '[ID2.SG1] IB ATR'])
                    variables['RTY_IB_HIGH'] = float(data.loc[1, '[ID1.SG2] IBH'])
                    variables['RTY_IB_LOW'] = float(data.loc[1, '[ID1.SG3] IBL'])
               
                case "RTY_4":
                    variables['RTY_OVNH'] = float(data.loc[2, '[ID1.SG2] OVN H'])
                    variables['RTY_OVNL'] = float(data.loc[2, '[ID1.SG3] OVN L'])
                    variables['RTY_TOTAL_OVN_DELTA'] = float(data.loc[2, '[ID3.SG4] OVN Total'])
                
                case "RTY_5":
                    variables['RTY_OVNTOIB_HI'] = float(data.loc[1, '[ID1.SG2] OVNTOIB_HI'])
                    variables['RTY_OVNTOIB_LO'] = float(data.loc[1, '[ID1.SG3] OVNTOIB_LO'])
                
                case "RTY_6":
                    variables['RTY_EURO_IBH'] = float(data.loc[1, '[ID1.SG2] EURO IBH'])
                    variables['RTY_EURO_IBL'] = float(data.loc[1, '[ID1.SG3] EURO IBL'])
                
                case "RTY_8":
                    variables['RTY_ORH'] = float(data.loc[1, '[ID1.SG2] ORH'])
                    variables['RTY_ORL'] = float(data.loc[1, '[ID1.SG3] ORL'])
                
                case "CL_1":
                    variables['CL_D_OPEN'] = float(data.loc[2, '[ID2.SG1] D_Open'])
                    variables['CL_D_HIGH'] = float(data.loc[2, '[ID2.SG2] D_High'])
                    variables['CL_D_LOW'] = float(data.loc[2, '[ID2.SG3] D_Low'])
                    variables['CL_D_CLOSE'] = float(data.loc[2, '[ID2.SG4] D_Close'])
                    variables['CL_VPOC'] = float(data.loc[2, '[ID1.SG1] VPOC'])
                    variables['CL_PVPOC'] = float(data.loc[2, '[ID9.SG1] P_VPOC'])
                    variables['CL_PRIOR_HIGH'] = float(data.loc[2, '[ID8.SG2] Prior High'])
                    variables['CL_PRIOR_LOW'] = float(data.loc[2, '[ID8.SG3] Prior Low'])
                    variables['CL_PRIOR_CLOSE'] = float(data.loc[2, '[ID8.SG4] Prior_Close'])
                    variables['CL_PERIODIC_RVOL'] = float(data.loc[2, '[ID6.SG1] 30 MIN RELATIVITY'])
                    variables['CL_CUMULATIVE_RVOL'] = float(data.loc[2, '[ID6.SG2] OVERALL RELATIVITY'])
                    variables['CL_TOTAL_RTH_DELTA'] = float(data.loc[2, '[ID4.SG4] Total'])
                
                case "CL_2":
                    variables['CL_ETH_VWAP'] = float(data.loc[2, '[ID7.SG1] ETH VWAP'])
                    variables['CL_CPL'] = float(data.loc[2, '[ID2.SG1] CPL'])
                    variables['CL_5D_VPOC'] = float(data.loc[2, '[ID4.SG2] 5DVPOC'])
                    variables['CL_20D_VPOC'] = float(data.loc[2, '[ID3.SG2] 20DVPOC'])
                    variables['CL_P_WOPEN'] = float(data.loc[2, '[ID5.SG1] P_WOPEN'])
                    variables['CL_P_WHIGH'] = float(data.loc[2, '[ID5.SG2] P_WHIGH'])
                    variables['CL_P_WLO'] = float(data.loc[2, '[ID5.SG3] P_WLO'])
                    variables['CL_P_WCLOSE'] = float(data.loc[2, '[ID5.SG4] P_WCLOSE'])
                    variables['CL_P_WVPOC'] = float(data.loc[2, '[ID11.SG1] P_WVPOC'])
                    variables['CL_WVWAP'] = float(data.loc[2, '[ID10.SG1] WVWAP'])
                    variables['CL_P_MOPEN'] = float(data.loc[2, '[ID8.SG1] P_MOPEN'])
                    variables['CL_P_MHIGH'] = float(data.loc[2, '[ID8.SG2] P_MHIGH'])
                    variables['CL_P_MLO'] = float(data.loc[2, '[ID8.SG3] P_MLO'])
                    variables['CL_P_MCLOSE'] = float(data.loc[2, '[ID8.SG4] P_MCLOSE'])
                    variables['CL_P_MVPOC'] = float(data.loc[2, '[ID12.SG1] P_MVPOC'])
                    variables['CL_MVWAP'] = float(data.loc[2, '[ID1.SG1] MVWAP'])
                
                case "CL_3":
                    variables['CL_IB_ATR'] = float(data.loc[2, '[ID2.SG1] IB ATR'])
                    variables['CL_IB_HIGH'] = float(data.loc[1, '[ID1.SG2] IBH'])
                    variables['CL_IB_LOW'] = float(data.loc[1, '[ID1.SG3] IBL'])
                
                case "CL_4":
                    variables['CL_OVNH'] = float(data.loc[2, '[ID1.SG2] OVN H'])
                    variables['CL_OVNL'] = float(data.loc[2, '[ID1.SG3] OVN L'])
                    variables['CL_TOTAL_OVN_DELTA'] = float(data.loc[2, '[ID3.SG4] OVN Total'])
                
                case "CL_5":
                    variables['CL_OVNTOIB_HI'] = float(data.loc[1, '[ID1.SG2] OVNTOIB_HI'])
                    variables['CL_OVNTOIB_LO'] = float(data.loc[1, '[ID1.SG3] OVNTOIB_LO'])
                
                case "CL_6":
                    variables['CL_EURO_IBH'] = float(data.loc[1, '[ID1.SG2] EURO IBH'])
                    variables['CL_EURO_IBL'] = float(data.loc[1, '[ID1.SG3] EURO IBL'])

                case "CL_8":
                    variables['CL_ORH'] = float(data.loc[1, '[ID1.SG2] ORH'])
                    variables['CL_ORL'] = float(data.loc[1, '[ID1.SG3] ORL'])
                
                case other:
                    print("Case Does Not Exist!")
                
                    
            product_name = task["name"].split('_')[0]

            # Initialize product's variables dictionary if not already present
            if product_name not in all_variables:
                all_variables[product_name] = {}

            # Update the product's variables
            all_variables[product_name].update(variables)  

        return all_variables